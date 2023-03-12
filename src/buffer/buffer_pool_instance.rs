use std::collections::{HashMap, VecDeque};
use std::io;

use crate::buffer::buffer_pool::PageIdIncrementFn;
use crate::buffer::disk_manager::DiskManager;
use crate::buffer::lru::LRU;
use crate::buffer::page::Page;
use crate::buffer::types::{FrameId, PageId};
use crate::types::{Arc, Mutex};

struct UnsafeBufferPoolInstance {
    lru: LRU,
    disk_manager: DiskManager,
    next_page_id: PageId,
    inc_fn: PageIdIncrementFn,
    pages: Vec<Page>,
    free_list: VecDeque<FrameId>,
    page_table: HashMap<PageId, FrameId>,
}

impl UnsafeBufferPoolInstance {
    fn find_page(&self, page_id: PageId) -> Option<(FrameId, Page)> {
        self.page_table.get(&page_id).map(|frame_id| (*frame_id, self.pages[*frame_id].clone()))
    }

    fn flush_page(&self, page_id: PageId) -> io::Result<bool> {
        if let Some((_frame_id, mut page)) = self.find_page(page_id) {
            self.write_page(&mut page)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn write_page(&self, page: &mut Page) -> io::Result<()> {
        if page.is_dirty() {
            let page_id = page.get_page_id().unwrap();
            page.access_page_data(|data| self.disk_manager.write(page_id, data))
        } else {
            Ok(())
        }
    }

    fn find_fresh_page(&mut self) -> io::Result<Option<(FrameId, Page)>> {
        if let Some(frame_id) = self.free_list.pop_back() {
            let mut page = self.pages[frame_id].clone();
            page.pin();
            Ok(Some((frame_id, page)))
        } else if let Some(frame_id) = self.lru.remove_last() {
            let mut page = self.pages[frame_id].clone();
            self.write_page(&mut page)?;
            page.get_page_id().map(|page_id| self.page_table.remove(&page_id));
            page.reset();
            page.pin();
            Ok(Some((frame_id, page)))
        } else {
            Ok(None)
        }
    }

    fn allocate_page(&mut self) -> PageId {
        let result = self.next_page_id;
        self.next_page_id = (self.inc_fn)(self.next_page_id);
        result
    }
}

#[derive(Clone)]
pub struct BufferPoolInstance(Arc<Mutex<UnsafeBufferPoolInstance>>);

impl BufferPoolInstance {
    fn new_simple(disk_manager: DiskManager, size: usize) -> Self {
        BufferPoolInstance::new(disk_manager, size, 0, Box::new(|page_id| page_id + 1))
    }

    pub fn new(disk_manager: DiskManager, size: usize, next_page_id: PageId, inc_fn: PageIdIncrementFn) -> Self {
        let mut pages = Vec::new();
        pages.resize_with(size, || Page::new());

        BufferPoolInstance(Arc::new(Mutex::new(UnsafeBufferPoolInstance {
            lru: LRU::new(),
            disk_manager,
            next_page_id,
            inc_fn,
            pages,
            free_list: VecDeque::from_iter(0..size),
            page_table: HashMap::with_capacity(size),
        })))
    }

    pub fn flush_page(&self, page_id: PageId) -> io::Result<bool> {
        let instance = self.0.lock().unwrap();
        instance.flush_page(page_id)
    }

    // fn flush_all(&self) -> io::Result<()> {
    //     let instance = self.0.lock().unwrap();
    //
    //     for (page_id, _frame_id) in &instance.page_table {
    //         instance.flush_page(*page_id)?;
    //     }
    //     Ok(())
    // }

    pub fn new_page(&mut self) -> io::Result<Option<Page>> {
        let mut instance = self.0.lock().unwrap();

        if let Some((frame_id, mut page)) = instance.find_fresh_page()? {
            let page_id = instance.allocate_page();
            instance.page_table.insert(page_id, frame_id);
            page.set_page_id(page_id);
            Ok(Some(page))
        } else {
            Ok(None)
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> io::Result<Option<Page>> {
        let mut instance = self.0.lock().unwrap();

        if let Some((frame_id, mut page)) = instance.find_page(page_id) {
            instance.lru.remove(frame_id);
            page.pin();
            Ok(Some(page))
        } else if let Some((frame_id, mut page)) = instance.find_fresh_page()? {
            instance.lru.remove(frame_id);
            instance.page_table.insert(page_id, frame_id);
            page.set_page_id(page_id);
            page.access_page_data(|data| instance.disk_manager.read(page_id, data))?;
            Ok(Some(page))
        } else {
            Ok(None)
        }
    }

    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> io::Result<bool> {
        let mut instance = self.0.lock().unwrap();

        instance
            .find_page(page_id)
            .filter(|(_frame_id, page)| page.get_pin_count() != 0)
            .map(|(frame_id, mut page)| {
                page.unpin();
                page.set_dirty(is_dirty);
                if page.get_pin_count() == 0 {
                    instance.lru.add(frame_id);
                }
                instance.write_page(&mut page).map(|_x| true)
            })
            .unwrap_or(Ok(false))
    }

    pub fn delete_page(&mut self, page_id: PageId) -> bool {
        let mut instance = self.0.lock().unwrap();

        instance
            .find_page(page_id)
            .filter(|(_frame_id, page)| page.get_pin_count() == 0)
            .map(|(frame_id, mut page)| {
                page.reset();
                instance.page_table.remove(&page_id);
                instance.free_list.push_back(frame_id);
                true
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::buffer::buffer_pool_instance::BufferPoolInstance;
    use crate::buffer::constants::PAGE_SIZE;
    use crate::buffer::disk_manager::DiskManager;
    use crate::buffer::test_utils::TempFile;
    use crate::types::{check_random, RngCore, thread, thread_rng};

    #[test]
    fn should_run_scenario() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 10;
        let mut instance = BufferPoolInstance::new_simple(disk_manager, size);

        let mut page0 = instance.new_page()?;
        assert!(page0.is_some());
        assert_eq!(page0.as_ref().and_then(|page| page.get_page_id()), Some(0));

        let mut copy0 = [0; PAGE_SIZE];
        page0.as_mut().unwrap().access_page_data(|data| {
            thread_rng().fill_bytes(data);
            copy0.clone_from_slice(data);
        });

        for page_id in 1..size {
            let page = instance.new_page()?;
            assert!(page.is_some());
            assert_eq!(page.as_ref().and_then(|page| page.get_page_id()), Some(page_id));
        }

        for _i in size..size * 2 {
            let page = instance.new_page()?;
            assert!(page.is_none());
        }

        for page_id in 0..5 {
            assert!(instance.unpin_page(page_id, true)?);
        }

        for i in 0..5 {
            let page = instance.new_page()?;
            let page_id = page.as_ref().unwrap().get_page_id().unwrap();
            assert!(page.is_some());
            assert_eq!(page_id, size + i);
            assert!(instance.unpin_page(page_id, false)?);
        }

        page0.as_mut().unwrap().access_page_data(|data| {
            let empty_page = [0; PAGE_SIZE];
            assert_eq!(data, empty_page);
        });

        page0 = instance.fetch_page(0)?;
        page0.as_mut().unwrap().access_page_data(|data| {
            assert_eq!(data, copy0);
        });

        Ok(())
    }

    #[test]
    fn should_read_page_without_write() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 1;
        let mut instance = BufferPoolInstance::new_simple(disk_manager, size);

        let mut page = instance.fetch_page(0)?;
        assert!(page.is_some());
        page.as_mut().unwrap().access_page_data(|data| {
            let empty_page = [0; PAGE_SIZE];
            assert_eq!(data, empty_page);
        });

        Ok(())
    }

    #[test]
    fn should_read_write_delete_page() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 1;
        let mut instance = BufferPoolInstance::new_simple(disk_manager, size);

        let mut page0 = instance.new_page()?;
        assert!(page0.is_some());

        let mut copy0 = [0; PAGE_SIZE];
        page0.as_mut().unwrap().access_page_data(|data| {
            thread_rng().fill_bytes(data);
            copy0.clone_from_slice(data);
        });

        assert!(instance.unpin_page(0, true)?);
        assert!(instance.delete_page(0));

        let another_page = instance.new_page()?;
        assert!(another_page.is_some());
        assert_eq!(another_page.unwrap().get_page_id(), Some(1));
        assert!(instance.unpin_page(1, true)?);
        assert!(instance.delete_page(1));

        page0 = instance.fetch_page(0)?;
        page0.as_mut().unwrap().access_page_data(|data| {
            assert_eq!(data, copy0);
        });

        Ok(())
    }

    #[test]
    fn should_not_flush_when_page_does_not_exist() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 1;
        let instance = BufferPoolInstance::new_simple(disk_manager, size);

        assert!(!instance.flush_page(0)?);
        Ok(())
    }

    #[test]
    fn should_fetch_cached_page() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 1;
        let mut instance = BufferPoolInstance::new_simple(disk_manager, size);

        assert!(instance.new_page()?.is_some());
        assert!(instance.fetch_page(0)?.is_some());

        Ok(())
    }

    #[test]
    fn should_create_new_page_when_full() -> io::Result<()> {
        let file = TempFile::new()?;
        let disk_manager = DiskManager::new(file.path())?;
        let size = 1;
        let mut instance = BufferPoolInstance::new_simple(disk_manager, size);

        assert!(instance.new_page()?.is_some());
        assert!(instance.new_page()?.is_none());

        Ok(())
    }

    #[test]
    fn should_work_concurrently() {
        check_random(
            || {
                let file = TempFile::new().unwrap();
                let disk_manager = DiskManager::new(file.path()).unwrap();
                let size = 2;
                let instance = BufferPoolInstance::new_simple(disk_manager, size);

                let mut instance1 = instance.clone();
                let handle1 = thread::spawn(move || {
                    let mut page = instance1.fetch_page(0).unwrap().unwrap();
                    let mut copy = [0; PAGE_SIZE];
                    page.access_page_data(|data| {
                        thread_rng().fill_bytes(data);
                        copy.clone_from_slice(data);
                    });
                    page.set_dirty(true);

                    instance1.flush_page(0).unwrap();
                    instance1.unpin_page(0, true).unwrap();
                    instance1.delete_page(0);

                    page = instance1.fetch_page(0).unwrap().unwrap();
                    page.access_page_data(|data| assert_eq!(data, copy));
                });

                let mut instance2 = instance.clone();
                let handle2 = thread::spawn(move || {
                    let mut page = instance2.fetch_page(1).unwrap().unwrap();
                    let mut copy = [0; PAGE_SIZE];
                    page.access_page_data(|data| {
                        thread_rng().fill_bytes(data);
                        copy.clone_from_slice(data);
                    });
                    page.set_dirty(true);

                    instance2.flush_page(1).unwrap();
                    instance2.unpin_page(1, true).unwrap();
                    instance2.delete_page(1);

                    page = instance2.fetch_page(1).unwrap().unwrap();
                    page.access_page_data(|data| assert_eq!(data, copy));
                });

                handle1.join().unwrap();
                handle2.join().unwrap();
            },
            100,
        )
    }
}
