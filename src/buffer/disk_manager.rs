use std::fs::File;
use std::io;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::types::{Arc, RwLock};

use super::constants::PAGE_SIZE;
use super::types::PageId;

struct UnsafeDiskManager {
    file: File,
}

#[derive(Clone)]
pub struct DiskManager(Arc<RwLock<UnsafeDiskManager>>);

impl DiskManager {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<DiskManager> {
        let file = File::options().create(true).read(true).write(true).append(true).open(path)?;
        Ok(DiskManager(Arc::new(RwLock::new(UnsafeDiskManager { file }))))
    }

    pub fn write(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()> {
        let mut manager = self.0.write().unwrap();

        manager.file.write_all_at(buf, DiskManager::offset(page_id))?;
        manager.file.flush()?;

        Ok(())
    }

    pub fn read(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()> {
        let manager = self.0.read().unwrap();

        let read_count = manager.file.read_at(buf, DiskManager::offset(page_id))?;
        if read_count < PAGE_SIZE {
            buf[read_count..].fill(0);
        }

        return Ok(());
    }

    fn offset(page_id: PageId) -> u64 {
        (page_id * PAGE_SIZE) as u64
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::buffer::constants::PAGE_SIZE;
    use crate::buffer::test_utils::{random_page, TempFile};
    use crate::buffer::types::PageData;
    use crate::types::{check_random, thread, thread_rng, RngCore};

    use super::DiskManager;

    #[test]
    fn should_read_write_page() -> io::Result<()> {
        let file = TempFile::new()?;
        let manager = DiskManager::new(file.path())?;
        let mut page: PageData = random_page();

        manager.write(0, page.as_mut_slice())?;
        let mut result: PageData = [0; PAGE_SIZE];
        manager.read(0, result.as_mut_slice())?;

        assert_eq!(result, page);
        Ok(())
    }

    #[test]
    fn should_read_write_page_at_non_0() -> io::Result<()> {
        let file = TempFile::new()?;
        let manager = DiskManager::new(file.path())?;
        let mut page: PageData = random_page();

        manager.write(1, page.as_mut_slice())?;
        let mut result: PageData = [0; PAGE_SIZE];
        manager.read(1, result.as_mut_slice())?;

        assert_eq!(result, page);
        Ok(())
    }

    #[test]
    fn should_read_without_writing() -> io::Result<()> {
        let file = TempFile::new()?;
        let manager: DiskManager = DiskManager::new(file.path())?;
        let page: PageData = [0; PAGE_SIZE];

        let mut result: PageData = random_page();
        manager.read(10, result.as_mut_slice())?;

        assert_eq!(result, page);
        Ok(())
    }

    #[test]
    fn should_work_concurrently() {
        check_random(
            || {
                let file = TempFile::new().unwrap();
                let manager = DiskManager::new(file.path()).unwrap();

                let manager1 = manager.clone();
                let handle1 = thread::spawn(move || {
                    let mut page: PageData = [0; PAGE_SIZE];
                    thread_rng().fill_bytes(page.as_mut_slice());

                    manager1.write(1, page.as_mut_slice()).unwrap();
                    let mut result: PageData = [0; PAGE_SIZE];
                    manager1.read(1, result.as_mut_slice()).unwrap();

                    assert_eq!(result, page);
                });

                let manager2 = manager.clone();
                let handle2 = thread::spawn(move || {
                    let mut page = random_page();

                    manager2.write(2, page.as_mut_slice()).unwrap();
                    let mut result: PageData = [0; PAGE_SIZE];
                    manager2.read(2, result.as_mut_slice()).unwrap();

                    assert_eq!(result, page);
                });
                handle1.join().unwrap();
                handle2.join().unwrap();
            },
            100,
        );
    }
}
