use crate::buffer::constants::PAGE_SIZE;
use crate::buffer::types::{PageData, PageId};
use std::sync::{Arc, RwLock};

struct UnsafePage {
    data: PageData,
    page_id: Option<PageId>,
    is_dirty: bool,
    pin_count: u32,
}

#[derive(Clone)]
pub struct Page(Arc<RwLock<UnsafePage>>);

impl Page {
    pub fn new() -> Self {
        Page(Arc::new(RwLock::new(UnsafePage { data: [0; PAGE_SIZE], page_id: None, is_dirty: false, pin_count: 0 })))
    }

    pub fn access_page_data<F, R>(&mut self, f: F) -> R
    where
        F: (FnOnce(&mut [u8]) -> R),
    {
        let mut page = self.0.write().unwrap();
        f(page.data.as_mut_slice())
    }

    pub fn get_page_id(&self) -> Option<PageId> {
        self.0.read().unwrap().page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.0.write().unwrap().page_id = Some(page_id)
    }

    pub fn is_dirty(&self) -> bool {
        self.0.read().unwrap().is_dirty
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.0.write().unwrap().is_dirty = is_dirty
    }

    pub fn get_pin_count(&self) -> u32 {
        self.0.read().unwrap().pin_count
    }

    pub fn pin(&mut self) {
        let mut page = self.0.write().unwrap();
        page.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        let mut page = self.0.write().unwrap();
        page.pin_count -= 1;
    }

    pub fn reset(&mut self) {
        let mut page = self.0.write().unwrap();
        page.page_id = None;
        page.data.fill(0);
        page.is_dirty = false;
    }
}

#[cfg(test)]
mod test {
    use super::Page;
    use crate::buffer::constants::PAGE_SIZE;
    use crate::types::{thread_rng, RngCore};

    #[test]
    fn should_read_write_page() {
        let mut page = Page::new();

        let mut copy = [0; PAGE_SIZE];
        page.access_page_data(|data| {
            thread_rng().fill_bytes(data);
            copy.clone_from_slice(data);
        });

        page.reset();
        page.access_page_data(|data| {
            assert_ne!(copy, data);
        });
    }

    #[test]
    fn should_pin_unpin_page() {
        let mut page = Page::new();

        page.pin();
        page.pin();
        assert_eq!(page.get_pin_count(), 2);
        page.unpin();
        page.unpin();
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn should_set_unset_is_dirty() {
        let mut page = Page::new();

        page.set_dirty(true);
        assert!(page.is_dirty());
        page.set_dirty(false);
        assert!(!page.is_dirty());
    }

    #[test]
    fn should_set_reset_page_id() {
        let mut page = Page::new();

        page.set_page_id(123);
        assert_eq!(page.get_page_id(), Some(123));

        page.reset();
        assert_eq!(page.get_page_id(), None);
    }
}
