use crate::buffer::types::PageId;

pub(super) type PageIdIncrementFn = Box<dyn Fn(PageId) -> PageId + Send>;
