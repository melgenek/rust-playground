use super::constants::PAGE_SIZE;

pub(super) type FrameId = usize;
pub(super) type PageId = usize;
pub(super) type PageData = [u8; PAGE_SIZE];
