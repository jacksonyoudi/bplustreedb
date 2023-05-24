use crate::page::PageID;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BucketMeta {
    pub(crate) root_page: PageID,
    pub(crate) next_int: u64,
}