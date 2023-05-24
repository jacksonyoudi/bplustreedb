use crate::meta::Meta;

pub(crate) type PageID = u64;

pub(crate) type PageType = u8;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct Page {
    // id * pagesize is the offset from the beginning of the file
    pub(crate) id: PageID,
    pub(crate) page_type: PageType,
    // Number of elements on this page, the type of element depends on the pageType
    pub(crate) count: u64,
    // Number of additional pages after this one that are part of this block
    pub(crate) overflow: u64,
    // ptr serves as a reference to where the actual data starts
    pub(crate) ptr: u64,
}


impl Page {

    #[inline]
    pub(crate) fn from_buf(buf: &[u8], id: PageID, pagesize: u64) -> &Page {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*(&buf[(id * pagesize) as usize] as *const u8 as *const Page)
        }
    }

    pub(crate) fn meta(&self) -> &Meta {
        assert_eq!(self.page_type, Page::TYPE_META);
        unsafe { &*(&self.ptr as *const u64 as *const Meta) }
    }
}

