#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions as FileOpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use fs2::FileExt;
use memmap2::Mmap;
use page_size::get as get_page_size;

const MAGIC_VALUE: u32 = 0x00AB_CDEF;
const VERSION: u32 = 1;

pub(crate) const MIN_ALLOC_SIZE: u64 = 8 * 1024 * 1024;

const DEFAULT_NUM_PAGES: usize = 32;


pub(crate) struct DBFlags {
    pub(crate) strict_mode: bool,
    pub(crate) mmap_populate: bool,
    pub(crate) direct_writes: bool,
}

pub struct OpenOptions {
    pagesize: u64,
    num_pages: usize,
    flags: DBFlags,
}

impl Default for OpenOptions {
    fn default() -> Self {
        // 获取当前系统内存页的大小
        let pagesize = get_page_size() as u64;
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        OpenOptions {
            pagesize,
            num_pages: DEFAULT_NUM_PAGES,
            flags: DBFlags {
                strict_mode: false,
                mmap_populate: false,
                direct_writes: false,
            },
        }
    }
}


impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    //  设置pagesize
    pub fn pagesize(mut self, pagesize: u64) -> Self {
        if pagesize < 1024 {
            panic!("Pagesize must be 1024 bytes minimum");
        }
        self.pagesize = pagesize;
        self
    }

    pub fn num_pages(mut self, num_pages: usize) -> Self {
        if num_pages < 4 {
            panic!("Must have a minimum of 4 pages");
        }
        self.num_pages = num_pages;
        self
    }

    pub fn strict_mode(mut self, strict_mode: bool) -> Self {
        self.flags.strict_mode = strict_mode;
        self
    }

    pub fn mmap_populate(mut self, mmap_populate: bool) -> Self {
        self.flags.mmap_populate = mmap_populate;
        self
    }

    pub fn direct_writes(mut self, direct_writes: bool) -> Self {
        self.flags.direct_writes = direct_writes;
        self
    }




}

















