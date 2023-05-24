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
use crate::errors::Result;
use crate::freelist::Freelist;
use crate::meta::Meta;
use crate::page::Page;

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


    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
        let path: &Path = path.as_ref();
        let file = if !path.exists() {
            init_file(
                path,
                self.pagesize,
                self.num_pages,
                self.flags.direct_writes,
            )?
        } else {
            open_file(path, false, self.flags.direct_writes)?
        };
        let db = DBInner::open(file, self.pagesize, self.flags)?;
        Ok(DB {
            inner: Arc::new(db),
        })
    }
}


#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        OpenOptions::new().open(path)
    }

    pub fn tx(&self, writable: bool) -> Result<Tx> {
        Tx::new(self, writable)
    }

    pub fn pagesize(&self) -> u64 {
        self.inner.pagesize
    }

    #[doc(hidden)]
    pub fn check(&self) -> Result<()> {
        self.tx(false)?.check()
    }
}


pub(crate) struct DBInner {
    pub(crate) data: Mutex<Arc<Mmap>>,
    pub(crate) mmap_lock: RwLock<()>,
    pub(crate) freelist: Mutex<Freelist>,
    pub(crate) file: Mutex<File>,
    pub(crate) open_ro_txs: Mutex<Vec<u64>>,
    pub(crate) flags: DBFlags,

    pub(crate) pagesize: u64,
}

impl DBInner {
    pub(crate) fn open(file: File, pagesize: u64, flags: DBFlags) -> Result<DBInner> {
        // 获取一个独占锁
        file.lock_exclusive()?;
        let mmap = mmap(&file, flags.mmap_populate)?;
        let mmap = Mutex::new(Arc::new(mmap));
        let db = DBInner {
            data: mmap,
            mmap_lock: RwLock::new(()),
            freelist: Mutex::new(Freelist::new()),
            file: Mutex::new(file),
            open_ro_txs: Mutex::new(Vec::new()),

            pagesize,
            flags,
        };
        {
            let meta = db.meta()?;
            let data = db.data.lock()?;
            let free_pages = Page::from_buf(&data, meta.freelist_page, pagesize).freelist();

            if !free_pages.is_empty() {
                db.freelist.lock()?.init(free_pages);
            }
        }

        Ok(db)
    }

    pub(crate) fn resize(&self, file: &File, new_size: u64) -> Result<Arc<Mmap>> {
        // 预分配空间
        file.allocate(new_size)?;
        let _lock_write_guard = self.mmap_lock.write()?;
        let mut data = self.data.lock()?;
        let mmap = mmap(file, self.flags.mmap_populate)?;
        *data = Arc::new(mmap);
        Ok(data.clone())
    }

    pub(crate) fn meta(&self) -> Result<Meta> {
        let data = self.data.lock()?;
        let meta1 = Page::from_buf(&data, 0, self.pagesize).meta();

        // Double check that we have the right pagesize before we read the second page.
        if meta1.valid() && meta1.pagesize != self.pagesize {
            assert_eq!(
                meta1.pagesize, self.pagesize,
                "Invalid pagesize from meta1 {}. Expected {}.",
                meta1.pagesize, self.pagesize
            );
        }

        let meta2 = Page::from_buf(&data, 1, self.pagesize).meta();
        let meta = match (meta1.valid(), meta2.valid()) {
            (true, true) => {
                assert_eq!(
                    meta1.pagesize, self.pagesize,
                    "Invalid pagesize from meta1 {}. Expected {}.",
                    meta1.pagesize, self.pagesize
                );
                assert_eq!(
                    meta2.pagesize, self.pagesize,
                    "Invalid pagesize from meta2 {}. Expected {}.",
                    meta2.pagesize, self.pagesize
                );
                if meta1.tx_id > meta2.tx_id {
                    meta1
                } else {
                    meta2
                }
            }
            (true, false) => {
                assert_eq!(
                    meta1.pagesize, self.pagesize,
                    "Invalid pagesize from meta1 {}. Expected {}.",
                    meta1.pagesize, self.pagesize
                );
                meta1
            }
            (false, true) => {
                assert_eq!(
                    meta2.pagesize, self.pagesize,
                    "Invalid pagesize from meta2 {}. Expected {}.",
                    meta2.pagesize, self.pagesize
                );
                meta2
            }
            (false, false) => panic!("NO VALID META PAGES"),
        };

        Ok(meta.clone())
    }
}


fn init_file(path: &Path, pagesize: u64, num_pages: usize, direct_write: bool) -> Result<File> {
    let mut file = open_file(path, true, direct_write)?;
    file.allocate(pagesize * (num_pages as u64))?;
    let mut buf = vec![0; (pagesize * 4) as usize];
    let mut get_page = |index: u64| {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &mut *(&mut buf[(index * pagesize) as usize] as *mut u8 as *mut Page)
        }
    };
    for i in 0..2 {
        let page = get_page(i);
        page.id = i;
        page.page_type = Page::TYPE_META;
        let m = page.meta_mut();
        m.meta_page = i as u32;
        m.magic = MAGIC_VALUE;
        m.version = VERSION;
        m.pagesize = pagesize;
        m.freelist_page = 2;
        m.root = BucketMeta {
            root_page: 3,
            next_int: 0,
        };
        m.num_pages = 4;
        m.hash = m.hash_self();
    }

    let p = get_page(2);
    p.id = 2;
    p.page_type = Page::TYPE_FREELIST;
    p.count = 0;

    let p = get_page(3);
    p.id = 3;
    p.page_type = Page::TYPE_LEAF;
    p.count = 0;

    file.write_all(&buf[..])?;
    file.flush()?;
    file.sync_all()?;
    Ok(file)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
const O_DIRECT: libc::c_int = 0;


// Have different mmap functions for Unix and Windows
#[cfg(unix)]
fn open_file<P: AsRef<Path>>(path: P, create: bool, direct_write: bool) -> Result<File> {
    let mut open_options = FileOpenOptions::new();
    open_options.write(true).read(true);
    if create {
        open_options.create_new(true);
    }
    if direct_write {
        open_options.custom_flags(O_DIRECT);
    }
    Ok(open_options.open(path)?)
}

#[cfg(windows)]
fn open_file<P: AsRef<Path>>(path: P, create: bool, direct_write: bool) -> Result<File> {
    let mut open_options = FileOpenOptions::new();
    open_options.write(true).read(true);
    if create {
        open_options.create_new(true);
    }
    Ok(open_options.open(path)?)
}

// Have different mmap functions for Unix and Windows
#[cfg(unix)]
fn mmap(file: &File, populate: bool) -> Result<Mmap> {
    use memmap2::MmapOptions;

    let mut options = MmapOptions::new();
    if populate {
        options.populate();
    }
    let mmap = unsafe { options.map(file)? };
    // On Unix we advice the OS that page access will be random.
    mmap.advise(memmap2::Advice::Random)?;
    Ok(mmap)
}

// On Windows there is no advice to give.
#[cfg(windows)]
fn mmap(file: &File, populate: bool) -> Result<Mmap> {
    let mmap = unsafe { Mmap::map(file)? };
    Ok(mmap)
}