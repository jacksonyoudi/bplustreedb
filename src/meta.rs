use crate::bucket::BucketMeta;
use crate::page::PageID;

#[repr(C)]
#[derive(Debug, Clone)]
pub(crate) struct Meta {
    pub(crate) meta_page: u32,
    pub(crate) magic: u32,
    pub(crate) version: u32,
    pub(crate) pagesize: u64,
    pub(crate) root: BucketMeta,
    pub(crate) num_pages: PageID,
    pub(crate) freelist_page: PageID,
    pub(crate) tx_id: u64,
    pub(crate) hash: [u8; 32],
}

impl Meta {
    pub(crate) fn valid(&self) -> bool {
        self.hash == self.hash_self()
    }

    pub(crate) fn hash_self(&self) -> [u8; 32] {
        let mut hash_result: [u8; 32] = [0; 32];
        let mut hasher = Sha3_256::new();
        hasher.update(self.bytes());
        let hash = hasher.finalize();
        assert_eq!(hash.len(), 32);
        hash_result.copy_from_slice(&hash[..]);
        hash_result
    }


}