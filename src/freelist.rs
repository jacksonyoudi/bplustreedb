use std::{
    collections::{BTreeMap, BTreeSet}
};
use crate::page::PageID;


#[derive(Clone)]
pub(crate) struct Freelist {
    free_pages: BTreeSet<PageID>,
    pending_pages: BTreeMap<u64, Vec<PageID>>,
}