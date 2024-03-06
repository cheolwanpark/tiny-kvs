use crate::pagemanager::PageId;
use serde_derive::{Serialize as SerializeDerive, Deserialize as DeserializeDerive};

mod exceptions;
mod leafpage;
mod internalpage;

#[derive(Clone, Default, SerializeDerive, DeserializeDerive)]
pub struct PageHeader {
    parent_id: PageId,
    is_leaf: u32,
    num_keys: u32,
}

pub const KEY_LENGTH_LIMIT: usize = 64;
pub const VALUE_LENGTH_LIMIT: usize = 256;