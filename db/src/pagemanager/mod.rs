use std::cell::Ref;
use serde_derive::{Serialize as SerializeDerive, Deserialize as DeserializeDerive};

mod diskbased;
pub use diskbased::DiskBasedPageManager;

mod inmemory;
pub use inmemory::InMemoryPageManager;

mod pageaccessor;
pub use pageaccessor::{from_page, into_page, bytes_into_page};

pub mod exceptions;
pub use exceptions::Result;
pub use exceptions::PageManagerError;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u64;

#[derive(Clone, Default, SerializeDerive, DeserializeDerive)]
pub struct HeaderPage {
    free_page_id: PageId,
    num_pages: u64,
}

pub trait PageManager {
    fn read_header_page(&mut self) -> Result<HeaderPage>;
    fn write_header_page(&mut self, header: HeaderPage) -> Result<()>;
    fn read_page(&mut self, id: PageId) -> Result<Box<dyn PageAccessor>>;
    fn write_page(&mut self, page: Box<dyn PageAccessor>) -> Result<()>;
    fn alloc_page(&mut self) -> Result<PageId>;
    fn free_page(&mut self, id: PageId) -> Result<()>;
}

pub trait PageAccessor {
    fn id(&self) -> PageId;
    fn size(&self) -> usize;
    fn data(&self) -> Ref<Vec<u8>>;
}
