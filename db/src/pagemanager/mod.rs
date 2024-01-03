mod diskbased;
use std::cell::Ref;

pub use diskbased::DiskBasedPageManager;

mod inmemory;
pub use inmemory::InMemoryPageManager;

mod pageaccessor;
pub use pageaccessor::{from_page, into_page, bytes_into_page};

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u64;

pub trait PageManager {
    fn read_page(&mut self, id: PageId) -> std::io::Result<Box<dyn PageAccessor>>;
    fn write_page(&mut self, page: Box<dyn PageAccessor>) -> std::io::Result<()>;
    fn alloc_page(&mut self) -> std::io::Result<PageId>;
    fn free_page(&mut self, id: PageId) -> std::io::Result<()>;
}

pub trait PageAccessor {
    fn id(&self) -> PageId;
    fn size(&self) -> usize;
    fn data(&self) -> Ref<Vec<u8>>;
}
