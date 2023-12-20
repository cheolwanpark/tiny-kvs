use serde::{Serialize, de::DeserializeOwned};

pub type PageId = u64;

pub trait PageManager {
    fn read_page<T: DeserializeOwned + Sized>(&mut self, id: PageId) -> std::io::Result<T>;
    fn write_page<T: Serialize>(&mut self, id: PageId, obj: &T) -> std::io::Result<()>;
    fn alloc_page(&mut self) -> std::io::Result<PageId>;
    fn free_page(&mut self, id: PageId) -> std::io::Result<()>;
}

pub mod diskbased;
pub use diskbased::DiskBasedPageManager;
