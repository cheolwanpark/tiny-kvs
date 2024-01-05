use thiserror::Error;
use super::PageId;

pub type Result<T> = std::result::Result<T, PageManagerError>;

#[derive(Error, Debug)]
pub enum PageManagerError {
    #[error("{0} (FileIOError)")]
    FileIOError(#[from] std::io::Error),
    #[error("Invalid Page Id, try to access page id {0}")]
    InvalidPageId(PageId),
    #[error("Tried to Evict Pinned Page, Page Id {0}")]
    TryToEvictPinnedPage(PageId),
    #[error("All pages are pinned")]
    AllPagesArePinned,
}
