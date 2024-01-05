use thiserror::Error;

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("DatabaseError in PageManager, {0}")]
    PageManagerError(#[from] crate::pagemanager::exceptions::PageManagerError),
}