use thiserror::Error;
use std::string::FromUtf8Error;

pub type Result<T> = std::result::Result<T, BPTreeError>;

#[derive(Error, Debug)]
pub enum BPTreeError {
    #[error("large key error, key length {0} exceeds limit {1}")]
    KeyLengthError(usize, usize),
    #[error("large value error, value length {0} exceeds limit {1}")]
    ValueLengthError(usize, usize),
    #[error("not enough remaining space to insert record, required {0} bytes, remaining {1} bytes")]
    NotEnoughSpaceError(usize, usize),
    #[error("invalid slot index {0}, num keys {1}")]
    InvalidSlotIndexError(u32, u32),
    #[error("can't convert bytes into utf8 string, {0}")]
    Utf8ConvertError(#[from] FromUtf8Error),
}
