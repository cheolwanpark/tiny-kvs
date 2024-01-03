use std::cell::RefCell;
use serde::{Serialize, de::DeserializeOwned};
use super::*;

pub fn from_page<T: DeserializeOwned>(page: Box<dyn PageAccessor>) -> T {
    bincode::deserialize(&page.data()).unwrap()
}

struct PageAccessorImpl {
    id: PageId,
    data: RefCell<Vec<u8>>,
}

impl PageAccessor for PageAccessorImpl {
    fn id(&self) -> PageId {
        self.id
    }

    fn size(&self) -> usize {
        self.data.borrow().len()
    }

    fn data(&self) -> Ref<Vec<u8>> {
        self.data.borrow()
    }

}

pub fn into_page<T: Serialize>(id: PageId, obj: &T) -> Box<dyn PageAccessor> {
    let data = bincode::serialize(obj).unwrap();
    Box::new(PageAccessorImpl {
        id,
        data: RefCell::new(data),
    })
}

pub fn bytes_into_page(id: PageId, data: Vec<u8>) -> Box<dyn PageAccessor> {
    Box::new(PageAccessorImpl {
        id,
        data: RefCell::new(data),
    })
}