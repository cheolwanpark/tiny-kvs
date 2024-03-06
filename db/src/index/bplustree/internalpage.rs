use super::PageHeader;
use crate::pagemanager::{PageId, PAGE_SIZE};
use std::mem::size_of;

pub struct InternalPage {
    header: InternalHeader,
    _slot_buffer: [u8; PAGE_SIZE - size_of::<InternalHeader>()],
}

struct InternalHeader {
    page_header: PageHeader,
    _reserved: [u8; 98],
    leftmost_child_id: PageId,
}

impl InternalPage {
    pub fn new() -> Self {
        InternalPage {
            header: InternalHeader {
                page_header: PageHeader {
                    parent_id: 0,
                    is_leaf: 0,
                    num_keys: 0,
                },
                _reserved: [0; 98],
                leftmost_child_id: 0,
            },
            _slot_buffer: [0; PAGE_SIZE - size_of::<InternalHeader>()],
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn internal_page_slot_test() {
        let page = InternalPage::new();
    }
}