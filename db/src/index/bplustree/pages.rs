use crate::pagemanager::{PageId, PAGE_SIZE};
use std::mem::size_of;

pub struct LeafPage {
    header: LeafHeader,
    _slot_buffer: [u8; PAGE_SIZE - size_of::<LeafHeader>()],
}

pub struct InternalPage {
    header: InternalHeader,
    _slot_buffer: [u8; PAGE_SIZE - size_of::<InternalHeader>()],
}

struct Header {
    parent_id: PageId,
    is_leaf: u32,
    num_keys: u32,
}

struct LeafHeader {
    page_header: Header,
    _reserved: [u8; 98],
    freespace: u32,
    right_sibling_id: PageId,
}

struct InternalHeader {
    page_header: Header,
    _reserved: [u8; 98],
    leftmost_child_id: PageId,
}

impl LeafPage {
    pub fn new() -> Self {
        LeafPage {
            header: LeafHeader {
                page_header: Header {
                    parent_id: 0,
                    is_leaf: 1,
                    num_keys: 0,
                },
                _reserved: [0; 98],
                freespace: 0,
                right_sibling_id: 0,
            },
            _slot_buffer: [0; PAGE_SIZE - size_of::<LeafHeader>()],
        }
    }
}

impl InternalPage {
    pub fn new() -> Self {
        InternalPage {
            header: InternalHeader {
                page_header: Header {
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
    fn leaf_page_slot_test() {
        let page = LeafPage::new();
        assert_eq!(page._slot_buffer.len(), PAGE_SIZE - size_of::<LeafHeader>());
    }
}