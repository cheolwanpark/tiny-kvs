use super::{*, exceptions::*};
use crate::pagemanager::{PageId, PAGE_SIZE};
use crate::bytes::Bytes;
use std::mem::size_of;

const SLOT_BUFFER_SIZE: usize = PAGE_SIZE - size_of::<LeafHeader>();
const KEY_VALUE_SLOT_SIZE: usize = size_of::<KeyValueSlot>();

#[derive(SerializeDerive, DeserializeDerive)]
pub struct LeafPage {
    header: LeafHeader,
    _slot_buffer: Bytes<SLOT_BUFFER_SIZE>,
}

#[derive(SerializeDerive, DeserializeDerive)]
struct LeafHeader {
    page_header: PageHeader,
    _reserved: Bytes<98>,
    freespace: u32,
    right_sibling_id: PageId,
}

#[derive(Clone, Copy, SerializeDerive, DeserializeDerive)]
struct KeyValueSlot {
    key_len: u16,
    key_offset: u16,
    value_len: u16,
    value_offset: u16,
}

impl LeafPage {
    pub fn new() -> Self {
        LeafPage {
            header: LeafHeader {
                page_header: PageHeader {
                    parent_id: 0,
                    is_leaf: 1,
                    num_keys: 0,
                },
                _reserved: Bytes([0; 98]),
                freespace: SLOT_BUFFER_SIZE as u32,
                right_sibling_id: 0,
            },
            _slot_buffer: Bytes([0; SLOT_BUFFER_SIZE]),
        }
    }

    pub fn set_parent_id(&mut self, parent_id: PageId) {
        self.header.page_header.parent_id = parent_id;
    }

    pub fn get_parent_id(&self) -> PageId {
        self.header.page_header.parent_id
    }

    pub fn set_right_sibling_id(&mut self, right_sibling_id: PageId) {
        self.header.right_sibling_id = right_sibling_id;
    }

    pub fn get_right_sibling_id(&self) -> PageId {
        self.header.right_sibling_id
    }

    pub fn insert_record(&mut self, key: &str, value: &str) -> Result<usize> {
        if key.len() > KEY_LENGTH_LIMIT {
            return Err(BPTreeError::KeyLengthError(key.len(), KEY_LENGTH_LIMIT));
        }
        if value.len() > VALUE_LENGTH_LIMIT {
            return Err(BPTreeError::ValueLengthError(value.len(), VALUE_LENGTH_LIMIT));
        }
        let data_size = key.len() + value.len();
        let required_size = data_size + size_of::<KeyValueSlot>();
        if required_size > self.header.freespace as usize {
            return Err(BPTreeError::NotEnoughSpaceError(required_size, self.header.freespace as usize));
        }

        // find inserting index and offset
        let num_keys = self.header.page_header.num_keys;
        let min_offset = if num_keys > 0 {
            self.get_slot(num_keys-1)?.value_offset as usize
        } else {
            SLOT_BUFFER_SIZE
        };
        let mut inserting_idx = 0;
        let mut prev_offset = SLOT_BUFFER_SIZE;
        for idx in 0..num_keys {
            let slot = self.get_slot(idx)?;
            let slot_key = self.get_key(slot)?;
            if slot_key > key.to_string() {
                break;
            }
            inserting_idx = idx+1;
            prev_offset = slot.value_offset as usize;
        }

        // shift slots and data
        for idx in inserting_idx..num_keys {
            let mut slot = self.get_slot(idx)?;
            slot.key_offset -= data_size as u16;
            slot.value_offset -= data_size as u16;
            self.set_slot(idx, slot)?;
        }
        let buffer = &mut self._slot_buffer.0;
        buffer.copy_within(
            inserting_idx as usize*KEY_VALUE_SLOT_SIZE..num_keys as usize*KEY_VALUE_SLOT_SIZE,
            (inserting_idx+1) as usize*KEY_VALUE_SLOT_SIZE
        );
        buffer.copy_within(
            min_offset..prev_offset,
            min_offset - data_size
        );

        // insert slot and data
        let slot = KeyValueSlot {
            key_len: key.len() as u16,
            key_offset: (prev_offset - key.len()) as u16,
            value_len: value.len() as u16,
            value_offset: (prev_offset - key.len() - value.len()) as u16,
        };
        buffer[inserting_idx as usize*KEY_VALUE_SLOT_SIZE..(inserting_idx+1) as usize*KEY_VALUE_SLOT_SIZE]
        .copy_from_slice(&bincode::serialize(&slot).unwrap());
        buffer[slot.key_offset as usize..(slot.key_offset+slot.key_len) as usize].copy_from_slice(key.as_bytes());
        buffer[slot.value_offset as usize..(slot.value_offset+slot.value_len) as usize].copy_from_slice(value.as_bytes());
        
        // update header values
        self.header.page_header.num_keys += 1;
        self.header.freespace -= required_size as u32;

        Ok(required_size)
    }

    fn get_slot(&self, idx: u32) -> Result<KeyValueSlot> {
        if idx >= self.header.page_header.num_keys {
            return Err(BPTreeError::InvalidSlotIndexError(idx, self.header.page_header.num_keys));
        }

        let slot_offset = idx as usize * KEY_VALUE_SLOT_SIZE;
        let slot_bytes = &self._slot_buffer.0[slot_offset..slot_offset+KEY_VALUE_SLOT_SIZE];
        Ok(bincode::deserialize(slot_bytes).unwrap())
    }

    fn set_slot(&mut self, idx: u32, slot: KeyValueSlot) -> Result<()> {
        if idx >= self.header.page_header.num_keys {
            return Err(BPTreeError::InvalidSlotIndexError(idx, self.header.page_header.num_keys));
        }

        let slot_offset = idx as usize * KEY_VALUE_SLOT_SIZE;
        let slot_bytes = &mut self._slot_buffer.0[slot_offset..slot_offset+KEY_VALUE_SLOT_SIZE];
        slot_bytes.copy_from_slice(&bincode::serialize(&slot).unwrap());
        Ok(())
    }

    fn get_key(&self, slot: KeyValueSlot) -> Result<String> {
        let key_offset = slot.key_offset as usize;
        let key_len = slot.key_len as usize;
        let key = String::from_utf8(self._slot_buffer.0[key_offset..key_offset+key_len].to_vec())?;
        Ok(key)
    }
}

mod test {
    use crate::rand::rand_string;
    use super::*;

    #[test]
    fn leaf_page_slot_test() {
        let mut page = LeafPage::new();
        assert!(matches!(
            page.insert_record(&rand_string(100), "value"), 
            Err(BPTreeError::KeyLengthError(_, _))
        ));
        assert!(matches!(
            page.insert_record("key", &rand_string(300)), 
            Err(BPTreeError::ValueLengthError(_, _))
        ));
        
        let mut total_size: usize = 0;
        let buffer_size: usize = page._slot_buffer.0.len();
        const RAND_KEY_LEN: usize = 32;
        const RAND_VALUE_LEN: usize = 64;
        let single_slot_size = size_of::<KeyValueSlot>() + RAND_KEY_LEN + RAND_VALUE_LEN;
        let mut keys = Vec::new();
        while total_size+single_slot_size < buffer_size {
            let key = format!("key_start__{}__key{:03}", rand_string(RAND_KEY_LEN-19), keys.len());
            let value = format!("value_start__{}__value{:03}", rand_string(RAND_VALUE_LEN-23), keys.len());
            keys.push(key.clone());
            let inserted_size = page.insert_record(&key, &value).unwrap();
            total_size += inserted_size;
        }
        assert!(matches!(
            page.insert_record(&rand_string(RAND_KEY_LEN), &rand_string(RAND_VALUE_LEN)), 
            Err(BPTreeError::NotEnoughSpaceError(_, _))
        ));
        keys.sort();
        for i in 0..page.header.page_header.num_keys-1 {
            let key = page.get_key(page.get_slot(i).unwrap()).unwrap();
            let next_key = page.get_key(page.get_slot(i+1).unwrap()).unwrap();
            assert!(key <= next_key, "key: {}, next_key: {}", key, next_key);
            assert!(key == keys[i as usize], "key: {}, keys[i]: {}", key, keys[i as usize]);
        }
    }
}