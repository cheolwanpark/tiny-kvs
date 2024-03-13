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

    pub fn can_insert_record(&self, key: &str, value: &str) -> bool {
        if key.len() > KEY_LENGTH_LIMIT || value.len() > VALUE_LENGTH_LIMIT {
            return false;
        }
        let data_size = key.len() + value.len();
        let required_size = data_size + KEY_VALUE_SLOT_SIZE;
        required_size <= self.header.freespace as usize
    }

    pub fn insert_record(&mut self, key: &str, value: &str) -> Result<usize> {
        if key.len() > KEY_LENGTH_LIMIT {
            return Err(BPTreeError::KeyLengthError(key.len(), KEY_LENGTH_LIMIT));
        }
        if value.len() > VALUE_LENGTH_LIMIT {
            return Err(BPTreeError::ValueLengthError(value.len(), VALUE_LENGTH_LIMIT));
        }
        let data_size = key.len() + value.len();
        let required_size = data_size + KEY_VALUE_SLOT_SIZE;
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

    pub fn remove_record(&mut self, key: &str) -> Result<usize> {
        // find deleting slot
        let num_keys = self.header.page_header.num_keys;
        let (removing_idx, removing_slot) = self.find_slot(key)?;

        let removing_data_size = removing_slot.key_len + removing_slot.value_len;
        let removing_size = removing_data_size as usize + KEY_VALUE_SLOT_SIZE;
        let mut min_offset = removing_slot.value_offset as usize;

        // update slots offset value and get min offset to shift data
        for idx in (removing_idx+1)..num_keys {
            let mut slot = self.get_slot(idx)?;
            min_offset = slot.value_offset as usize;
            slot.key_offset += removing_data_size;
            slot.value_offset += removing_data_size;
            self.set_slot(idx, slot)?;
        }

        // shift slots and data
        let buffer = &mut self._slot_buffer.0;
        buffer.copy_within(
            (removing_idx+1) as usize*KEY_VALUE_SLOT_SIZE..num_keys as usize*KEY_VALUE_SLOT_SIZE,
            removing_idx as usize*KEY_VALUE_SLOT_SIZE
        );
        buffer.copy_within(
            min_offset..removing_slot.value_offset as usize,
            min_offset + removing_data_size as usize
        );

        // update header values
        self.header.page_header.num_keys -= 1;
        self.header.freespace += removing_size as u32;

        Ok(removing_size)
    }

    pub fn find_record(&self, key: &str) -> Result<String> {
        let (_, slot) = self.find_slot(key)?;
        let value_offset = slot.value_offset as usize;
        let value_len = slot.value_len as usize;
        let value = String::from_utf8(self._slot_buffer.0[value_offset..value_offset+value_len].to_vec())?;
        Ok(value)
    }

    // split page into two pages, returned page is the right page
    pub fn split(&mut self) -> Result<(String, LeafPage)> {
        // find split index
        let num_keys = self.header.page_header.num_keys;
        let mut total_size = 0;
        let mut split_idx = 0;
        while total_size < SLOT_BUFFER_SIZE/2 && split_idx < num_keys{
            let slot = self.get_slot(split_idx)?;
            total_size += KEY_VALUE_SLOT_SIZE + slot.key_len as usize + slot.value_len as usize;
            split_idx += 1;
        }
        split_idx -= 1;
        let split_key = self.get_key(self.get_slot(split_idx)?)?;
        
        // create right page
        let mut right_page = LeafPage::new();
        right_page.header.page_header.num_keys = num_keys - split_idx;

        // fill right page with slots and data
        let mut offset = SLOT_BUFFER_SIZE as u16;
        let mut min_orig_offset = SLOT_BUFFER_SIZE;
        let mut data_len = 0;
        for idx in split_idx..num_keys {
            let mut slot = self.get_slot(idx)?;
            min_orig_offset = slot.value_offset as usize;
            slot.key_offset = offset - slot.key_len;
            slot.value_offset = slot.key_offset - slot.value_len;
            offset = slot.value_offset;
            data_len += slot.key_len as usize + slot.value_len as usize;
            right_page.set_slot(idx-split_idx, slot)?;
        }
        let orig_buffer = &self._slot_buffer.0;
        let new_buffer = &mut right_page._slot_buffer.0;
        new_buffer[offset as usize..].copy_from_slice(&orig_buffer[min_orig_offset..min_orig_offset+data_len]);
        
        // update header values
        let total_len = data_len + (num_keys-split_idx) as usize*KEY_VALUE_SLOT_SIZE;
        self.header.page_header.num_keys = split_idx;
        self.header.freespace += total_len as u32;
        right_page.header.freespace = SLOT_BUFFER_SIZE as u32 - total_len as u32;

        Ok((split_key, right_page))
    }

    fn find_slot(&self, key: &str) -> Result<(u32, KeyValueSlot)> {
        let num_keys = self.header.page_header.num_keys;
        for idx in 0..num_keys {
            let slot = self.get_slot(idx)?;
            let slot_key = self.get_key(slot)?;
            if slot_key == key {
                return Ok((idx, slot));
            }
        }
        Err(BPTreeError::KeyNotFoundError(key.to_string()))
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
    use std::collections::HashMap;

    use crate::rand::{rand_string, rand_usize};
    use super::*;

    #[test]
    fn leaf_page_slot_test() {
        let mut page = LeafPage::new();
        
        // key, value length error checking
        assert!(matches!(
            page.insert_record(&rand_string(100), "value"), 
            Err(BPTreeError::KeyLengthError(_, _))
        ));
        assert!(matches!(
            page.insert_record("key", &rand_string(300)), 
            Err(BPTreeError::ValueLengthError(_, _))
        ));
        
        // insertion phase 1
        let mut total_size: usize = 0;
        let buffer_size: usize = page._slot_buffer.0.len();
        const RAND_KEY_LEN: usize = 32;
        const RAND_VALUE_LEN: usize = 64;
        let single_slot_size = size_of::<KeyValueSlot>() + RAND_KEY_LEN + RAND_VALUE_LEN;
        let mut records = HashMap::new();
        let mut keys = Vec::new();
        while total_size+single_slot_size < buffer_size {
            let key = format!("key_start__{}__key{:03}", rand_string(RAND_KEY_LEN-19), keys.len());
            let value = format!("value_start__{}__value{:03}", rand_string(RAND_VALUE_LEN-23), keys.len());
            records.insert(key.clone(), value.clone());
            keys.push(key.clone());
            let inserted_size = page.insert_record(&key, &value).unwrap();
            total_size += inserted_size;
        }
        assert!(matches!(
            page.insert_record(&rand_string(RAND_KEY_LEN), &rand_string(RAND_VALUE_LEN)), 
            Err(BPTreeError::NotEnoughSpaceError(_, _))
        ));
        keys.sort();
        for i in 0..page.header.page_header.num_keys {
            let key = page.get_key(page.get_slot(i).unwrap()).unwrap();
            let value = page.find_record(&key).unwrap();
            assert_eq!(key, keys[i as usize]);
            assert_eq!(value, records[&key]);
        }

        // deletion phase
        const DELETING_N: usize = 10;
        let mut deleted_keys = Vec::new();
        for _ in 0..DELETING_N {
            let idx = rand_usize(0, keys.len());
            let key = keys.remove(idx);
            records.remove(&key);
            page.remove_record(&key).unwrap();
            deleted_keys.push(key);
        }
        keys.sort();
        for i in 0..page.header.page_header.num_keys {
            let key = page.get_key(page.get_slot(i).unwrap()).unwrap();
            let value = page.find_record(&key).unwrap();
            assert_eq!(key, keys[i as usize]);
            assert_eq!(value, records[&key]);
        }

        for key in deleted_keys {
            assert!(matches!(
                page.find_record(&key), 
                Err(BPTreeError::KeyNotFoundError(_))
            ));
        }

        // insertion phase 2
        for i in 0..DELETING_N {
            let key = format!("key_start__{}__del{:03}", rand_string(RAND_KEY_LEN-19), i);
            let value = format!("value_start__{}__del{:03}", rand_string(RAND_VALUE_LEN-21), i);
            records.insert(key.clone(), value.clone());
            keys.push(key.clone());
            page.insert_record(&key, &value).unwrap();
        }
        keys.sort();
        for i in 0..page.header.page_header.num_keys {
            let key = page.get_key(page.get_slot(i).unwrap()).unwrap();
            let value = page.find_record(&key).unwrap();
            assert_eq!(key, keys[i as usize]);
            assert_eq!(value, records[&key]);
        }
    }

    #[test]
    fn leaf_page_split_test() {
        let mut page = LeafPage::new();
        
        // insertion
        let mut total_size: usize = 0;
        let buffer_size: usize = page._slot_buffer.0.len();
        const RAND_KEY_LEN: usize = 32;
        const RAND_VALUE_LEN: usize = 64;
        let single_slot_size = size_of::<KeyValueSlot>() + RAND_KEY_LEN + RAND_VALUE_LEN;
        let mut records = HashMap::new();
        let mut keys = Vec::new();
        while total_size+single_slot_size < buffer_size {
            let key = format!("key_start__{}__key{:03}", rand_string(RAND_KEY_LEN-19), keys.len());
            let value = format!("value_start__{}__value{:03}", rand_string(RAND_VALUE_LEN-23), keys.len());
            records.insert(key.clone(), value.clone());
            keys.push(key.clone());
            let inserted_size = page.insert_record(&key, &value).unwrap();
            total_size += inserted_size;
        }
        assert!(!page.can_insert_record(&rand_string(RAND_KEY_LEN), &rand_string(RAND_VALUE_LEN)));
        keys.sort();
        
        // split and check validity
        let (split_key, right_page) = page.split().unwrap();
        let left_num_keys = page.header.page_header.num_keys;
        let right_num_keys = right_page.header.page_header.num_keys;
        assert_eq!(split_key, right_page.get_key(right_page.get_slot(0).unwrap()).unwrap());
        for i in 0..left_num_keys {
            let key = page.get_key(page.get_slot(i).unwrap()).unwrap();
            let value = page.find_record(&key).unwrap();
            assert_eq!(key, keys[i as usize]);
            assert_eq!(value, records[&key]);
        }
        for i in 0..right_num_keys {
            let key = right_page.get_key(right_page.get_slot(i).unwrap()).unwrap();
            let value = right_page.find_record(&key).unwrap();
            assert_eq!(key, keys[(i+left_num_keys) as usize]);
            assert_eq!(value, records[&key]);
        }
    }
}