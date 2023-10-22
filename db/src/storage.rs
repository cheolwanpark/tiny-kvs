use std::collections::HashMap;

pub struct Storage {
    map: HashMap<String, String>
}

impl Storage {
    pub fn new() -> Self {
        Self { 
            map: HashMap::new()
        }
    }

    pub fn put(&mut self, key: &str) {
        self.map.entry(key.to_string()).and_modify(|e| {
            *e = key.to_string();
        }).or_insert(key.to_string());
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.map.remove(key)
    }

    pub fn has(&self, key: &str) -> bool {
        match self.map.get(key) {
            Some(_) => true,
            None => false
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }
}