mod bplustree;

pub trait Index {
    fn insert(&mut self, elem: Box<dyn IndexAccessor>);
    fn remove(&mut self, key: &str);
    fn find(&self, key: &str) -> Option<Box<dyn IndexAccessor>>;
    fn update(&mut self, elem: Box<dyn IndexAccessor>);
}

pub trait IndexAccessor {
    fn key(&self) -> &str;
    fn value(&self) -> &str;
}