use super::*;

pub struct InMemoryPageManager<T: PageManager> {
    page_manager: Box<T>
}

impl<P: PageManager> InMemoryPageManager<P> {
    pub fn new(page_manager: P) -> Self {
        Self { page_manager: Box::new(page_manager) }
    }
}

impl<P: PageManager> PageManager for InMemoryPageManager<P> {
    fn read_page<T: DeserializeOwned>(&mut self, id: PageId) -> std::io::Result<T> {
        self.page_manager.read_page(id)
    }

    fn write_page<T: Serialize>(&mut self, id: PageId, obj: &T) -> std::io::Result<()> {
        self.page_manager.write_page(id, obj)
    }

    fn alloc_page(&mut self) -> std::io::Result<PageId> {
        self.page_manager.alloc_page()
    }

    fn free_page(&mut self, id: PageId) -> std::io::Result<()> {
        self.page_manager.free_page(id)
    }
}

mod test {
    use super::*;
    use std::{path::Path, fs};
    use crate::pagemanager::diskbased::PAGE_SIZE;
    use rand::Rng;

    struct CleanupFileGuard<'a> {
        path: &'a Path,
    }
    
    impl<'a> Drop for CleanupFileGuard<'a> {
        fn drop(&mut self) {
            fs::remove_file(self.path).unwrap();
        }
    }


    #[test]
    fn test_write_and_read_page() {
        let filename = "test_buffered_write_and_read_page.db";
        let _guard = CleanupFileGuard { path: Path::new(filename) };

        let path = Path::new(filename);
        let mut disk_manager = InMemoryPageManager::new(
            DiskBasedPageManager::new(&path).unwrap()
        );

        let page_id = disk_manager.alloc_page().unwrap();
        let mut rng = rand::thread_rng();
        let data: Vec<u8> = (0..PAGE_SIZE/2).map(|_| rng.sample(rand::distributions::Alphanumeric) as u8).collect();
        disk_manager.write_page(page_id, &data).unwrap();

        let read_data = disk_manager.read_page::<Vec<u8>>(page_id).unwrap();
        assert_eq!(read_data, data);
    }
}
