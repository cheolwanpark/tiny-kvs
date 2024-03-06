use std::{fs::{File, self}, path::Path, io::{Write, Read, Seek, SeekFrom}, cell::RefCell};
use bincode;
use super::*;

pub const DEFAULT_FILE_SIZE: u64 = 1024*1024*10;
pub const DEFAULT_FILE_NUM_PAGES: u64 = DEFAULT_FILE_SIZE / PAGE_SIZE as u64;


#[derive(Clone, Default, SerializeDerive, DeserializeDerive)]
struct FreePage {
    next_free_page_id: PageId,
}

pub struct DiskBasedPageManager {
    file: File,
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

impl PageManager for DiskBasedPageManager {
    fn read_header_page(&mut self) -> Result<HeaderPage> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.rewind()?;
        self.file.read_exact(buffer.as_mut_slice())?;
        Ok(bincode::deserialize(&buffer).unwrap())
    }

    fn write_header_page(&mut self, header: HeaderPage) -> Result<()> {
        self.write_header_page_nosync(header)?;
        self.file.sync_data()?;
        Ok(())
    }

    fn read_page(&mut self, id: PageId) -> Result<Box<dyn PageAccessor>> {
        if id > self.read_header_page()?.num_pages {
            return Err(PageManagerError::InvalidPageId(id));
        }
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.read_exact(buffer.as_mut_slice())?;
        Ok(Box::new(PageAccessorImpl {
            id,
            data: RefCell::new(buffer),
        }))
    }

    fn write_page(&mut self, page: Box<dyn PageAccessor>) -> Result<()> {
        self.write_page_nosync(page)?;
        self.file.sync_data()?;
        Ok(())
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        let mut header = self.read_header_page()?;
        if header.free_page_id == 0 {
            self.append_free_pages(header.num_pages)?;
            header = self.read_header_page()?;
        }
        let free_page_id = header.free_page_id;
        let free_page = self.read_page(free_page_id)?;
        let free_page: FreePage = from_page(free_page);
        header.free_page_id = free_page.next_free_page_id;
        self.write_header_page(header)?;
        Ok(free_page_id)
    }

    fn free_page(&mut self, id: PageId) -> Result<()> {
        let mut header = self.read_header_page()?;
        if id > header.num_pages {
            return Err(PageManagerError::InvalidPageId(id));
        }
        let page = FreePage { next_free_page_id: header.free_page_id };
        header.free_page_id = id;
        self.write_header_page_nosync(header)?;
        self.write_page(into_page(id, &page))?;
        Ok(())
    }
}

impl DiskBasedPageManager {
    pub fn new(path: &Path) -> Result<Self> {
        if path.exists() {
            let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
            Ok(Self { file })
        } else {
            let file = fs::OpenOptions::new().read(true).write(true).create(true).open(path)?;
            let mut disk_manager = Self { file };
            disk_manager.write_header_page(HeaderPage {
                free_page_id: 0,
                num_pages: 0,
            })?;
            disk_manager.append_free_pages(DEFAULT_FILE_NUM_PAGES - 1)?;
            Ok(disk_manager)
        }
    }

    fn append_free_pages(&mut self, num_pages: u64) -> Result<()> {
        let mut header = self.read_header_page()?;
        let mut buffer = [0; PAGE_SIZE];
        let mut page = FreePage { next_free_page_id: 0 };
        let mut last_page_id = header.free_page_id;

        self.file.seek(SeekFrom::End(0))?;
        for i in 1..=num_pages {
            page.next_free_page_id = last_page_id;
            bincode::serialize_into(&mut buffer.as_mut_slice(), &page).unwrap();
            self.file.write(&buffer)?;
            last_page_id = header.free_page_id + i;
        }

        header.free_page_id = last_page_id;
        header.num_pages += num_pages;
        self.write_header_page(header)
    }

    fn write_header_page_nosync(&mut self, header: HeaderPage) -> Result<usize> {
        let mut buffer = [0u8; PAGE_SIZE];
        bincode::serialize_into(&mut buffer.as_mut_slice(), &header).unwrap();
        self.file.rewind()?;
        Ok(self.file.write(&buffer)?)
    }

    fn write_page_nosync(&mut self, page: Box<dyn PageAccessor>) -> Result<usize> {
        if page.id() > self.read_header_page()?.num_pages {
            return Err(PageManagerError::InvalidPageId(page.id()));
        }
        self.file.seek(SeekFrom::Start(page.id() * PAGE_SIZE as u64))?;
        Ok(self.file.write(&page.data())?)
    }
}

#[cfg(test)]
mod test {
    use crate::rand::rand_bytes;

    use super::*;

    struct CleanupFileGuard<'a> {
        path: &'a Path,
    }
    
    impl<'a> Drop for CleanupFileGuard<'a> {
        fn drop(&mut self) {
            fs::remove_file(self.path).unwrap();
        }
    }

    #[test]
    fn test_new() {
        let filename = "test_new.db";
        let _guard = CleanupFileGuard{ path: Path::new(filename)};

        let path = Path::new(filename);
        let _ = DiskBasedPageManager::new(&path);

        let metadata = fs::metadata(path).unwrap();
        assert_eq!(metadata.len(), DEFAULT_FILE_SIZE);
    }

    #[test]
    fn test_alloc_and_free_pages() {
        let filename = "test_alloc_and_free_pages.db";
        let _guard = CleanupFileGuard{ path: Path::new(filename)};

        let path = Path::new(filename);
        let mut disk_manager = DiskBasedPageManager::new(&path).unwrap();

        let free_page_id = disk_manager.alloc_page().unwrap();
        let allocated_page_id = disk_manager.alloc_page().unwrap();
        disk_manager.free_page(free_page_id).unwrap();

        let header = disk_manager.read_header_page().unwrap();
        let mut cur_free_page_id = header.free_page_id;
        let mut free_page_id_exist = false;
        while cur_free_page_id != 0 {
            assert_ne!(cur_free_page_id, allocated_page_id);
            if cur_free_page_id == free_page_id {
                free_page_id_exist = true;
            }
            let free_page: FreePage = from_page(disk_manager.read_page(cur_free_page_id).unwrap());
            cur_free_page_id = free_page.next_free_page_id;
        }
        assert!(free_page_id_exist);
    }

    #[test]
    fn test_write_and_read_page() {
        let filename = "test_write_and_read_page.db";
        let _guard = CleanupFileGuard { path: Path::new(filename) };

        let path = Path::new(filename);
        let mut disk_manager = DiskBasedPageManager::new(&path).unwrap();

        let page_id = disk_manager.alloc_page().unwrap();
        let data = rand_bytes(PAGE_SIZE);
        disk_manager.write_page(bytes_into_page(page_id, data.clone())).unwrap();

        let read_page = disk_manager.read_page(page_id).unwrap();
        assert_eq!(read_page.data()[..], data[..]);
    }

    #[test]
    #[ignore]
    fn test_db_growing() {
        let filename = "test_db_growing.db";
        let _guard = CleanupFileGuard{ path: Path::new(filename)};

        let path = Path::new(filename);
        let mut disk_manager = DiskBasedPageManager::new(&path).unwrap();

        for _ in 0..DEFAULT_FILE_NUM_PAGES {
            disk_manager.alloc_page().unwrap();
        }

        // num_pages should be doubled
        // num_pages is DEFAULT_FILE_NUM_PAGES - 1 when db is created (first page is header)
        // after grow, num_pages should be (DEFAULT_FILE_NUM_PAGES - 1) * 2
        // and file size should be (DEFAULT_FILE_NUM_PAGES - 1) * 2 * PAGE_SIZE including header page
        let expected_size = (DEFAULT_FILE_NUM_PAGES * 2 - 1) * PAGE_SIZE as u64;

        let metadata = fs::metadata(path).unwrap();
        assert_eq!(metadata.len(), expected_size);
    }
}