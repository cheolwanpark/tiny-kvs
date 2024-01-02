use std::{fs::{File, self}, path::Path, io::{Write, Read, Seek, SeekFrom}};
use serde_derive::{Serialize as SerializeDerive, Deserialize as DeserializeDerive};
use serde::{Serialize, de::DeserializeOwned};
use bincode;
#[allow(unused_imports)]
use rand::Rng;
use super::*;

pub const DEFAULT_FILE_SIZE: u64 = 1024*1024*10;
pub const DEFAULT_FILE_NUM_PAGES: u64 = DEFAULT_FILE_SIZE / PAGE_SIZE as u64;

#[derive(Clone, Default, SerializeDerive, DeserializeDerive)]
struct HeaderPage {
    free_page_id: PageId,
    num_pages: u64,
}

#[derive(Clone, Default, SerializeDerive, DeserializeDerive)]
struct FreePage {
    next_free_page_id: PageId,
}

pub struct DiskBasedPageManager {
    file: File,
}

impl PageManager for DiskBasedPageManager {
    fn read_page<T: DeserializeOwned>(&mut self, id: PageId) -> std::io::Result<T> {
        if id > self.read_header_page()?.num_pages {
            panic!("Invalid id is used");
        }
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.read_exact(buffer.as_mut_slice())?;
        Ok(bincode::deserialize(&buffer).unwrap())
    }

    fn write_page<T: Serialize>(&mut self, id: PageId, obj: &T) -> std::io::Result<()> {
        self.write_page_nosync(id, obj)?;
        self.file.sync_data()
    }

    fn alloc_page(&mut self) -> std::io::Result<PageId> {
        let mut header = self.read_header_page()?;
        if header.free_page_id == 0 {
            self.append_free_pages(header.num_pages)?;
            header = self.read_header_page()?;
        }
        let free_page_id = header.free_page_id;
        let free_page = self.read_page::<FreePage>(free_page_id)?;
        header.free_page_id = free_page.next_free_page_id;
        self.write_header_page(header)?;
        Ok(free_page_id)
    }

    fn free_page(&mut self, id: PageId) -> std::io::Result<()> {
        let mut header = self.read_header_page()?;
        if id > header.num_pages {
            panic!("Invalid id is used");
        }
        let page = FreePage { next_free_page_id: header.free_page_id };
        header.free_page_id = id;
        self.write_header_page_nosync(header)?;
        self.write_page(id, &page)?;
        Ok(())
    }
}

impl DiskBasedPageManager {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        if path.exists() {
            let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
            Ok(Self { file })
        } else {
            match fs::OpenOptions::new().read(true).write(true).create(true).open(path)  {
                Ok(file) => {
                    let mut disk_manager = Self { file };
                    disk_manager.write_header_page(HeaderPage {
                        free_page_id: 0,
                        num_pages: 0,
                    })?;
                    disk_manager.append_free_pages(DEFAULT_FILE_NUM_PAGES - 1)?;
                    Ok(disk_manager)
                },
                Err(reason) => panic!("Couldn't create {} : {}", path.display(), reason)
            }
        }
    }

    fn append_free_pages(&mut self, num_pages: u64) -> std::io::Result<()> {
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

    fn read_header_page(&mut self) -> std::io::Result<HeaderPage> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.rewind()?;
        self.file.read_exact(buffer.as_mut_slice())?;
        Ok(bincode::deserialize(&buffer).unwrap())
    }

    fn write_header_page(&mut self, header: HeaderPage) -> std::io::Result<()> {
        self.write_header_page_nosync(header)?;
        self.file.sync_data()
    }

    fn write_header_page_nosync(&mut self, header: HeaderPage) -> std::io::Result<usize> {
        let mut buffer = [0u8; PAGE_SIZE];
        bincode::serialize_into(&mut buffer.as_mut_slice(), &header).unwrap();
        self.file.rewind()?;
        self.file.write(&buffer)
    }

    fn write_page_nosync<T: Serialize>(&mut self, id: PageId, obj: &T) -> std::io::Result<usize> {
        if id > self.read_header_page()?.num_pages {
            panic!("Invalid id is used");
        }
        let mut buffer = [0u8; PAGE_SIZE];
        bincode::serialize_into(&mut buffer.as_mut_slice(), &obj).unwrap();
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.write(&buffer)
    }
}

#[cfg(test)]
mod test {
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
            let free_page = disk_manager.read_page::<FreePage>(cur_free_page_id).unwrap();
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
        let mut rng = rand::thread_rng();
        let data: Vec<u8> = (0..PAGE_SIZE/2).map(|_| rng.sample(rand::distributions::Alphanumeric) as u8).collect();
        disk_manager.write_page(page_id, &data).unwrap();

        let read_data = disk_manager.read_page::<Vec<u8>>(page_id).unwrap();
        assert_eq!(read_data, data);
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