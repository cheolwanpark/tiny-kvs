use std::{collections::HashMap, rc::Rc, cell::RefCell};
use super::*;

pub struct InMemoryPageManager<T: PageManager> {
    page_manager: Box<T>,
    header_page: HeaderPage,
    frame_map: HashMap<PageId, usize>,
    frames: Vec<PageFrame>,
    clock_hand: usize,
}

#[derive(Clone)]
struct PageFrame {
    page_id: PageId,
    data: Rc<RefCell<Vec<u8>>>,
    pin_count: u32,
    is_dirty: bool,
    ref_bit: bool,
}

struct PageAccessorImpl {
    id: PageId,
    data: Rc<RefCell<Vec<u8>>>,
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

impl<P: PageManager> InMemoryPageManager<P> {
    pub fn new(num_frames: usize, mut page_manager: P) -> Self {
        let header_page = page_manager.read_header_page().unwrap();
        Self { 
            page_manager: Box::new(page_manager),
            header_page,
            frame_map: HashMap::new(),
            frames: vec![PageFrame {
                page_id: 0,
                data: Rc::new(RefCell::new(vec![0; PAGE_SIZE])),
                pin_count: 0,
                is_dirty: false,
                ref_bit: false,
            }; num_frames],
            clock_hand: 0,
        }
    }

    fn get_frame(&mut self, page_id: PageId) -> Option<&mut PageFrame> {
        if let Some(frame_index) = self.frame_map.get(&page_id) {
            Some(&mut self.frames[*frame_index])
        } else {
            None
        }
    }

    fn find_victim(&mut self) -> Result<usize> {
        let start_idx = self.clock_hand;
        let mut second_loop = false;
        loop {
            let frame = &mut self.frames[self.clock_hand];
            if frame.pin_count == 0 {
                if frame.ref_bit {
                    frame.ref_bit = false;
                } else {
                    return Ok(self.clock_hand);
                }
            }
            self.clock_hand = (self.clock_hand + 1) % self.frames.len();
            if self.clock_hand == start_idx {
                if second_loop {
                    return Err(PageManagerError::AllPagesArePinned);
                } else {
                    second_loop = true;
                }
            }
        }
    }

    fn evict_page(&mut self, idx: usize) -> Result<()> {
        let frame = &mut self.frames[idx];
        if frame.pin_count > 0 {
            return Err(PageManagerError::TryToEvictPinnedPage(frame.page_id));
        }
        if frame.is_dirty {
            self.page_manager.write_page(bytes_into_page(frame.page_id, frame.data.borrow().clone())).unwrap();
            frame.is_dirty = false;
        }
        self.frame_map.remove(&frame.page_id);
        Ok(())
    }
}

impl<P: PageManager> PageManager for InMemoryPageManager<P> {
    fn read_header_page(&mut self) -> Result<HeaderPage> {
        Ok(self.header_page.clone())
    }

    fn write_header_page(&mut self, header: HeaderPage) -> Result<()> {
        self.header_page = header.clone();
        self.page_manager.write_header_page(header)?;
        Ok(())
    }

    fn read_page(&mut self, id: PageId) -> Result<Box<dyn PageAccessor>> {
        let frame = match self.get_frame(id) {
            Some(frame) => frame,
            None => {
                let victim_idx = self.find_victim()?;
                self.evict_page(victim_idx)?;
                let frame = &mut self.frames[victim_idx];

                let page = self.page_manager.read_page(id)?;
                frame.page_id = id;
                frame.data.borrow_mut().copy_from_slice(&page.data());
                frame.pin_count = 0;
                frame.is_dirty = false;
                self.frame_map.insert(id, victim_idx);
                frame
            }
        };
        frame.pin_count += 1;
        frame.ref_bit = true;
        Ok(Box::new(PageAccessorImpl {
            id,
            data: frame.data.clone(),
        }))
    }

    fn write_page(&mut self, page: Box<dyn PageAccessor>) -> Result<()> {
        let frame = match self.get_frame(page.id()) {
            Some(frame) => frame,
            None => {
                let victim_idx = self.find_victim()?;
                self.evict_page(victim_idx)?;
                let frame = &mut self.frames[victim_idx];
                frame.page_id = page.id();
                frame.pin_count = 0;
                frame
            }
        };
        frame.data.borrow_mut().copy_from_slice(&page.data());
        frame.is_dirty = true;
        Ok(())
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        let page_id = self.page_manager.alloc_page()?;
        self.header_page = self.page_manager.read_header_page()?;
        Ok(page_id)
    }

    fn free_page(&mut self, id: PageId) -> Result<()> {
        self.page_manager.free_page(id)?;
        self.header_page = self.page_manager.read_header_page()?;
        Ok(())
    }
}

mod test {
    use std::{path::Path, fs};
    #[allow(unused_imports)]
    use rand::Rng;
    #[allow(unused_imports)]
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
    fn test_write_and_read_page() {
        let filename = "test_buffered_write_and_read_page.db";
        let _guard = CleanupFileGuard { path: Path::new(filename) };

        let path = Path::new(filename);
        let mut disk_manager = InMemoryPageManager::new(
            1024,
            DiskBasedPageManager::new(&path).unwrap()
        );

        let page_id = disk_manager.alloc_page().unwrap();
        let mut rng = rand::thread_rng();
        let data: Vec<u8> = (0..PAGE_SIZE).map(|_| rng.sample(rand::distributions::Alphanumeric) as u8).collect();
        disk_manager.write_page(bytes_into_page(page_id, data.clone())).unwrap();

        let read_page = disk_manager.read_page(page_id).unwrap();
        assert_eq!(read_page.data()[..], data[..]);
    }
}
