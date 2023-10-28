use std::{fs::{File, self}, path::Path, io::{BufWriter, Write, Read, Seek, SeekFrom}};

type PageId = u64;
type PageBuffer = [u8; PAGE_SIZE];

const DEFAULT_FILE_SIZE: u64 = 1024*1024*10;
const PAGE_SIZE: usize = 4096;
const DEFAULT_FILE_NUM_PAGES: u64 = DEFAULT_FILE_SIZE / PAGE_SIZE as u64;

pub struct DiskManager {
    file: File,
    header: Option<HeaderPage>,
}

#[derive(Clone)]
struct HeaderPage {
    free_page_id: PageId,
    num_pages: u64,
}

#[derive(Clone)]
struct FreePage {
    next_free_page_id: PageId,
}

impl DiskManager {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        if path.exists() {
            let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
            Ok(Self { file, header: None })
        } else {
            match fs::OpenOptions::new().read(true).write(true).create(true).open(path)  {
                Ok(mut file) => {
                    let mut writer = BufWriter::new(&mut file);
                    let mut buffer = [0; PAGE_SIZE];
                    copy_to_buffer(&HeaderPage {
                        free_page_id: 0,
                        num_pages: 0,
                    }, &mut buffer);
                    writer.write(&mut buffer)?;
                    writer.flush()?;
                    drop(writer);
                    let mut disk_manager = Self { file, header: None };
                    disk_manager.append_free_pages(DEFAULT_FILE_NUM_PAGES - 1)?;
                    Ok(disk_manager)
                },
                Err(reason) => panic!("Couldn't create {} : {}", path.display(), reason)
            }
        }
    }

    fn append_free_pages(&mut self, num_pages: u64) -> std::io::Result<()> {
        let mut header = self.read_header_page()?;
        let mut writer = BufWriter::new(&mut self.file);
        let mut buffer = [0; PAGE_SIZE];
        let mut page = FreePage { next_free_page_id: 0 };
        let mut last_page_id = header.free_page_id;
        writer.seek(SeekFrom::End(0))?;
        for i in 1..=num_pages {
            page.next_free_page_id = last_page_id;
            copy_to_buffer(&page, &mut buffer);
            if writer.write(&mut buffer)? != buffer.len() {
                panic!("Couldn't write more");
            }
            last_page_id = header.free_page_id + i;
        }
        header.free_page_id = last_page_id;
        header.num_pages += num_pages;
        writer.flush()?;
        drop(writer);
        self.write_header_page(header)?;
        Ok(())
    }

    fn read_header_page(&mut self) -> std::io::Result<HeaderPage> {
        match self.header.clone() {
            Some(header) => Ok(header),
            None => {
                let mut buffer = [0u8; PAGE_SIZE];
                self.file.rewind()?;
                self.file.read(&mut buffer)?;
                let mut header = HeaderPage {
                    free_page_id: 0,
                    num_pages: 0
                };
                copy_to_obj(&buffer, &mut header);
                self.header = Some(header.clone());
                Ok(header)
            }
        }
    }

    fn write_header_page(&mut self, header: HeaderPage) -> std::io::Result<usize> {
        let mut buffer = [0u8; PAGE_SIZE];
        copy_to_buffer(&header, &mut buffer);
        self.header = Some(header);
        self.write_page(0, &buffer)
    }

    pub fn read_page(&mut self, id: PageId) -> std::io::Result<PageBuffer> {
        if id > self.read_header_page()?.num_pages {
            panic!("Invalid id is used");
        }
        let mut buffer = [0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.read(&mut buffer)?;
        Ok(buffer)
    }

    pub fn write_page(&mut self, id: PageId, buffer: &PageBuffer) -> std::io::Result<usize> {
        if id > self.read_header_page()?.num_pages {
            panic!("Invalid id is used");
        }
        self.file.seek(SeekFrom::Start(id * PAGE_SIZE as u64))?;
        self.file.write(buffer)
    }
}

fn copy_to_buffer<T: Sized>(obj: &T, buffer: &mut PageBuffer) {
    let obj_slice;
    unsafe {
        obj_slice = core::slice::from_raw_parts(
            (obj as *const T) as *const u8,
            core::mem::size_of::<T>(),
        );
    }
    buffer[..obj_slice.len()].copy_from_slice(obj_slice);
}

fn copy_to_obj<T: Sized>(buffer: &PageBuffer, obj: &mut T) {
    let obj_slice;
    unsafe {
        obj_slice = core::slice::from_raw_parts_mut(
            (obj as *mut T) as *mut u8,
            core::mem::size_of::<T>(),
        );
    }
    obj_slice.copy_from_slice(&buffer[..obj_slice.len()]);
}