use std::{fs::{File, self}, path::Path, io::{BufWriter, Write}};

const DEFAULT_FILE_SIZE: u64 = 1024*1024*10;
const BLOCK_SIZE: usize = 4096;
const DEFAULT_FILE_NUM_BLOCKS: u64 = DEFAULT_FILE_SIZE / BLOCK_SIZE as u64;

pub struct DiskManager {
    file: File
}

struct HeaderPage {
    free_page_num: u64,
    num_pages: u64,
}

struct FreePage {
    next_free_page_num: u64,
}

impl DiskManager {
    pub fn new(path: &Path) -> Self {
        if path.exists() {
            match fs::OpenOptions::new().read(true).write(true).open(path) {
                Ok(file) => Self { file },
                Err(reason) => panic!("Couldn't open {} : {}", path.display(), reason)
            }
        } else {
            let mut file = match fs::OpenOptions::new().read(true).write(true).create(true).open(path)  {
                Ok(f) => f,
                Err(reason) => panic!("Couldn't create {} : {}", path.display(), reason)
            };
            let mut writer = BufWriter::new(&mut file);
            let mut buffer = [0; BLOCK_SIZE];
            let mut page = FreePage { next_free_page_num: 0 };
            unsafe {
                copy_to_buffer(&mut buffer, &HeaderPage {
                    free_page_num: DEFAULT_FILE_NUM_BLOCKS-1,
                    num_pages: DEFAULT_FILE_NUM_BLOCKS,
                });
            }
            if writer.write(&mut buffer).expect("Writing is blocked") != buffer.len() {
                panic!("Couldn't write more");
            }
            for i in 0..DEFAULT_FILE_NUM_BLOCKS-1 {
                page.next_free_page_num = i;
                unsafe { copy_to_buffer(&mut buffer, &page) }
                if writer.write(&mut buffer).expect("Writing is blocked") != buffer.len() {
                    panic!("Couldn't write more");
                }
            }
            writer.flush().expect("Couldn't flush writer");
            drop(writer);
            Self { file }
        }
    }
}

unsafe fn copy_to_buffer<T: Sized>(buffer: &mut [u8], obj: &T) {
    let obj_slice = core::slice::from_raw_parts(
        (obj as *const T) as *const u8,
        core::mem::size_of::<T>(),
    );
    buffer[..obj_slice.len()].copy_from_slice(obj_slice);
}