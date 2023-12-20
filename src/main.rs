use std::path::Path;
use db::pagemanager::DiskBasedPageManager;

fn main() {
    let _ = DiskBasedPageManager::new(Path::new("test.db")).unwrap();
}
