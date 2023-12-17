use std::path::Path;

use db::diskmanager::DiskManager;
fn main() {
    let _ = DiskManager::new(Path::new("test.db")).unwrap();
}
