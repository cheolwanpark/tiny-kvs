use std::path::Path;

use db::diskmanager::DiskManager;
fn main() {
    DiskManager::new(Path::new("test.db"));
}
