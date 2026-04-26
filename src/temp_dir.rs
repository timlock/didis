use std::ops::{AddAssign, Deref};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::{fs, io};

static ID: LazyLock<Mutex<u64>> = LazyLock::new(|| Mutex::new(0));

fn next_id() -> u64 {
    let mut guard = ID.lock().unwrap();
    guard.add_assign(1);
    guard.clone()
}

#[derive(Debug)]
pub struct TempDir {
    path: Box<Path>,
}

impl TempDir {
    pub fn create_at(path: PathBuf) -> io::Result<TempDir> {
        match fs::remove_dir_all(&path) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
            Ok(()) => {}
        }
        fs::create_dir_all(&path)?;
        Ok(TempDir {
            path: path.into_boxed_path(),
        })
    }

    pub fn create(path: PathBuf) -> io::Result<TempDir> {
        let id = next_id();
        let path = path.join(id.to_string());
        TempDir::create_at(path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // TODO check if unwrap makes sense
        fs::remove_dir_all(&self.path).unwrap();
    }
}
