use std::env::temp_dir;
use std::ops::AddAssign;
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

    pub fn new() -> io::Result<TempDir> {
        let id = next_id();
        let temp_dir = temp_dir();
        TempDir::create_at(temp_dir.join(id.to_string()))
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
#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn create_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path.join("temp_file");

        let mut file = File::create(&file_path)?;
        write!(&mut file, "Hello temp_dir")?;
        drop(file);

        let content = fs::read_to_string(&file_path)?;
        assert_eq!("Hello temp_dir", content);

        drop(temp_dir);

        assert!(
            matches!(fs::read_to_string(&file_path), Err(err) if err.kind() == io::ErrorKind::NotFound)
        );

        Ok(())
    }
}
