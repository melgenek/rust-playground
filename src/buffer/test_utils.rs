use std::env::temp_dir;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{fs, io};

use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::buffer::constants::PAGE_SIZE;
use crate::buffer::types::PageData;
use crate::types::{thread_rng, RngCore};

#[cfg(test)]
pub(super) struct TempFile {
    path: PathBuf,
}

#[cfg(test)]
impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_file(&self.path).unwrap()
    }
}

#[cfg(test)]
impl TempFile {
    pub fn new() -> io::Result<Self> {
        let path = TempFile::temp_file_name();
        File::create(&path)?;
        Ok(TempFile { path })
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn temp_file_name() -> PathBuf {
        let mut dir = temp_dir();
        let file_name: String = thread_rng().sample_iter(&Alphanumeric).take(20).map(char::from).collect();
        dir.push(file_name);
        dir.set_extension(".db");
        return dir;
    }
}

#[cfg(test)]
pub(super) fn random_page() -> PageData {
    let mut page: PageData = [0; PAGE_SIZE];
    thread_rng().fill_bytes(page.as_mut_slice());
    page
}
