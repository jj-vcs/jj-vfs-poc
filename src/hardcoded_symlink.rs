use std::path::PathBuf;

use async_trait::async_trait;

use crate::jj_error::JjError;
use crate::virtual_file::{FileAttributes, FileType, VirtualFile};

pub struct HardcodedSymlink {
    target: PathBuf,
}

impl HardcodedSymlink {
    pub fn new(target: PathBuf) -> Self {
        Self { target }
    }
}

#[async_trait]
impl VirtualFile for HardcodedSymlink {
    async fn read_link(&self) -> Result<PathBuf, JjError> {
        Ok(self.target.clone())
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes {
            size: self.target.as_os_str().len() as u64,
            file_type: FileType::Symlink,
            created: std::time::SystemTime::UNIX_EPOCH,
            modified: std::time::SystemTime::UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Symlink)
    }
}
