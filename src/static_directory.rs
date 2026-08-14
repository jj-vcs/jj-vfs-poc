use std::time::UNIX_EPOCH;

use async_trait::async_trait;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct StaticDirectory {
    children: Vec<DirectoryEntry>,
}

impl StaticDirectory {
    pub fn new(children: Vec<DirectoryEntry>) -> Self {
        Self { children }
    }
}

#[async_trait]
impl VirtualFile for StaticDirectory {
    async fn list(&self) -> Result<DirectoryStream, JjError> {
        Ok(Box::pin(futures::stream::iter(self.children.clone())))
    }

    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes {
            size: 0,
            file_type: FileType::Directory,
            created: UNIX_EPOCH, // TODO: implement proper timestamps
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Directory)
    }
}
