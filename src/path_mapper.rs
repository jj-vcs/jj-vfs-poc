use std::path::Path;
use std::pin::Pin;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::Stream;
use ustr::Ustr;

use crate::jj_error::JjError;

pub enum FileType {
    File,
    Directory,
}

pub struct DirectoryEntry {
    pub name: Ustr,
    pub file_type: FileType,
}

impl DirectoryEntry {
    pub fn new(name: &str, file_type: FileType) -> Self {
        Self {
            name: Ustr::from(name),
            file_type,
        }
    }
}

/// This trait represents a file in our virtual file system. This can either be
/// a normal file you can read from or for example a directory, in which case
/// you can list its contents.
///
/// A `VirtualFile` is not meant to be created by the user, but instead returned
/// by a `PathMapper`. The underlying implementation of `VirtualFile` should
/// contain all the necessary logic for interacting with the underlying
/// filesystem (jj-lib in this case) to get the data for a file.
#[async_trait]
pub trait VirtualFile: Send + Sync {
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError>;
    async fn list<'a>(
        &'a self,
    ) -> Result<Box<dyn Stream<Item = DirectoryEntry> + Send + 'a>, JjError>;
    async fn size(&self) -> Result<u64, JjError>;
    async fn file_type(&self) -> Result<FileType, JjError>;
}

/// This trait represents the VFS mountpoint structure by mapping a given
/// absolute path to a `VirtualFile`.
#[async_trait]
pub trait PathMapper: Send + Sync {
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError>;
}
