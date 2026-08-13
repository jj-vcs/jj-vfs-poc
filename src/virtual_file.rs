use std::pin::Pin;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::stream::BoxStream;
use ustr::Ustr;

use crate::jj_error::JjError;

#[derive(Clone, Copy)]
pub enum FileType {
    File,
    Directory,
}

impl From<FileType> for fuser::FileType {
    fn from(file_type: FileType) -> Self {
        match file_type {
            FileType::File => fuser::FileType::RegularFile,
            FileType::Directory => fuser::FileType::Directory,
        }
    }
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

pub type DirectoryStream = BoxStream<'static, DirectoryEntry>;

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
    async fn list(&self) -> Result<DirectoryStream, JjError>;
    async fn size(&self) -> Result<u64, JjError>;
    async fn file_type(&self) -> Result<FileType, JjError>;
}
