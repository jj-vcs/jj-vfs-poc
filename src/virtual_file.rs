use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::stream::BoxStream;

use crate::jj_error::JjError;

#[derive(Clone, Copy)]
pub enum FileType {
    File,
    Directory,
}

pub struct FileAttributes {
    pub size: u64,
    pub file_type: FileType,
    pub created: SystemTime,
    pub modified: SystemTime,
}

pub struct DirectoryEntry {
    pub name: String,
    pub file_type: FileType,
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
    async fn attributes(&self) -> Result<FileAttributes, JjError>;
    async fn file_type(&self) -> Result<FileType, JjError>;
}
