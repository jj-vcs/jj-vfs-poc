use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::stream::BoxStream;

use crate::jj_error::JjError;
use crate::jj_error::JjResult;

#[derive(Clone, Copy)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

pub struct FileAttributes {
    pub size: u64,
    pub file_type: FileType,
    pub created: SystemTime,
    pub modified: SystemTime,
}

#[derive(Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub file_type: FileType,
}

pub type DirectoryStream = BoxStream<'static, DirectoryEntry>;

#[derive(Debug, Clone)]
pub enum CreateFile {
    File { name: String, executable: bool },
    Directory { name: String },
    Symlink { name: String, target: String },
}

impl CreateFile {
    pub fn name(&self) -> &str {
        match self {
            CreateFile::File { name, .. } => name,
            CreateFile::Directory { name } => name,
            CreateFile::Symlink { name, .. } => name,
        }
    }
}

impl From<CreateFile> for FileType {
    fn from(value: CreateFile) -> Self {
        match value {
            CreateFile::File { .. } => FileType::File,
            CreateFile::Directory { .. } => FileType::Directory,
            CreateFile::Symlink { .. } => FileType::Symlink,
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
    async fn read(&self) -> JjResult<Pin<Box<dyn AsyncRead + Send>>> {
        Err(JjError::NotAFile)
    }

    async fn list(&self) -> JjResult<DirectoryStream> {
        Err(JjError::NotADirectory)
    }

    async fn read_link(&self) -> JjResult<PathBuf> {
        Err(JjError::NotASymlink)
    }

    async fn attributes(&self) -> JjResult<FileAttributes>;
    async fn file_type(&self) -> JjResult<FileType>;

    async fn create(&self, _file: CreateFile) -> JjResult<FileAttributes> {
        Err(JjError::Readonly)
    }

    async fn write(&self, _offset: u64, _data: &[u8]) -> JjResult<u32> {
        Err(JjError::Readonly)
    }

    async fn delete(&self) -> JjResult<()> {
        Err(JjError::Readonly)
    }
}
