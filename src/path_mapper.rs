use std::path::Path;

use async_trait::async_trait;

use crate::jj_error::JjError;
use crate::virtual_file::VirtualFile;

/// This trait represents the VFS mountpoint structure by mapping a given
/// absolute path to a `VirtualFile`.
#[async_trait]
pub trait PathMapper: Send + Sync {
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError>;
}
