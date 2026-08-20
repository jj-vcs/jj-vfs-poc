use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::io::AsyncReadExt as _;

use crate::jj_error::JjError;
use crate::path_mapper::PathMapper;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;

/// Middle-layer filesystem abstraction representing inode-based VFS operations.
/// This trait acts as the intermediate layer between the FUSE filesystem layer
/// and the path mapper.
#[async_trait]
pub trait VirtualFilesystem {
    async fn get_attributes(&self, path: &Path) -> Result<FileAttributes, JjError>;
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn read_directory(&self, path: &Path) -> Result<DirectoryStream, JjError>;
    async fn read_link(&self, path: &Path) -> Result<PathBuf, JjError>;
}

pub struct PathMappedVfs<P: PathMapper> {
    path_mapper: Arc<P>,
}

impl<P: PathMapper> PathMappedVfs<P> {
    pub fn new(path_mapper: P) -> Self {
        Self {
            path_mapper: Arc::new(path_mapper),
        }
    }
}

#[async_trait]
impl<P: PathMapper> VirtualFilesystem for PathMappedVfs<P> {
    #[tracing::instrument(skip(self))]
    async fn get_attributes(&self, path: &Path) -> Result<FileAttributes, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.attributes().await
    }

    #[tracing::instrument(skip(self))]
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        let reader = virtual_file.read().await?;
        let mut limited_stream = reader.take(offset); // TODO: handle proper seek()
        futures::io::copy(&mut limited_stream, &mut futures::io::sink()).await?;
        let mut original_reader = limited_stream.into_inner();
        let mut content = Vec::with_capacity(size as usize);
        original_reader.read_to_end(&mut content).await?;
        Ok(content.into())
    }

    #[tracing::instrument(skip(self))]
    async fn read_directory(&self, path: &Path) -> Result<DirectoryStream, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.list().await
    }

    #[tracing::instrument(skip(self))]
    async fn read_link(&self, path: &Path) -> Result<PathBuf, JjError> {
        let virtual_file = self.path_mapper.get_entry(path).await?;
        virtual_file.read_link().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use futures::StreamExt as _;
    use futures::io::Cursor;
    use futures::stream;
    use pollster::FutureExt;

    use super::*;
    use crate::virtual_file::DirectoryEntry;
    use crate::virtual_file::DirectoryStream;
    use crate::virtual_file::FileType;
    use crate::virtual_file::VirtualFile;

    enum MockVirtualFile {
        File(Vec<u8>),
        Directory(HashMap<String, Arc<MockVirtualFile>>),
        Symlink(PathBuf),
    }

    #[async_trait]
    impl VirtualFile for Arc<MockVirtualFile> {
        async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
            match &**self {
                MockVirtualFile::File(content) => Ok(Box::pin(Cursor::new(content.clone()))),
                MockVirtualFile::Directory(_) => Err(JjError::NotAFile),
                MockVirtualFile::Symlink(_) => Err(JjError::NotAFile),
            }
        }

        async fn list(&self) -> Result<DirectoryStream, JjError> {
            match &**self {
                MockVirtualFile::Directory(entries) => {
                    let children = entries
                        .iter()
                        .map(|(name, file)| {
                            let file_type = match &**file {
                                MockVirtualFile::File(_) => FileType::File,
                                MockVirtualFile::Directory(_) => FileType::Directory,
                                MockVirtualFile::Symlink(_) => FileType::Symlink,
                            };
                            DirectoryEntry {
                                name: name.to_string(),
                                file_type,
                            }
                        })
                        .collect::<Vec<_>>();
                    let stream: DirectoryStream = Box::pin(stream::iter(children));
                    Ok(stream)
                }
                MockVirtualFile::File(_) | MockVirtualFile::Symlink(_) => {
                    Err(JjError::NotADirectory)
                }
            }
        }

        async fn read_link(&self) -> Result<PathBuf, JjError> {
            match &**self {
                MockVirtualFile::Symlink(target) => Ok(target.clone()),
                _ => Err(JjError::NotASymlink),
            }
        }

        async fn attributes(&self) -> Result<FileAttributes, JjError> {
            match &**self {
                MockVirtualFile::File(content) => Ok(FileAttributes {
                    size: content.len() as u64,
                    file_type: FileType::File,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                }),
                MockVirtualFile::Directory(_) => Ok(FileAttributes {
                    size: 0,
                    file_type: FileType::Directory,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                }),
                MockVirtualFile::Symlink(target) => Ok(FileAttributes {
                    size: target.to_string_lossy().len() as u64,
                    file_type: FileType::Symlink,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                }),
            }
        }

        async fn file_type(&self) -> Result<FileType, JjError> {
            match &**self {
                MockVirtualFile::File(_) => Ok(FileType::File),
                MockVirtualFile::Directory(_) => Ok(FileType::Directory),
                MockVirtualFile::Symlink(_) => Ok(FileType::Symlink),
            }
        }
    }

    struct MockPathMapper {
        root: Arc<MockVirtualFile>,
    }

    #[async_trait]
    impl PathMapper for MockPathMapper {
        async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
            let mut current_entry = self.root.clone();
            for part in path.iter() {
                let part_str = part.to_str().ok_or(JjError::InvalidPath)?;
                if let MockVirtualFile::Directory(entries) = &*current_entry {
                    current_entry = entries.get(part_str).ok_or(JjError::NotFound)?.clone();
                } else {
                    return Err(JjError::NotFound);
                }
            }
            Ok(Box::new(current_entry))
        }
    }

    fn setup_test_vfs() -> PathMappedVfs<MockPathMapper> {
        let mut root_children = HashMap::new();
        root_children.insert(
            "file.txt".to_string(),
            Arc::new(MockVirtualFile::File(b"hello world".to_vec())),
        );
        root_children.insert(
            "symlink.txt".to_string(),
            Arc::new(MockVirtualFile::Symlink(PathBuf::from("file.txt"))),
        );

        let mut dir_children = HashMap::new();
        dir_children.insert(
            "nested.txt".to_string(),
            Arc::new(MockVirtualFile::File(b"nested content".to_vec())),
        );
        root_children.insert(
            "dir".to_string(),
            Arc::new(MockVirtualFile::Directory(dir_children)),
        );

        let root = Arc::new(MockVirtualFile::Directory(root_children));
        let mapper = MockPathMapper { root };

        PathMappedVfs::new(mapper)
    }

    #[test]
    fn test_read_root_file() {
        let fs = setup_test_vfs();
        let content = fs.read(Path::new("file.txt"), 0, 11).block_on().unwrap();
        assert_eq!(&*content, b"hello world");
    }

    #[test]
    fn test_read_nested_file() {
        let fs = setup_test_vfs();
        let content = fs
            .read(Path::new("dir/nested.txt"), 0, 14)
            .block_on()
            .unwrap();
        assert_eq!(&*content, b"nested content");
    }

    #[test]
    fn test_list_directory() {
        let fs = setup_test_vfs();

        let entries: Vec<_> = fs
            .read_directory(Path::new("dir"))
            .block_on()
            .unwrap()
            .collect()
            .block_on();

        assert_eq!(entries.len(), 1);
        let entry0 = entries.into_iter().next().unwrap();
        assert_eq!(entry0.name, "nested.txt");
        assert!(matches!(entry0.file_type, FileType::File));
    }

    #[test]
    fn test_list_directory_not_a_directory() {
        let fs = setup_test_vfs();
        let result = fs.read_directory(Path::new("file.txt")).block_on();
        assert!(matches!(result, Err(JjError::NotADirectory)));
    }

    #[test]
    fn test_list_directory_not_found() {
        let fs = setup_test_vfs();
        let result = fs.read_directory(Path::new("does_not_exist")).block_on();
        assert!(matches!(result, Err(JjError::NotFound)));
    }

    #[test]
    fn test_read_link() {
        let fs = setup_test_vfs();
        let target = fs.read_link(Path::new("symlink.txt")).block_on().unwrap();
        assert_eq!(target, PathBuf::from("file.txt"));
    }

    #[test]
    fn test_read_link_not_a_symlink() {
        let fs = setup_test_vfs();
        let result = fs.read_link(Path::new("file.txt")).block_on();
        assert!(matches!(result, Err(JjError::NotASymlink)));
    }
}
