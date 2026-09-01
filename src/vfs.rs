use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt as _;
use futures::io::AsyncReadExt as _;
use futures::stream;

use crate::inode_map::Inode;
use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::path_mapper::PathMapper;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

#[derive(Clone)]
pub struct ReadDirEntry {
    pub ino: Inode,
    pub offset: u64,
    pub file_type: FileType,
    pub name: String,
}

pub type ReadDirStream = Pin<Box<dyn Stream<Item = Result<ReadDirEntry, JjError>> + Send>>;

/// Middle-layer filesystem abstraction representing inode-based VFS operations.
/// This trait acts as the intermediate layer between the FUSE filesystem layer
/// and the path mapper.
#[async_trait]
pub trait VirtualFilesystem: Send + Sync {
    async fn get_ino(&self, parent: Inode, name: &str) -> Result<Inode, JjError>;
    async fn get_attributes(&self, ino: Inode) -> Result<FileAttributes, JjError>;
    async fn read(&self, ino: Inode, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn read_directory(&self, ino: Inode, offset: u64) -> Result<ReadDirStream, JjError>;
    async fn read_link(&self, ino: Inode) -> Result<PathBuf, JjError>;
}

pub struct PathMappedVfs<P: PathMapper> {
    path_mapper: Arc<P>,
    inode_map: Arc<InodeMap>,
}

impl<P: PathMapper> PathMappedVfs<P> {
    pub fn new(path_mapper: P) -> Self {
        Self {
            path_mapper: Arc::new(path_mapper),
            inode_map: Arc::new(InodeMap::new()),
        }
    }

    fn get_parent_ino(&self, ino: Inode) -> Result<Inode, JjError> {
        self.inode_map.get_parent_ino(ino).inspect_err(|err| {
            tracing::error!(ino = ino, error = %err, "Failed to resolve parent inode for path");
        })
    }

    fn get_path(&self, ino: Inode) -> Result<PathBuf, JjError> {
        self.inode_map.get_path(ino).inspect_err(|err| {
            tracing::error!(ino = ino, error = %err, "Failed to resolve path for inode");
        })
    }

    async fn get_virtual_file(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
        self.path_mapper.get_entry(path).await.inspect_err(|err| {
            tracing::debug!(path = %format_args!("./{}", path.display()), error = %err, "Failed to resolve virtual file for path");
        })
    }
}

#[async_trait]
impl<P: PathMapper> VirtualFilesystem for PathMappedVfs<P> {
    #[tracing::instrument(skip(self))]
    async fn get_ino(&self, parent: Inode, name: &str) -> Result<Inode, JjError> {
        self.inode_map.get_ino(parent, name).inspect_err(|err| {
            tracing::error!(parent = parent, name = name, error = %err, "Failed to resolve inode for path");
        })
    }

    async fn get_attributes(&self, ino: Inode) -> Result<FileAttributes, JjError> {
        let path = self.get_path(ino)?;
        let virtual_file = self.get_virtual_file(&path).await?;
        virtual_file.attributes().await.inspect_err(|err| {
            tracing::error!(path = %format_args!("./{}", path.display()), error = %err, "Failed to resolve attributes for inode");
        })
    }

    async fn read(&self, ino: Inode, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        let path = self.get_path(ino)?;
        let virtual_file = self.get_virtual_file(&path).await?;

        let reader = virtual_file.read().await.inspect_err(|err| {
            tracing::error!(path = %format_args!("./{}", path.display()), error = %err, "Failed to open file for reading");
        })?;
        let mut limited_stream = reader.take(offset); // TODO: handle proper seek()
        futures::io::copy(&mut limited_stream, &mut futures::io::sink()).await?;
        let original_reader = limited_stream.into_inner();
        let mut content = Vec::with_capacity(size as usize);
        original_reader
            .take(size as u64)
            .read_to_end(&mut content)
            .await?;
        Ok(content.into())
    }

    #[tracing::instrument(skip(self))]
    async fn read_directory(&self, ino: Inode, offset: u64) -> Result<ReadDirStream, JjError> {
        let path = self.get_path(ino)?;
        let virtual_file = self.get_virtual_file(&path).await?;
        let parent_ino = self.get_parent_ino(ino)?;

        let mut prefix_entries = Vec::new();
        if offset < 1 {
            prefix_entries.push(Ok(ReadDirEntry {
                ino,
                offset: 1,
                file_type: FileType::Directory,
                name: ".".to_string(),
            }));
        }
        if offset < 2 {
            prefix_entries.push(Ok(ReadDirEntry {
                ino: parent_ino,
                offset: 2,
                file_type: FileType::Directory,
                name: "..".to_string(),
            }));
        }

        let skip = offset.saturating_sub(2) as usize;
        let children_stream = virtual_file.list().await.inspect_err(|err| {
            tracing::error!(path = %format_args!("./{}", path.display()), error = %err, "Failed to open directory for reading");
        })?;

        let inode_map = self.inode_map.clone();
        let children =
            stream::iter(3..)
                .zip(children_stream)
                .skip(skip)
                .map(move |(index, entry)| {
                    let child_ino = inode_map.get_ino(ino, &entry.name)?;
                    Ok(ReadDirEntry {
                        ino: child_ino,
                        offset: index,
                        file_type: entry.file_type,
                        name: entry.name,
                    })
                });

        let full_stream = stream::iter(prefix_entries).chain(children);
        Ok(Box::pin(full_stream))
    }

    #[tracing::instrument(skip(self), fields(ino = ino, path))]
    async fn read_link(&self, ino: Inode) -> Result<PathBuf, JjError> {
        let path = self.get_path(ino)?;
        let virtual_file = self.get_virtual_file(&path).await?;
        virtual_file.read_link().await.inspect_err(|err| {
            tracing::error!(path = %format_args!("./{}", path.display()), error = %err, "Failed to read symlink");
        })
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
    use futures::io::Cursor;
    use futures::stream;

    use super::*;
    use crate::inode_map::ROOT_INODE;
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

    #[tokio::test]
    async fn test_read_root_file() {
        let fs = setup_test_vfs();
        let file_ino = fs.get_ino(ROOT_INODE, "file.txt").await.unwrap();
        let attr = fs.get_attributes(file_ino).await.unwrap();
        assert_eq!(attr.size, 11);
        let content = fs.read(file_ino, 0, 11).await.unwrap();
        assert_eq!(&*content, b"hello world");
    }

    #[tokio::test]
    async fn test_read_nested_file() {
        let fs = setup_test_vfs();
        let dir_ino = fs.get_ino(ROOT_INODE, "dir").await.unwrap();
        let file_ino = fs.get_ino(dir_ino, "nested.txt").await.unwrap();
        let attr = fs.get_attributes(file_ino).await.unwrap();
        assert_eq!(attr.size, 14);
        let content = fs.read(file_ino, 0, 14).await.unwrap();
        assert_eq!(&*content, b"nested content");
    }

    #[tokio::test]
    async fn test_list_directory() {
        let fs = setup_test_vfs();
        let dir_ino = fs.get_ino(ROOT_INODE, "dir").await.unwrap();

        let entries: Vec<_> = fs
            .read_directory(dir_ino, 0)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].name, ".");
        assert_eq!(entries[0].offset, 1);
        assert_eq!(entries[1].name, "..");
        assert_eq!(entries[1].offset, 2);
        assert_eq!(entries[2].name, "nested.txt");
        assert_eq!(entries[2].offset, 3);
        assert!(matches!(entries[2].file_type, FileType::File));
    }

    #[tokio::test]
    async fn test_list_directory_with_offset() {
        let fs = setup_test_vfs();
        let dir_ino = fs.get_ino(ROOT_INODE, "dir").await.unwrap();

        // Skip '.' (offset = 1)
        let entries: Vec<_> = fs
            .read_directory(dir_ino, 1)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "..");
        assert_eq!(entries[1].name, "nested.txt");

        // Skip '.' and '..' (offset = 2)
        let entries: Vec<_> = fs
            .read_directory(dir_ino, 2)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "nested.txt");
    }

    #[tokio::test]
    async fn test_list_directory_not_a_directory() {
        let fs = setup_test_vfs();
        let file_ino = fs.get_ino(ROOT_INODE, "file.txt").await.unwrap();
        let result = fs.read_directory(file_ino, 0).await;
        assert!(matches!(result, Err(JjError::NotADirectory)));
    }

    #[tokio::test]
    async fn test_list_directory_not_found() {
        let fs = setup_test_vfs();
        let result = fs.read_directory(999, 0).await;
        assert!(matches!(result, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_read_link() {
        let fs = setup_test_vfs();
        let symlink_ino = fs.get_ino(ROOT_INODE, "symlink.txt").await.unwrap();
        let target = fs.read_link(symlink_ino).await.unwrap();
        assert_eq!(target, PathBuf::from("file.txt"));
    }

    #[tokio::test]
    async fn test_read_link_not_a_symlink() {
        let fs = setup_test_vfs();
        let file_ino = fs.get_ino(ROOT_INODE, "file.txt").await.unwrap();
        let result = fs.read_link(file_ino).await;
        assert!(matches!(result, Err(JjError::NotASymlink)));
    }
}
