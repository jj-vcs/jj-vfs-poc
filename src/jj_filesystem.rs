use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use fuser::FileAttr; /* TODO: This file should not depend on fuser at all. Leaving it for
                       * now for simplicity. */
use futures::StreamExt as _;
use futures::io::AsyncReadExt as _;
use futures::stream::BoxStream;

use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::path_mapper::DirectoryEntry;
use crate::path_mapper::FileType;
use crate::path_mapper::PathMapper;

pub type ReaddirStream = BoxStream<'static, Result<(u64, DirectoryEntry), JjError>>;

/// Middle-layer filesystem abstraction representing inode-based VFS operations.
/// This trait acts as the intermediate layer between the FUSE filesystem layer
/// and the path mapper.
#[async_trait]
pub trait JjFilesystem {
    async fn lookup(&self, parent: u64, name: &str) -> Result<FileAttr, JjError>;
    async fn getattr(&self, ino: u64) -> Result<FileAttr, JjError>;
    async fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn readdir(&self, ino: u64, offset: u64) -> Result<ReaddirStream, JjError>;
}

pub struct JjVfsState<P: PathMapper> {
    inodes: Arc<InodeMap>,
    path_mapper: Arc<P>,
}

impl<P: PathMapper> JjVfsState<P> {
    pub fn new(path_mapper: P) -> Self {
        Self {
            inodes: Arc::new(InodeMap::new()),
            path_mapper: Arc::new(path_mapper),
        }
    }
}

#[async_trait]
impl<P: PathMapper> JjFilesystem for JjVfsState<P> {
    async fn lookup(&self, parent: u64, name: &str) -> Result<FileAttr, JjError> {
        let ino = self.inodes.get_ino(parent, name).await?;
        self.getattr(ino).await
    }

    async fn getattr(&self, ino: u64) -> Result<FileAttr, JjError> {
        let path = self.inodes.get_path(ino).await?;
        let virtual_file = self.path_mapper.get_entry(&path).await?;
        let size = virtual_file.size().await?;
        let file_type = virtual_file.file_type().await?;
        Ok(create_attr(ino, size, file_type))
    }

    async fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Box<[u8]>, JjError> {
        let path = self.inodes.get_path(ino).await?;
        let virtual_file = self.path_mapper.get_entry(&path).await?;
        let reader = virtual_file.read().await?;
        let mut limited_stream = reader.take(offset); // TODO: handle proper seek()
        futures::io::copy(&mut limited_stream, &mut futures::io::sink()).await?;
        let mut original_reader = limited_stream.into_inner();
        let mut content = Vec::with_capacity(size as usize);
        original_reader.read_to_end(&mut content).await?;
        Ok(content.into())
    }

    async fn readdir(&self, ino: u64, offset: u64) -> Result<ReaddirStream, JjError> {
        let mut prefix = Vec::with_capacity(2);
        if offset < 1 {
            prefix.push(Ok((ino, DirectoryEntry::new(".", FileType::Directory))));
        }

        if offset < 2 {
            let parent_ino = self.inodes.get_parent_ino(ino).await?;
            prefix.push(Ok((
                parent_ino,
                DirectoryEntry::new("..", FileType::Directory),
            )));
        }

        let path = self.inodes.get_path(ino).await?;
        let virtual_file = self.path_mapper.get_entry(&path).await?;
        let stream = virtual_file.list().await?;
        let skip_count = offset.saturating_sub(2) as usize;

        let skipped_stream = stream.skip(skip_count);
        let inodes = self.inodes.clone();
        let mapped_stream = skipped_stream.then(move |entry| {
            let inodes = inodes.clone();
            async move {
                let child_ino = inodes.get_ino(ino, entry.name.as_str()).await?;
                Ok((child_ino, entry))
            }
        });

        let prefix_stream = futures::stream::iter(prefix);
        let full_stream = prefix_stream.chain(mapped_stream);
        Ok(Box::pin(full_stream))
    }
}

fn create_attr(ino: u64, size: u64, file_type: FileType) -> FileAttr {
    FileAttr {
        ino: fuser::INodeNo(ino),
        size,
        blocks: size.div_ceil(512),
        atime: UNIX_EPOCH, // TODO: properly set timestamps
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: file_type.into(),
        perm: match file_type {
            FileType::Directory => 0o755,
            _ => 0o644,
        },
        nlink: match file_type {
            FileType::Directory => 2,
            _ => 1,
        },
        uid: 1000,
        gid: 1000,
        rdev: 0,
        flags: 0,
        blksize: 4096,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::io::Cursor;
    use futures::stream;
    use pollster::FutureExt;

    use super::*;
    use crate::path_mapper::DirectoryEntry;
    use crate::path_mapper::DirectoryStream;
    use crate::path_mapper::VirtualFile;

    enum MockVirtualFile {
        File(Vec<u8>),
        Directory(HashMap<String, Arc<MockVirtualFile>>),
    }

    #[async_trait]
    impl VirtualFile for Arc<MockVirtualFile> {
        async fn read(&self) -> Result<Pin<Box<dyn futures::AsyncRead + Send>>, JjError> {
            match &**self {
                MockVirtualFile::File(content) => Ok(Box::pin(Cursor::new(content.clone()))),
                MockVirtualFile::Directory(_) => Err(JjError::NotAFile),
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
                            };
                            DirectoryEntry::new(name, file_type)
                        })
                        .collect::<Vec<_>>();
                    let stream: DirectoryStream = Box::pin(stream::iter(children));
                    Ok(stream)
                }
                MockVirtualFile::File(_) => Err(JjError::NotADirectory),
            }
        }

        async fn size(&self) -> Result<u64, JjError> {
            match &**self {
                MockVirtualFile::File(content) => Ok(content.len() as u64),
                MockVirtualFile::Directory(_) => Ok(0),
            }
        }

        async fn file_type(&self) -> Result<FileType, JjError> {
            match &**self {
                MockVirtualFile::File(_) => Ok(FileType::File),
                MockVirtualFile::Directory(_) => Ok(FileType::Directory),
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

    fn setup_test_vfs() -> JjVfsState<MockPathMapper> {
        let mut root_children = HashMap::new();
        root_children.insert(
            "file.txt".to_string(),
            Arc::new(MockVirtualFile::File(b"hello world".to_vec())),
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

        JjVfsState::new(mapper)
    }

    #[test]
    fn test_read_root_file() {
        let fs = setup_test_vfs();
        let attr = fs.lookup(1, "file.txt").block_on().unwrap();
        let content = fs.read(attr.ino.0, 0, 11).block_on().unwrap();
        assert_eq!(&*content, b"hello world");
    }

    #[test]
    fn test_read_nested_file() {
        let fs = setup_test_vfs();
        let dir_attr = fs.lookup(1, "dir").block_on().unwrap();
        let file_attr = fs.lookup(dir_attr.ino.0, "nested.txt").block_on().unwrap();
        let content = fs.read(file_attr.ino.0, 0, 14).block_on().unwrap();
        assert_eq!(&*content, b"nested content");
    }

    #[test]
    fn test_list_directory() {
        let fs = setup_test_vfs();
        let dir_attr = fs.lookup(1, "dir").block_on().unwrap();

        let entries: Vec<_> = fs
            .readdir(dir_attr.ino.0, 0)
            .block_on()
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .block_on();

        assert_eq!(entries.len(), 3);

        let mut iter = entries.into_iter();

        let entry0 = iter.next().unwrap();
        assert_eq!(entry0.0, dir_attr.ino.0);
        assert_eq!(entry0.1.name, ".");
        assert!(matches!(entry0.1.file_type, FileType::Directory));

        let entry1 = iter.next().unwrap();
        assert_eq!(entry1.0, 1);
        assert_eq!(entry1.1.name, "..");
        assert!(matches!(entry1.1.file_type, FileType::Directory));

        let entry2 = iter.next().unwrap();
        assert_eq!(entry2.1.name, "nested.txt");
        assert!(matches!(entry2.1.file_type, FileType::File));

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_list_directory_with_offset() {
        let fs = setup_test_vfs();
        let dir_attr = fs.lookup(1, "dir").block_on().unwrap();

        let entries: Vec<_> = fs
            .readdir(dir_attr.ino.0, 2)
            .block_on()
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
            .block_on();

        assert_eq!(entries.len(), 1);
        let entry0 = entries.into_iter().next().unwrap();
        assert_eq!(entry0.1.name, "nested.txt");
        assert!(matches!(entry0.1.file_type, FileType::File));
    }

    #[test]
    fn test_list_directory_not_a_directory() {
        let fs = setup_test_vfs();
        let file_attr = fs.lookup(1, "file.txt").block_on().unwrap();
        let result = fs.readdir(file_attr.ino.0, 0).block_on();
        assert!(matches!(result, Err(JjError::NotADirectory)));
    }

    #[test]
    fn test_list_directory_not_found() {
        let fs = setup_test_vfs();
        let result = fs.readdir(9999, 0).block_on();
        assert!(matches!(result, Err(JjError::NotFound)));
    }
}
