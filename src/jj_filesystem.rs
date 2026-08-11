use std::sync::Arc;
use std::time::UNIX_EPOCH;

use fuser::FileAttr; /* TODO: This file should not depend on fuser at all. Leaving it for
                       * now for simplicity. */
use futures::StreamExt as _;
use futures::io::AsyncReadExt as _;

use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::path_mapper::DirectoryEntry;
use crate::path_mapper::FileType;
use crate::path_mapper::PathMapper;

/// Middle-layer filesystem abstraction representing inode-based VFS operations.
/// This trait acts as the intermediate layer between the FUSE filesystem layer
/// and the path mapper.
trait JjFilesystem {
    async fn lookup(&self, parent: u64, name: &str) -> Result<FileAttr, JjError>;
    async fn getattr(&self, ino: u64) -> Result<FileAttr, JjError>;
    async fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Box<[u8]>, JjError>;
    async fn readdir<F>(&self, ino: u64, offset: u64, f: F) -> Result<(), JjError>
    where
        Self: Sized,
        F: FnMut((u64, DirectoryEntry)) -> bool;
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

    // TODO: for now readdir takes callback function f() for iterating through
    // directory entries for easier implementation. readdir() should be fully
    // refactored in a separate PR.
    async fn readdir<F>(&self, ino: u64, offset: u64, mut f: F) -> Result<(), JjError>
    where
        F: FnMut((u64, DirectoryEntry)) -> bool,
    {
        if offset < 2 {
            if offset < 1 && f((ino, DirectoryEntry::new(".", FileType::Directory))) {
                return Ok(());
            }

            if f((
                self.inodes.get_parent_ino(ino).await?,
                DirectoryEntry::new("..", FileType::Directory),
            )) {
                return Ok(());
            }
        }

        let path = self.inodes.get_path(ino).await?;
        let virtual_file = self.path_mapper.get_entry(&path).await?;
        let stream = virtual_file.list().await?;
        let stream = Box::into_pin(stream);
        let skip_count = offset.saturating_sub(2) as usize;

        let mut skipped_stream = stream.skip(skip_count);
        while let Some(entry) = skipped_stream.next().await {
            let name = entry.name.as_str();
            let file_type = entry.file_type;
            let child_ino = self.inodes.get_ino(ino, name).await?;
            if f((child_ino, DirectoryEntry::new(name, file_type))) {
                break;
            }
        }

        Ok(())
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
        kind: match file_type {
            FileType::File => fuser::FileType::RegularFile,
            FileType::Directory => fuser::FileType::Directory,
        },
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
    use futures::Stream;
    use futures::io::Cursor;
    use futures::stream;

    use super::*;
    use crate::path_mapper::DirectoryEntry;
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

        async fn list<'a>(
            &'a self,
        ) -> Result<Box<dyn Stream<Item = DirectoryEntry> + Send + 'a>, JjError> {
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
                    Ok(Box::new(stream::iter(children)))
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
        let attr = futures::executor::block_on(fs.lookup(1, "file.txt")).unwrap();
        let content = futures::executor::block_on(fs.read(attr.ino.0, 0, 11)).unwrap();
        assert_eq!(&*content, b"hello world");
    }

    #[test]
    fn test_read_nested_file() {
        let fs = setup_test_vfs();
        let dir_attr = futures::executor::block_on(fs.lookup(1, "dir")).unwrap();
        let file_attr =
            futures::executor::block_on(fs.lookup(dir_attr.ino.0, "nested.txt")).unwrap();
        let content = futures::executor::block_on(fs.read(file_attr.ino.0, 0, 14)).unwrap();
        assert_eq!(&*content, b"nested content");
    }

    #[test]
    fn test_list_directory() {
        let fs = setup_test_vfs();
        let dir_attr = futures::executor::block_on(fs.lookup(1, "dir")).unwrap();

        let mut entries = Vec::new();
        futures::executor::block_on(fs.readdir(dir_attr.ino.0, 0, |(child_ino, entry)| {
            entries.push((child_ino, entry.name, entry.file_type));
            false
        }))
        .unwrap();

        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].1, ".");
        assert_eq!(entries[0].0, dir_attr.ino.0);
        assert!(matches!(entries[0].2, FileType::Directory));

        assert_eq!(entries[1].1, "..");
        assert_eq!(entries[1].0, 1);
        assert!(matches!(entries[1].2, FileType::Directory));

        assert_eq!(entries[2].1, "nested.txt");
        assert!(matches!(entries[2].2, FileType::File));
    }

    #[test]
    fn test_list_directory_with_offset() {
        let fs = setup_test_vfs();
        let dir_attr = futures::executor::block_on(fs.lookup(1, "dir")).unwrap();

        let mut entries = Vec::new();
        futures::executor::block_on(fs.readdir(dir_attr.ino.0, 2, |(child_ino, entry)| {
            entries.push((child_ino, entry.name, entry.file_type));
            false
        }))
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, "nested.txt");
        assert!(matches!(entries[0].2, FileType::File));
    }
}
