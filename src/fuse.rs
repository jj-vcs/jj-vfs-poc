use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use fuser::Errno;
use fuser::FileAttr;
use fuser::FileHandle;
use fuser::Filesystem;
use fuser::INodeNo;
use fuser::LockOwner;
use fuser::OpenFlags;
use fuser::ReplyAttr;
use fuser::ReplyData;
use fuser::ReplyDirectory;
use fuser::ReplyEntry;
use fuser::Request;
use futures::AsyncReadExt;
use futures::StreamExt;

use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::path_mapper::FileType;
use crate::path_mapper::PathMapper;

const TTL: Duration = Duration::from_secs(1);

pub struct JjVfs<P: PathMapper> {
    inodes: Arc<InodeMap>,
    path_mapper: Arc<P>,
    rt_handle: tokio::runtime::Handle,
}

/// Helper macro used within `Filesystem` implementation of `JjVfs`.
///
/// It takes care of spawning a thread to run the async code and call
/// reply.error(err.to_posix()) if the async code returns an error.
/// This means that the body of the macro can simply be a sequence of async
/// statements and expressions that return a `Result<T, JjError>`.  
macro_rules! reply_async {
    ($self:ident, $reply:ident, $async_body:expr, $success_fn:expr) => {
        let rt_handle = $self.rt_handle.clone();
        rt_handle.spawn(async move {
            let res: Result<_, JjError> = $async_body.await;
            match res {
                Ok(value) => {
                    $success_fn($reply, value);
                }
                Err(err) => {
                    $reply.error(err.to_posix());
                }
            }
        });
    };
}

impl<P: PathMapper> JjVfs<P> {
    pub fn new(path_mapper: P) -> Self {
        Self {
            inodes: Arc::new(InodeMap::new()),
            path_mapper: Arc::new(path_mapper),
            rt_handle: tokio::runtime::Handle::current(),
        }
    }
}

impl<P: PathMapper + 'static> Filesystem for JjVfs<P> {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_os_string();
        let inodes = self.inodes.clone();
        let path_mapper = self.path_mapper.clone();
        reply_async!(
            self,
            reply,
            async {
                let ino = inodes.get_ino(parent.0, name.to_str().unwrap()).await?;
                let path = inodes.get_path(ino).await?;
                let virtual_file = path_mapper.get_entry(&path).await?;
                let size = virtual_file.size().await?;
                let file_type = virtual_file.file_type().await?;
                let attr = create_attr(ino, size, file_type);
                Ok(attr)
            },
            |reply: ReplyEntry, attr| reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let inodes = self.inodes.clone();
        let path_mapper = self.path_mapper.clone();
        reply_async!(
            self,
            reply,
            async {
                let path = inodes.get_path(ino.0).await?;
                let virtual_file = path_mapper.get_entry(&path).await?;
                let size = virtual_file.size().await?;
                let file_type = virtual_file.file_type().await?;
                let attr = create_attr(ino.0, size, file_type);
                Ok(attr)
            },
            |reply: ReplyAttr, attr| reply.attr(&TTL, &attr)
        );
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let inodes = self.inodes.clone();
        let path_mapper = self.path_mapper.clone();
        reply_async!(
            self,
            reply,
            async {
                let path = inodes.get_path(ino.0).await?;
                let virtual_file = path_mapper.get_entry(&path).await?;
                let reader = virtual_file.read().await?;
                let mut limited_stream = reader.take(offset); // TODO: handle proper seek()
                futures::io::copy(&mut limited_stream, &mut futures::io::sink())
                    .await
                    .unwrap();
                let mut original_reader = limited_stream.into_inner();
                let mut content = Vec::with_capacity(size as usize);
                original_reader.read_to_end(&mut content).await.unwrap();
                Ok(content)
            },
            |reply: ReplyData, content: Vec<u8>| reply.data(&content)
        );
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let inodes = self.inodes.clone();
        let path_mapper = self.path_mapper.clone();

        if offset < 1 && reply.add(ino, 1, fuser::FileType::Directory, ".") {
            reply.ok();
            return;
        }

        reply_async!(
            self,
            reply,
            async {
                let path = inodes.get_path(ino.0).await?;
                let virtual_file = path_mapper.get_entry(&path).await?;
                let entries = virtual_file.list().await?;
                let entries = Box::into_pin(entries);

                if offset < 2 {
                    let parent_ino = inodes.get_parent_ino(ino.0).await?;
                    if reply.add(INodeNo(parent_ino), 2, fuser::FileType::Directory, "..") {
                        return Ok(());
                    }
                }

                let skip_count = offset.saturating_sub(2);
                let mut stream = entries.skip(skip_count as usize).enumerate();
                while let Some((stream_index, entry)) = stream.next().await {
                    let name = entry.name.as_str();
                    let kind = match entry.file_type {
                        FileType::Directory => fuser::FileType::Directory,
                        FileType::File => fuser::FileType::RegularFile,
                    };
                    let child_ino = inodes.get_ino(ino.0, name).await?;
                    let entry_offset = skip_count + stream_index as u64 + 3;
                    if reply.add(INodeNo(child_ino), entry_offset, kind, name) {
                        break;
                    }
                }
                Ok(())
            },
            |reply: ReplyDirectory, _: ()| reply.ok()
        );
    }

    fn readlink(&self, _req: &Request, _ino: INodeNo, reply: ReplyData) {
        reply.error(Errno::ENOSYS);
    }
}

fn create_attr(ino: u64, size: u64, file_type: FileType) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
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
    use std::fs;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::Stream;
    use futures::io::Cursor;
    use futures::stream;
    use tempfile::tempdir;

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

    fn setup_test_mount() -> (fuser::BackgroundSession, tempfile::TempDir) {
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
        let fs = JjVfs::new(mapper);

        let tmp_dir = tempdir().unwrap();
        let mountpoint = tmp_dir.path().to_path_buf();

        let mut config = fuser::Config::default();
        config.mount_options = vec![
            fuser::MountOption::RO,
            fuser::MountOption::FSName("jjfs_test".to_string()),
        ];

        // Spawn the FUSE mount
        let session = fuser::spawn_mount(fs, &mountpoint, &config).expect("Failed to mount FUSE");
        (session, tmp_dir)
    }

    #[test]
    fn test_read_root_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let (session, tmp_dir) = setup_test_mount();

        let file_path = tmp_dir.path().join("file.txt");
        let content = fs::read_to_string(&file_path).expect("Failed to read file.txt");
        assert_eq!(content, "hello world");

        drop(session);
    }

    #[test]
    fn test_read_nested_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let (session, tmp_dir) = setup_test_mount();

        let nested_path = tmp_dir.path().join("dir/nested.txt");
        let nested_content = fs::read_to_string(&nested_path).expect("Failed to read nested.txt");
        assert_eq!(nested_content, "nested content");

        drop(session);
    }

    #[test]
    fn test_list_directory() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let (session, tmp_dir) = setup_test_mount();

        let dir_path = tmp_dir.path().join("dir");
        let mut dir_entries: Vec<String> = fs::read_dir(&dir_path)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        dir_entries.sort();
        assert_eq!(dir_entries, vec!["nested.txt"]);

        drop(session);
    }
}
