use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use fuser::*;
use futures::StreamExt as _;
use futures::stream;

use crate::inode_map::InodeMap;
use crate::jj_error::JjError;
use crate::vfs::VirtualFilesystem;
use crate::virtual_file::FileAttributes;

const TTL: Duration = Duration::from_secs(1);

macro_rules! reply_async {
    (
        $self:ident,
        $reply:ident,
        $ino:expr,
        $path:ident => $async_body:expr,
        $value:pat => $success_body:expr
    ) => {
        let inode_map = $self.inode_map.clone();
        let rt_handle = $self.rt_handle.clone();
        let ino = $ino;
        rt_handle.spawn(async move {
            match inode_map.get_path(ino) {
                Ok(mut path_buf) => {
                    let $path = &mut path_buf;
                    let res: Result<_, JjError> = $async_body.await;
                    match res {
                        Ok($value) => {
                            $success_body
                        }
                        Err(err) => {
                            tracing::debug!(path = %format_args!("./{}", path_buf.display()), error = %err, "FUSE operation failed");
                            $reply.error(err.into());
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(ino = %ino, error = %err, "Failed to resolve path for inode");
                    $reply.error(err.into());
                }
            }
        });
    };
}

pub struct JjFuse<FS: VirtualFilesystem> {
    fs: Arc<FS>,
    inode_map: Arc<InodeMap>,
    rt_handle: tokio::runtime::Handle,
}

impl<FS: VirtualFilesystem> JjFuse<FS> {
    pub fn new(fs: Arc<FS>, rt_handle: tokio::runtime::Handle) -> Self {
        Self {
            fs,
            inode_map: Arc::new(InodeMap::new()),
            rt_handle,
        }
    }
}

impl<FS: VirtualFilesystem + Send + Sync + 'static> Filesystem for JjFuse<FS> {
    #[tracing::instrument(level = "debug", skip_all)]
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.add_capabilities(
            fuser::InitFlags::FUSE_PARALLEL_DIROPS | fuser::InitFlags::FUSE_ASYNC_READ,
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        let name = name.to_os_string();
        reply_async!(
            self,
            reply,
            parent,
            parent_path => async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                let child_ino = inode_map.get_ino(parent, name_str)?;
                parent_path.push(name_str);
                let attr = fs.get_attributes(parent_path).await?;
                Ok(attr.to_fuse(child_ino))
            },
            attr => reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async { Ok(fs.get_attributes(path).await?.to_fuse(ino)) },
            attr => reply.attr(&TTL, &attr)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
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
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async { fs.read(path, offset, size).await },
            content => reply.data(&content)
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let fs = self.fs.clone();
        let inode_map = self.inode_map.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async {
                if offset < 1 && reply.add(ino, 1, FileType::Directory, ".") {
                    return Ok(());
                }

                if offset < 2 {
                    let parent_ino = inode_map.get_parent_ino(ino)?;
                    if reply.add(parent_ino, 2, FileType::Directory, "..") {
                        return Ok(());
                    }
                }

                let skip = offset.saturating_sub(2) as usize;
                let entries = fs.read_directory(path).await?;
                let mut stream = stream::iter(3..).zip(entries).skip(skip);
                while let Some((index, entry)) = stream.next().await {
                    let name = entry.name.as_str();
                    let file_type = entry.file_type.into();
                    let child_ino = inode_map.get_ino(ino, name)?;
                    if reply.add(child_ino, index, file_type, name) {
                        break;
                    }
                }
                Ok(())
            },
            () => reply.ok()
        );
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            ino,
            path => async move {
                let target = fs.read_link(path).await?;
                let bytes = target.as_os_str().as_bytes().to_vec();
                Ok(bytes)
            },
            target => reply.data(&target)
        );
    }
}

impl FileAttributes {
    pub fn to_fuse(&self, ino: INodeNo) -> FileAttr {
        FileAttr {
            ino,
            size: self.size,
            blocks: self.size.div_ceil(512),
            atime: SystemTime::now(), // TODO: properly set timestamps
            mtime: self.modified,
            ctime: self.modified,
            crtime: self.created,
            kind: self.file_type.into(),
            perm: match self.file_type {
                crate::virtual_file::FileType::Directory => 0o755,
                _ => 0o644,
            },
            nlink: match self.file_type {
                crate::virtual_file::FileType::Directory => 2,
                _ => 1,
            },
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }
}

impl From<crate::virtual_file::FileType> for FileType {
    fn from(file_type: crate::virtual_file::FileType) -> Self {
        match file_type {
            crate::virtual_file::FileType::File => FileType::RegularFile,
            crate::virtual_file::FileType::Directory => FileType::Directory,
            crate::virtual_file::FileType::Symlink => FileType::Symlink,
        }
    }
}

impl From<JjError> for Errno {
    fn from(value: JjError) -> Self {
        match value {
            JjError::NotFound => Errno::ENOENT,
            JjError::NotADirectory => Errno::ENOTDIR,
            JjError::NotAFile => Errno::EISDIR,
            JjError::NotASymlink => Errno::EINVAL,
            _ => Errno::EIO,
        }
    }
}
