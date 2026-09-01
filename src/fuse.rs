use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use fuser::*;
use futures::StreamExt as _;

use crate::jj_error::JjError;
use crate::vfs::VirtualFilesystem;
use crate::virtual_file::FileAttributes;

const TTL: Duration = Duration::from_secs(1);

pub struct JjFuse<FS: VirtualFilesystem> {
    fs: Arc<FS>,
    rt_handle: tokio::runtime::Handle,
}

impl<FS: VirtualFilesystem> JjFuse<FS> {
    pub fn new(fs: Arc<FS>, rt_handle: tokio::runtime::Handle) -> Self {
        Self { fs, rt_handle }
    }
}

impl<FS: VirtualFilesystem + 'static> Filesystem for JjFuse<FS> {
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
        let name = name.to_os_string();
        self.rt_handle.spawn(async move {
            let res: Result<_, JjError> = async {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                let child_ino = fs.get_ino(parent.0, name_str).await?;
                let attr = fs.get_attributes(child_ino).await?;
                Ok(attr.to_fuse(INodeNo(child_ino)))
            }
            .await;

            match res {
                Ok(attr) => reply.entry(&TTL, &attr, fuser::Generation(0)),
                Err(err) => reply.error(err.into()),
            }
        });
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let fs = self.fs.clone();
        self.rt_handle.spawn(async move {
            match fs.get_attributes(ino.0).await {
                Ok(attr) => reply.attr(&TTL, &attr.to_fuse(ino)),
                Err(err) => reply.error(err.into()),
            }
        });
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
        self.rt_handle.spawn(async move {
            match fs.read(ino.0, offset, size).await {
                Ok(content) => reply.data(&content),
                Err(err) => reply.error(err.into()),
            }
        });
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
        self.rt_handle.spawn(async move {
            let mut stream = match fs.read_directory(ino.0, offset).await {
                Ok(stream) => stream,
                Err(err) => {
                    reply.error(err.into());
                    return;
                }
            };

            while let Some(entry_res) = stream.next().await {
                match entry_res {
                    Ok(entry) => {
                        if reply.add(
                            INodeNo(entry.ino),
                            entry.offset,
                            entry.file_type.into(),
                            &entry.name,
                        ) {
                            break;
                        }
                    }
                    Err(err) => {
                        reply.error(err.into());
                        return;
                    }
                }
            }
            reply.ok();
        });
    }

    #[tracing::instrument(level = "debug", skip(self, _req, reply))]
    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let fs = self.fs.clone();
        self.rt_handle.spawn(async move {
            match fs.read_link(ino.0).await {
                Ok(target) => reply.data(target.as_os_str().as_bytes()),
                Err(err) => reply.error(err.into()),
            }
        });
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
