use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;

use fuser::*;
use futures::StreamExt as _;

use crate::jj_error::JjError;
use crate::jj_filesystem::JjFilesystem;

const TTL: Duration = Duration::from_secs(1);

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
                    $reply.error(err.into());
                }
            }
        });
    };
}

pub struct JjFuse<FS: JjFilesystem> {
    fs: Arc<FS>,
    rt_handle: tokio::runtime::Handle,
}

impl<FS: JjFilesystem> JjFuse<FS> {
    pub fn new(fs: Arc<FS>, rt_handle: tokio::runtime::Handle) -> Self {
        Self { fs, rt_handle }
    }
}

impl<FS: JjFilesystem + Send + Sync + 'static> Filesystem for JjFuse<FS> {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.add_capabilities(
            fuser::InitFlags::FUSE_PARALLEL_DIROPS | fuser::InitFlags::FUSE_ASYNC_READ,
        );
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let fs = self.fs.clone();
        let name = name.to_os_string();
        reply_async!(
            self,
            reply,
            async move {
                let name_str = name.to_str().ok_or(JjError::InvalidPath)?;
                fs.lookup(parent.into(), name_str).await
            },
            |reply: ReplyEntry, attr| reply.entry(&TTL, &attr, fuser::Generation(0))
        );
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            async move { fs.getattr(ino.into()).await },
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
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            async move { fs.read(ino.into(), offset, size).await },
            |reply: ReplyData, content: Box<[u8]>| reply.data(&content)
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
        let fs = self.fs.clone();
        reply_async!(
            self,
            reply,
            async {
                let mut entries = fs.readdir(ino.into(), offset).await?.enumerate();
                while let Some((index, entry)) = entries.next().await {
                    let (child_ino, entry) = entry?;
                    if reply.add(
                        fuser::INodeNo(child_ino),
                        offset + index as u64 + 1,
                        entry.file_type.into(),
                        entry.name.as_str(),
                    ) {
                        break;
                    }
                }
                Ok(())
            },
            |reply: ReplyDirectory, ()| reply.ok()
        );
    }

    fn readlink(&self, _req: &Request, _ino: INodeNo, reply: ReplyData) {
        reply.error(Errno::ENOSYS);
    }
}
