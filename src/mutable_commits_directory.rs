use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use futures::StreamExt as _;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::revset::ResolvedRevsetExpression;

use crate::jj_error::JjError;
use crate::jj_error::JjResult;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

/// This is a `VirtualFile` that represents a directory where mutable commits
/// are listed as symlinks pointing to `<prefix_path>/<commit_id>`.
pub struct MutableCommitsDirectory {
    repo: Arc<ReadonlyRepo>,
    prefix_path: PathBuf,
}

impl MutableCommitsDirectory {
    pub fn new(repo: Arc<ReadonlyRepo>, prefix_path: PathBuf) -> Self {
        Self { repo, prefix_path }
    }

    pub fn get_symlink(&self, commit_id: &CommitId) -> JjResult<Box<dyn VirtualFile>> {
        if !Self::is_mutable(&self.repo, commit_id)? {
            return Err(JjError::NotFound);
        }
        let target = self.prefix_path.join(commit_id.hex());
        Ok(Box::new(MutableCommitSymlink::new(target)))
    }

    pub fn is_mutable(repo: &ReadonlyRepo, commit_id: &CommitId) -> JjResult<bool> {
        let mut immutable_heads = vec![repo.store().root_commit_id().clone()];
        for (_name, target) in repo.view().tags() {
            immutable_heads.extend(target.local_target.added_ids().cloned());
        }
        for (_symbol, remote_ref) in repo.view().all_remote_bookmarks() {
            immutable_heads.extend(remote_ref.target.added_ids().cloned());
        }

        let immutable_expression = ResolvedRevsetExpression::commits(immutable_heads).ancestors();
        let expression =
            ResolvedRevsetExpression::commits(vec![commit_id.clone()]).minus(&immutable_expression);

        let revset = match expression.evaluate(repo) {
            Ok(revset) => revset,
            Err(_) => return Ok(false),
        };
        Ok(!revset.is_empty())
    }
}

#[async_trait]
impl VirtualFile for MutableCommitsDirectory {
    #[tracing::instrument(skip(self))]
    async fn list(&self) -> JjResult<DirectoryStream> {
        let mut immutable_heads = vec![self.repo.store().root_commit_id().clone()];
        for (_name, target) in self.repo.view().tags() {
            immutable_heads.extend(target.local_target.added_ids().cloned());
        }
        for (_symbol, remote_ref) in self.repo.view().all_remote_bookmarks() {
            immutable_heads.extend(remote_ref.target.added_ids().cloned());
        }

        let immutable_expression = ResolvedRevsetExpression::commits(immutable_heads).ancestors();
        let expression = ResolvedRevsetExpression::all().minus(&immutable_expression);

        let revset = expression
            .evaluate(self.repo.as_ref())
            .map_err(|e| e.into_backend_error())?;
        // Revset does not implement Send, meaning it cannot be sent across
        // threads. We use blocking for now since this will be rewritten
        // in the future anyways.
        let commits: Vec<DirectoryEntry> = futures::executor::block_on(async {
            revset
                .stream()
                .filter_map(|commit_id_res| futures::future::ready(commit_id_res.ok()))
                .map(|commit_id| DirectoryEntry {
                    name: commit_id.hex(),
                    file_type: FileType::Symlink,
                })
                .collect()
                .await
        }); // TODO: currently there is no proper pagination implemented here
        Ok(Box::pin(futures::stream::iter(commits)))
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> JjResult<FileAttributes> {
        Ok(FileAttributes {
            size: 0,
            file_type: FileType::Directory,
            created: UNIX_EPOCH, // TODO: implement proper timestamps
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> JjResult<FileType> {
        Ok(FileType::Directory)
    }
}

pub struct MutableCommitSymlink {
    target: PathBuf,
}

impl MutableCommitSymlink {
    pub fn new(target: PathBuf) -> Self {
        Self { target }
    }
}

#[async_trait]
impl VirtualFile for MutableCommitSymlink {
    async fn read_link(&self) -> JjResult<PathBuf> {
        Ok(self.target.clone())
    }

    async fn attributes(&self) -> JjResult<FileAttributes> {
        Ok(FileAttributes {
            size: self.target.as_os_str().len() as u64,
            file_type: FileType::Symlink,
            created: UNIX_EPOCH,
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> JjResult<FileType> {
        Ok(FileType::Symlink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_repo_mutable_commits() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let commits_dir = MutableCommitsDirectory::new(repo.clone(), PathBuf::from("../commits"));
        let stream = commits_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;

        // The repository has: root commit (immutable) + initial workspace
        // commit (mutable) + our test commit (mutable), so 2 mutable
        // commits in total.
        assert_eq!(files.len(), 2);

        let root_commit_hex = repo.store().root_commit_id().hex();
        for file in &files {
            assert!(matches!(file.file_type, FileType::Symlink));
            assert_ne!(file.name, root_commit_hex);
        }
    }

    #[tokio::test]
    async fn test_is_mutable() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        assert!(MutableCommitsDirectory::is_mutable(&repo, &commit_id).unwrap());
        assert!(
            !MutableCommitsDirectory::is_mutable(&repo, repo.store().root_commit_id()).unwrap()
        );

        let fake_commit_id = CommitId::from_bytes(&[0u8; 32]);
        assert!(!MutableCommitsDirectory::is_mutable(&repo, &fake_commit_id).unwrap());
    }

    #[tokio::test]
    async fn test_get_symlink() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commits_dir = MutableCommitsDirectory::new(repo.clone(), PathBuf::from("../commits"));

        let symlink = commits_dir.get_symlink(&commit_id).unwrap();
        assert!(matches!(
            symlink.file_type().await.unwrap(),
            FileType::Symlink
        ));
        assert_eq!(
            symlink.read_link().await.unwrap(),
            PathBuf::from("../commits").join(commit_id.hex())
        );

        let root_commit_id = repo.store().root_commit_id();
        assert!(matches!(
            commits_dir.get_symlink(root_commit_id),
            Err(JjError::NotFound)
        ));
    }
}
