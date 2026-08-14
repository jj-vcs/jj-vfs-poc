use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use futures::StreamExt as _;
use jj_lib::object_id::ObjectId;
use jj_lib::repo::ReadonlyRepo;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

/// This is a `VirtualFile` that represents a directory where all the commits
/// are listed, with each commit being a separate directory.
pub struct CommitsDirectory {
    repo: Arc<ReadonlyRepo>,
}

impl CommitsDirectory {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl VirtualFile for CommitsDirectory {
    #[tracing::instrument(skip(self))]
    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let expression = jj_lib::revset::ResolvedRevsetExpression::all();
        let revset = expression
            .evaluate(self.repo.as_ref())
            .map_err(|e| e.into_backend_error())?;
        // Revset does not implement Send, meaning it cannot be sent across threads. We
        // use blocking for now since this will be rewritten in the future anyways (to a
        // version that doesn't list all commits)
        let commits: Vec<DirectoryEntry> = futures::executor::block_on(async {
            revset
                .stream()
                .filter_map(|commit_id_res| futures::future::ready(commit_id_res.ok()))
                .map(|commit_id| DirectoryEntry {
                    name: commit_id.hex(),
                    file_type: FileType::Directory,
                })
                .collect()
                .await
        }); // TODO: currently there is no proper pagination implemented here
        Ok(Box::pin(futures::stream::iter(commits)))
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        Ok(FileAttributes {
            size: 0,
            file_type: FileType::Directory,
            created: UNIX_EPOCH, // TODO: implement proper timestamps
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Directory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_repo_commits() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let commits_dir = CommitsDirectory::new(repo);
        let stream = commits_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;

        // The repository has: root commit + initial workspace commit + our test commit,
        // so 3 in total.
        assert_eq!(files.len(), 3);

        for file in &files {
            assert!(matches!(file.file_type, FileType::Directory));
        }
    }
}
