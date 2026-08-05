use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::Stream;
use futures::StreamExt as _;
use jj_lib::object_id::ObjectId;
use jj_lib::repo::ReadonlyRepo;

use crate::jj_error::JjError;
use crate::path_mapper::DirectoryEntry;
use crate::path_mapper::FileType;
use crate::path_mapper::VirtualFile;

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
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError> {
        Err(JjError::NotAFile)
    }

    async fn list<'a>(&'a self) -> Result<Box<dyn Stream<Item = DirectoryEntry> + 'a>, JjError> {
        let expression = jj_lib::revset::ResolvedRevsetExpression::all();
        let revset = expression
            .evaluate(self.repo.as_ref())
            .map_err(|e| e.into_backend_error())?;
        let stream = revset
            .stream()
            .filter_map(|commit_id_res| futures::future::ready(commit_id_res.ok()))
            .map(|commit_id| DirectoryEntry::new(&commit_id.hex(), FileType::Directory));
        Ok(Box::new(stream))
    }

    async fn size(&self) -> Result<u64, JjError> {
        Ok(0)
    }

    async fn file_type(&self) -> Result<FileType, JjError> {
        Ok(FileType::Directory)
    }
}

#[cfg(test)]
mod tests {
    use pollster::FutureExt as _;

    use super::*;
    use crate::vfs::test_helpers::setup_test_repo;

    #[test]
    fn test_repo_commits() {
        let (_temp_dir, repo, _commit) = setup_test_repo();
        let commits_dir = CommitsDirectory::new(repo);
        let stream = commits_dir.list().block_on().unwrap();
        let files: Vec<DirectoryEntry> = std::pin::Pin::from(stream).collect().block_on();

        // The repository has: root commit + initial workspace commit + our test commit,
        // so 3 in total.
        assert_eq!(files.len(), 3);

        for file in &files {
            assert!(matches!(file.file_type, FileType::Directory));
        }
    }
}
