use std::path::PathBuf;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use jj_lib::backend::CommitId;
use jj_lib::object_id::ObjectId;
use jj_lib::ref_name::RefName;
use jj_lib::repo::ReadonlyRepo;

use crate::jj_error::JjError;
use crate::jj_error::JjResult;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

/// This is a `VirtualFile` that represents a directory where local bookmarks
/// are listed as symlinks pointing to `<prefix_path>/<commit_id>`.
pub struct BookmarksDirectory {
    repo: Arc<ReadonlyRepo>,
    prefix_path: PathBuf,
}

impl BookmarksDirectory {
    pub fn new(repo: Arc<ReadonlyRepo>, prefix_path: PathBuf) -> Self {
        Self { repo, prefix_path }
    }

    pub fn get_bookmark_commit_id(repo: &ReadonlyRepo, bookmark_name: &str) -> JjResult<CommitId> {
        let view = repo.view();
        let target = view.get_local_bookmark(RefName::new(bookmark_name));
        target.as_normal().cloned().ok_or(JjError::NotFound)
    }

    pub fn get_symlink(&self, bookmark_name: &str) -> JjResult<Box<dyn VirtualFile>> {
        let commit_id = Self::get_bookmark_commit_id(&self.repo, bookmark_name)?;
        let target = self.prefix_path.join(commit_id.hex());
        Ok(Box::new(BookmarkSymlink::new(target)))
    }
}

#[async_trait]
impl VirtualFile for BookmarksDirectory {
    #[tracing::instrument(skip(self))]
    async fn list(&self) -> JjResult<DirectoryStream> {
        let view = self.repo.view();
        let mut bookmarks: Vec<DirectoryEntry> = Vec::new();
        for (name, target) in view.local_bookmarks() {
            if target.as_normal().is_some() {
                bookmarks.push(DirectoryEntry {
                    name: name.as_str().to_string(),
                    file_type: FileType::Symlink,
                });
            }
        }
        bookmarks.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Box::pin(futures::stream::iter(bookmarks)))
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> JjResult<FileAttributes> {
        Ok(FileAttributes {
            size: 0,
            file_type: FileType::Directory,
            created: UNIX_EPOCH,
            modified: UNIX_EPOCH,
        })
    }

    async fn file_type(&self) -> JjResult<FileType> {
        Ok(FileType::Directory)
    }
}

pub struct BookmarkSymlink {
    target: PathBuf,
}

impl BookmarkSymlink {
    pub fn new(target: PathBuf) -> Self {
        Self { target }
    }
}

#[async_trait]
impl VirtualFile for BookmarkSymlink {
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
    use futures::StreamExt as _;
    use jj_lib::op_store::RefTarget;
    use jj_lib::repo::Repo as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_bookmarks_directory_empty() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let bookmarks_dir = BookmarksDirectory::new(repo.clone(), PathBuf::from("../commits"));
        let stream = bookmarks_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;
        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn test_bookmarks_directory_with_bookmarks() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;

        let mut tx = repo.start_transaction();
        tx.repo_mut()
            .set_local_bookmark_target(RefName::new("main"), RefTarget::normal(commit_id.clone()));
        tx.repo_mut().set_local_bookmark_target(
            RefName::new("feat-test"),
            RefTarget::normal(commit_id.clone()),
        );
        let repo = tx.commit("set bookmarks").await.unwrap();

        let bookmarks_dir = BookmarksDirectory::new(repo.clone(), PathBuf::from("../commits"));
        let stream = bookmarks_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name, "feat-test");
        assert!(matches!(files[0].file_type, FileType::Symlink));
        assert_eq!(files[1].name, "main");
        assert!(matches!(files[1].file_type, FileType::Symlink));

        let symlink = bookmarks_dir.get_symlink("main").unwrap();
        assert!(matches!(
            symlink.file_type().await.unwrap(),
            FileType::Symlink
        ));
        assert_eq!(
            symlink.read_link().await.unwrap(),
            PathBuf::from("../commits").join(commit_id.hex())
        );

        let not_found = bookmarks_dir.get_symlink("nonexistent");
        assert!(matches!(not_found, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_bookmarks_directory_conflicted_skipped() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;

        let root_id = repo.store().root_commit_id().clone();
        let mut tx = repo.start_transaction();
        tx.repo_mut().set_local_bookmark_target(
            RefName::new("conflicted"),
            RefTarget::from_legacy_form(vec![], vec![commit_id.clone(), root_id]),
        );
        let repo = tx.commit("set conflicted bookmark").await.unwrap();

        let bookmarks_dir = BookmarksDirectory::new(repo.clone(), PathBuf::from("../commits"));
        let stream = bookmarks_dir.list().await.unwrap();
        let files: Vec<DirectoryEntry> = stream.collect().await;
        assert_eq!(files.len(), 0);

        let res = bookmarks_dir.get_symlink("conflicted");
        assert!(matches!(res, Err(JjError::NotFound)));
    }
}
