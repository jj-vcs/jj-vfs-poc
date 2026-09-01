use std::path::PathBuf;
use std::pin::Pin;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use futures::AsyncRead;
use itertools::Itertools;
use jj_lib::backend::CommitId;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::merge::Merge;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;

use crate::jj_error::JjError;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct CommitTreeFile {
    commit: Commit,
    path: PathBuf,
}

impl CommitTreeFile {
    #[tracing::instrument(skip(repo))]
    pub async fn new(
        repo: &ReadonlyRepo,
        commit_id: CommitId,
        path: PathBuf,
    ) -> Result<Self, JjError> {
        let commit = repo.store().get_commit_async(&commit_id).await?;
        Ok(Self { commit, path })
    }
}

#[async_trait]
impl VirtualFile for CommitTreeFile {
    #[tracing::instrument(skip(self))]
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError> {
        let repo_path =
            RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;
        let merged_val = self.commit.tree().path_value(&repo_path).await?;

        let resolved_val = merged_val.as_resolved().ok_or(JjError::NotADirectory)?; // TODO: Files with conflicts should also be readable
        let file_id = match resolved_val {
            Some(TreeValue::File { id, .. }) => id,
            None => return Err(JjError::NotFound),
            _ => return Err(JjError::NotAFile), /* TODO: do research on how to handle git
                                                 * submodules */
        };

        let reader = self.commit.store().read_file(&repo_path, file_id).await?;
        Ok(reader)
    }

    #[tracing::instrument(skip(self))]
    async fn list(&self) -> Result<DirectoryStream, JjError> {
        let repo_path =
            RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;
        let root_tree = self.commit.tree();

        let trees = root_tree.trees().await?;
        let Some(sub_trees) = trees.sub_tree_recursive(&repo_path).await? else {
            let merged_val = root_tree.path_value(&repo_path).await?;
            let resolved_val = merged_val.as_resolved().ok_or(JjError::NotADirectory)?;
            return Err(match resolved_val {
                Some(TreeValue::Tree(_)) | None => JjError::NotFound,
                _ => JjError::NotADirectory,
            });
        };

        let files: Vec<DirectoryEntry> = sub_trees
            .iter()
            .flat_map(|tree| tree.entries_non_recursive())
            .map(|entry| entry.name().to_owned())
            .unique() // TODO: .skip of iterator may still have to iterate through all elements to eliminate
            // duplicates (no proper pagination).
            .map(|component| {
                let merged_val: Merge<Option<TreeValue>> =
                    sub_trees.map(|tree| tree.value(&component).cloned());

                let file_type = if let Some(maybe_val) = merged_val.as_resolved() {
                    match maybe_val {
                        Some(TreeValue::Tree(_)) => FileType::Directory,
                        Some(TreeValue::File { .. }) => FileType::File,
                        Some(TreeValue::Symlink(_)) => FileType::Symlink,
                        _ => FileType::File, // TODO: Handle all file types
                    }
                } else {
                    FileType::File
                };

                DirectoryEntry {
                    name: component.as_internal_str().to_string(),
                    file_type,
                }
            })
            .collect(); // TODO: No proper pagination here, since the entire iterator needs to be collected

        Ok(Box::pin(futures::stream::iter(files)))
    }

    #[tracing::instrument(skip(self))]
    async fn read_link(&self) -> Result<PathBuf, JjError> {
        let repo_path =
            RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;
        let merged_val = self.commit.tree().path_value(&repo_path).await?;

        let resolved_val = merged_val.as_resolved().ok_or(JjError::NotASymlink)?;
        let symlink_id = match resolved_val {
            Some(TreeValue::Symlink(id)) => id,
            None => return Err(JjError::NotFound),
            _ => return Err(JjError::NotASymlink),
        };

        let target = self
            .commit
            .store()
            .read_symlink(&repo_path, symlink_id)
            .await?;
        Ok(PathBuf::from(target))
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> Result<FileAttributes, JjError> {
        let file_type = self.file_type().await?;
        let size = match file_type {
            FileType::File => {
                let mut reader = self.read().await?;

                // TODO: we should be able to get the file size without having
                // to read the entire file (requires changes in
                // jj-lib).
                futures::io::copy(&mut reader, &mut futures::io::sink()).await?
            }
            FileType::Directory => 0,
            FileType::Symlink => {
                let target = self.read_link().await?;
                target.as_os_str().len() as u64
            }
        };

        Ok(FileAttributes {
            size,
            file_type,
            created: UNIX_EPOCH, // TODO: implement proper timestamps
            modified: UNIX_EPOCH,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn file_type(&self) -> Result<FileType, JjError> {
        let repo_path =
            RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;
        let merged_val = self.commit.tree().path_value(&repo_path).await?;

        let resolved_val = merged_val.as_resolved().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Conflicted path", /* TODO: support file conflicts to be shown in the
                                * filesystem */
        ))?;
        let file_type = match resolved_val {
            Some(TreeValue::Tree(_)) => FileType::Directory,
            Some(TreeValue::File { .. }) => FileType::File,
            Some(TreeValue::Symlink(_)) => FileType::Symlink,
            _ => FileType::File, // TODO: Handle all file types
        };
        Ok(file_type)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::AsyncReadExt as _;
    use futures::StreamExt as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;
    use crate::virtual_file::VirtualFile;

    #[tokio::test]
    async fn test_list_directory_root() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from(""))
            .await
            .unwrap();

        let stream = commit_tree.list().await.unwrap();
        let root_files: Vec<DirectoryEntry> = stream.collect().await;

        assert_eq!(root_files.len(), 3);
        assert_eq!(root_files[0].name, "dir");
        assert!(matches!(root_files[0].file_type, FileType::Directory));
        assert_eq!(root_files[1].name, "file1.txt");
        assert!(matches!(root_files[1].file_type, FileType::File));
        assert_eq!(root_files[2].name, "symlink");
        assert!(matches!(root_files[2].file_type, FileType::Symlink));
    }

    #[tokio::test]
    async fn test_list_directory_sub() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("dir"))
            .await
            .unwrap();

        let stream = commit_tree.list().await.unwrap();
        let dir_files: Vec<DirectoryEntry> = stream.collect().await;

        assert_eq!(dir_files.len(), 1);
        let file2 = dir_files.iter().find(|f| f.name == "file2.txt").unwrap();
        assert!(matches!(file2.file_type, FileType::File));
    }

    #[tokio::test]
    async fn test_file_size() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .await
            .unwrap();

        let size = commit_tree.attributes().await.unwrap().size;
        assert_eq!(size, b"hello content 1".len() as u64);
    }

    #[tokio::test]
    async fn test_read_file() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .await
            .unwrap();

        let mut reader = commit_tree.read().await.unwrap();
        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"hello content 1");
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        // Setup CommitTreeFile with invalid path
        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id.clone(),
            PathBuf::from("some/../path"),
        )
        .await
        .unwrap();

        let err = match commit_tree.list().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::InvalidPath));

        let commit_tree =
            CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("some/../path"))
                .await
                .unwrap();
        let err = match commit_tree.read().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::InvalidPath));
    }

    #[tokio::test]
    async fn test_not_found() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id.clone(),
            PathBuf::from("non_existent_dir"),
        )
        .await
        .unwrap();

        let err = match commit_tree.list().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotFound));

        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id,
            PathBuf::from("non_existent_file.txt"),
        )
        .await
        .unwrap();
        let err = match commit_tree.read().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotFound));
    }

    #[tokio::test]
    async fn test_is_directory() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("dir"))
            .await
            .unwrap();

        let err = match commit_tree.read().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotAFile));
    }

    #[tokio::test]
    async fn test_list_file_not_a_directory() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .await
            .unwrap();

        let err = match commit_tree.list().await {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotADirectory));
    }

    #[tokio::test]
    async fn test_file_type() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;

        let commit_tree_dir =
            CommitTreeFile::new(repo.as_ref(), commit_id.clone(), PathBuf::from("dir"))
                .await
                .unwrap();
        let type_dir = commit_tree_dir.file_type().await.unwrap();
        assert!(matches!(type_dir, FileType::Directory));

        let commit_tree_file =
            CommitTreeFile::new(repo.as_ref(), commit_id.clone(), PathBuf::from("file1.txt"))
                .await
                .unwrap();
        let type_file = commit_tree_file.file_type().await.unwrap();
        assert!(matches!(type_file, FileType::File));

        let commit_tree_symlink =
            CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("symlink"))
                .await
                .unwrap();
        let type_symlink = commit_tree_symlink.file_type().await.unwrap();
        assert!(matches!(type_symlink, FileType::Symlink));
    }

    #[tokio::test]
    async fn test_read_symlink_file() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("symlink"))
            .await
            .unwrap();

        let target = commit_tree.read_link().await.unwrap();
        assert_eq!(target, PathBuf::from("file1.txt"));
    }
}
