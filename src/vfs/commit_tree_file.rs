use std::path::PathBuf;
use std::pin::Pin;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::Stream;
use itertools::Itertools;
use jj_lib::backend::CommitId;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::merge::Merge;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;

use crate::jj_error::JjError;
use crate::path_mapper::DirectoryEntry;
use crate::path_mapper::FileType;
use crate::path_mapper::VirtualFile;

pub struct CommitTreeFile {
    commit: Commit,
    path: PathBuf,
}

impl CommitTreeFile {
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
    async fn read(&self) -> Result<Pin<Box<dyn AsyncRead + Send>>, JjError> {
        let repo_path =
            RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;
        let merged_val = self.commit.tree().path_value(&repo_path).await?;

        let resolved_val = merged_val.as_resolved().ok_or(JjError::NotADirectory)?; // TODO: Files with conflicts should also be readable
        let file_id = match resolved_val {
            Some(TreeValue::File { id, .. }) => id,
            None => return Err(JjError::NotFound),
            _ => return Err(JjError::NotAFile), /* TODO: do research on how to handle symlinks
                                                 * and git submodules */
        };

        let reader = self.commit.store().read_file(&repo_path, file_id).await?;
        Ok(reader)
    }

    async fn list<'a>(
        &'a self,
    ) -> Result<Box<dyn Stream<Item = DirectoryEntry> + Send + 'a>, JjError> {
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
                        _ => FileType::File, // TODO: Handle all file types
                    }
                } else {
                    FileType::File
                };

                DirectoryEntry::new(component.as_internal_str(), file_type)
            })
            .collect(); // TODO: No proper pagination here, since the entire iterator needs to be collected

        Ok(Box::new(futures::stream::iter(files)))
    }

    async fn size(&self) -> Result<u64, JjError> {
        if matches!(self.file_type().await?, FileType::Directory) {
            return Ok(0);
        }

        let mut reader = self.read().await?;

        // TODO: we should be able to get the file size without having to read the
        // entire file (requires changes in jj-lib).
        let size = futures::io::copy(&mut reader, &mut futures::io::sink()).await?;
        Ok(size)
    }

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
    use pollster::FutureExt as _;

    use super::*;
    use crate::path_mapper::VirtualFile;
    use crate::vfs::test_helpers::setup_test_repo;

    #[test]
    fn test_list_directory_root() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from(""))
            .block_on()
            .unwrap();

        let stream = commit_tree.list().block_on().unwrap();
        let root_files: Vec<DirectoryEntry> = std::pin::Pin::from(stream).collect().block_on();

        assert_eq!(root_files.len(), 3);
        assert_eq!(root_files[0].name, "dir");
        assert!(matches!(root_files[0].file_type, FileType::Directory));
        assert_eq!(root_files[1].name, "file1.txt");
        assert!(matches!(root_files[1].file_type, FileType::File));
        assert_eq!(root_files[2].name, "symlink");
        assert!(matches!(root_files[2].file_type, FileType::File));
    }

    #[test]
    fn test_list_directory_sub() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("dir"))
            .block_on()
            .unwrap();

        let stream = commit_tree.list().block_on().unwrap();
        let dir_files: Vec<DirectoryEntry> = std::pin::Pin::from(stream).collect().block_on();

        assert_eq!(dir_files.len(), 1);
        let file2 = dir_files.iter().find(|f| f.name == "file2.txt").unwrap();
        assert!(matches!(file2.file_type, FileType::File));
    }

    #[test]
    fn test_file_size() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .block_on()
            .unwrap();

        let size = commit_tree.size().block_on().unwrap();
        assert_eq!(size, b"hello content 1".len() as u64);
    }

    #[test]
    fn test_read_file() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .block_on()
            .unwrap();

        let mut reader = commit_tree.read().block_on().unwrap();
        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).block_on().unwrap();
        assert_eq!(contents, b"hello content 1");
    }

    #[test]
    fn test_invalid_path() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        // Setup CommitTreeFile with invalid path
        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id.clone(),
            PathBuf::from("some/../path"),
        )
        .block_on()
        .unwrap();

        let err = match commit_tree.list().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::InvalidPath));

        let commit_tree =
            CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("some/../path"))
                .block_on()
                .unwrap();
        let err = match commit_tree.read().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::InvalidPath));
    }

    #[test]
    fn test_not_found() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id.clone(),
            PathBuf::from("non_existent_dir"),
        )
        .block_on()
        .unwrap();

        let err = match commit_tree.list().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotFound));

        let commit_tree = CommitTreeFile::new(
            repo.as_ref(),
            commit_id,
            PathBuf::from("non_existent_file.txt"),
        )
        .block_on()
        .unwrap();
        let err = match commit_tree.read().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotFound));
    }

    #[test]
    fn test_is_directory() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("dir"))
            .block_on()
            .unwrap();

        let err = match commit_tree.read().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotAFile));
    }

    #[test]
    fn test_list_file_not_a_directory() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let commit_tree = CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
            .block_on()
            .unwrap();

        let err = match commit_tree.list().block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };
        assert!(matches!(err, JjError::NotADirectory));
    }

    #[test]
    fn test_file_type() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();

        let commit_tree_dir =
            CommitTreeFile::new(repo.as_ref(), commit_id.clone(), PathBuf::from("dir"))
                .block_on()
                .unwrap();
        let type_dir = commit_tree_dir.file_type().block_on().unwrap();
        assert!(matches!(type_dir, FileType::Directory));

        let commit_tree_file =
            CommitTreeFile::new(repo.as_ref(), commit_id, PathBuf::from("file1.txt"))
                .block_on()
                .unwrap();
        let type_file = commit_tree_file.file_type().block_on().unwrap();
        assert!(matches!(type_file, FileType::File));
    }
}
