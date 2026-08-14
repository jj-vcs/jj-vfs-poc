use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::CommitId;
use jj_lib::repo::ReadonlyRepo;

use crate::commit_tree_file::CommitTreeFile;
use crate::commits_directory::CommitsDirectory;
use crate::hardcoded_directory::HardcodedDirectory;
use crate::jj_error::JjError;
use crate::path_mapper::PathMapper;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct AllCommitsPathMapper {
    repo: Arc<ReadonlyRepo>,
}

impl AllCommitsPathMapper {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl PathMapper for AllCommitsPathMapper {
    #[tracing::instrument(skip(self))]
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
        let mut segments = path.iter();

        let Some(first) = segments.next() else {
            return Ok(Box::new(HardcodedDirectory::new(vec![DirectoryEntry {
                name: "commits".to_string(),
                file_type: FileType::Directory,
            }])));
        };

        match first.to_str().ok_or(JjError::InvalidPath)? {
            "commits" => {
                let Some(commit_id_str) = segments.next() else {
                    return Ok(Box::new(CommitsDirectory::new(self.repo.clone())));
                };
                let commit_id =
                    CommitId::try_from_hex(commit_id_str.to_str().ok_or(JjError::InvalidPath)?)
                        .ok_or(JjError::NotFound)?;
                Ok(Box::new(
                    CommitTreeFile::new(&self.repo, commit_id, segments.collect()).await?,
                ))
            }
            _ => Err(JjError::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use jj_lib::object_id::ObjectId;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_all_commit_trees_mapper_root() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper.get_entry(Path::new("")).await.unwrap();
        assert!(entry.list().await.is_ok());
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_commit_root() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let commit_hex = commit_id.hex();
        let path = Path::new("commits").join(&commit_hex);

        let entry = mapper.get_entry(&path).await.unwrap();
        assert!(entry.list().await.is_ok());
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_commit_subpath() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let commit_hex = commit_id.hex();
        let path = Path::new("commits").join(&commit_hex).join("file1.txt");

        let entry = mapper.get_entry(&path).await.unwrap();
        assert!(entry.read().await.is_ok());
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_invalid_commit_id() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("invalid_commit_hex");
        let err = match mapper.get_entry(path).await {
            Err(e) => e,
            Ok(_) => panic!("Expected NotFound error"),
        };
        assert!(matches!(err, JjError::NotFound));
    }
}
