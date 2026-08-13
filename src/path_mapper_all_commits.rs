use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::CommitId;
use jj_lib::repo::ReadonlyRepo;

use crate::commit_tree_file::CommitTreeFile;
use crate::commits_directory::CommitsDirectory;
use crate::jj_error::JjError;
use crate::path_mapper::PathMapper;
use crate::virtual_file::VirtualFile;

pub struct AllCommitsPathMapper {
    repo: Arc<ReadonlyRepo>,
}

impl AllCommitsPathMapper {
    #[tracing::instrument(skip(repo))]
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl PathMapper for AllCommitsPathMapper {
    #[tracing::instrument(skip(self))]
    async fn get_entry(&self, path: &Path) -> Result<Box<dyn VirtualFile>, JjError> {
        if path == Path::new("") {
            Ok(Box::new(CommitsDirectory::new(self.repo.clone())))
        } else {
            let mut components = path.iter();
            let commit_id_str = components
                .next()
                .ok_or(JjError::NotFound)?
                .to_str()
                .ok_or(JjError::InvalidPath)?;
            let commit_id = CommitId::try_from_hex(commit_id_str).ok_or(JjError::NotFound)?;

            let remaining_path: PathBuf = components.collect();

            Ok(Box::new(
                CommitTreeFile::new(&self.repo, commit_id, remaining_path).await?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use jj_lib::object_id::ObjectId;
    use pollster::FutureExt as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[test]
    fn test_all_commit_trees_mapper_root() {
        let (_temp_dir, repo, _commit) = setup_test_repo();
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper.get_entry(Path::new("")).block_on().unwrap();
        assert!(entry.list().block_on().is_ok());
    }

    #[test]
    fn test_all_commit_trees_mapper_commit_root() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let mapper = AllCommitsPathMapper { repo };

        let commit_hex = commit_id.hex();
        let path = Path::new(&commit_hex);

        let entry = mapper.get_entry(path).block_on().unwrap();
        assert!(entry.list().block_on().is_ok());
    }

    #[test]
    fn test_all_commit_trees_mapper_commit_subpath() {
        let (_temp_dir, repo, commit_id) = setup_test_repo();
        let mapper = AllCommitsPathMapper { repo };

        let commit_hex = commit_id.hex();
        let mut path = PathBuf::from(&commit_hex);
        path.push("file1.txt");

        let entry = mapper.get_entry(&path).block_on().unwrap();
        assert!(entry.read().block_on().is_ok());
    }

    #[test]
    fn test_all_commit_trees_mapper_invalid_commit_id() {
        let (_temp_dir, repo, _commit) = setup_test_repo();
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("invalid_commit_hex");
        let err = match mapper.get_entry(path).block_on() {
            Err(e) => e,
            Ok(_) => panic!("Expected NotFound error"),
        };
        assert!(matches!(err, JjError::NotFound));
    }
}
