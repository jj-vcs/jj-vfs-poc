use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use jj_lib::backend::CommitId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;

use crate::bookmarks_directory::BookmarksDirectory;
use crate::commit_tree_file::CommitTreeFile;
use crate::commits_directory::CommitsDirectory;
use crate::jj_error::JjError;
use crate::jj_error::JjResult;
use crate::mutable_commits_directory::MutableCommitsDirectory;
use crate::path_mapper::PathMapper;
use crate::static_directory::StaticDirectory;
use crate::virtual_file::DirectoryEntry;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;
use crate::workspaces_directory::WorkspacesDirectory;

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
    async fn get_entry(&self, path: &Path) -> JjResult<Box<dyn VirtualFile>> {
        let repo = self.repo.clone();
        let repo = tokio::task::spawn_blocking(move || pollster::block_on(repo.reload_at_head()))
            .await
            .map_err(std::io::Error::other)?
            .map_err(std::io::Error::other)?;

        let mut segments = path.iter();

        let Some(first) = segments.next() else {
            return Ok(Box::new(StaticDirectory::new(vec![
                DirectoryEntry {
                    name: "bookmarks".to_string(),
                    file_type: FileType::Directory,
                },
                DirectoryEntry {
                    name: "commits".to_string(),
                    file_type: FileType::Directory,
                },
                DirectoryEntry {
                    name: "mutable_commits".to_string(),
                    file_type: FileType::Directory,
                },
                DirectoryEntry {
                    name: "workspaces".to_string(),
                    file_type: FileType::Directory,
                },
            ])));
        };

        match first.to_str().ok_or(JjError::InvalidPath)? {
            "commits" => {
                let Some(commit_id_str) = segments.next() else {
                    return Ok(Box::new(CommitsDirectory::new(repo.clone())));
                };
                let commit_id =
                    CommitId::try_from_hex(commit_id_str.to_str().ok_or(JjError::InvalidPath)?)
                        .ok_or(JjError::NotFound)?;
                Ok(Box::new(
                    CommitTreeFile::new(&repo, commit_id, segments.collect()).await?,
                ))
            }
            "workspaces" => {
                let Some(workspace_name_segment) = segments.next() else {
                    return Ok(Box::new(WorkspacesDirectory::new(repo.clone())));
                };
                let workspace_name_str = workspace_name_segment
                    .to_str()
                    .ok_or(JjError::InvalidPath)?;
                let wc_commit_ids = repo.view().wc_commit_ids();
                let workspace_name = WorkspaceName::new(workspace_name_str);
                let commit_id = wc_commit_ids
                    .get(workspace_name)
                    .cloned()
                    .ok_or(JjError::NotFound)?;
                Ok(Box::new(
                    CommitTreeFile::new(&repo, commit_id, segments.collect()).await?,
                ))
            }
            "mutable_commits" => {
                let mutable_dir =
                    MutableCommitsDirectory::new(repo.clone(), PathBuf::from("../commits"));
                let Some(commit_id_str) = segments.next() else {
                    return Ok(Box::new(mutable_dir));
                };
                if segments.next().is_some() {
                    return Err(JjError::NotFound);
                }
                let commit_id =
                    CommitId::try_from_hex(commit_id_str.to_str().ok_or(JjError::InvalidPath)?)
                        .ok_or(JjError::NotFound)?;
                mutable_dir.get_symlink(&commit_id)
            }
            "bookmarks" => {
                let bookmarks_dir =
                    BookmarksDirectory::new(repo.clone(), PathBuf::from("../commits"));
                let Some(bookmark_name_segment) = segments.next() else {
                    return Ok(Box::new(bookmarks_dir));
                };
                if segments.next().is_some() {
                    return Err(JjError::NotFound);
                }
                let bookmark_name_str =
                    bookmark_name_segment.to_str().ok_or(JjError::InvalidPath)?;
                bookmarks_dir.get_symlink(bookmark_name_str)
            }
            _ => Err(JjError::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use jj_lib::object_id::ObjectId;
    use jj_lib::repo::Repo as _;

    use super::*;
    use crate::test_helpers::setup_test_repo;

    #[tokio::test]
    async fn test_all_commit_trees_mapper_root() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper.get_entry(Path::new("")).await.unwrap();
        let list = entry.list().await.unwrap();
        let mut entries: Vec<String> = list.map(|e| e.name).collect().await;
        entries.sort();
        assert_eq!(
            entries,
            vec!["bookmarks", "commits", "mutable_commits", "workspaces"]
        );
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

    #[tokio::test]
    async fn test_all_commit_trees_mapper_workspaces_root() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper.get_entry(Path::new("workspaces")).await.unwrap();
        let list = entry.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = list.collect().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "default");
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_workspace_file() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("workspaces").join("default").join("file1.txt");
        let entry = mapper.get_entry(&path).await.unwrap();
        assert!(entry.read().await.is_ok());
    }
    #[tokio::test]
    async fn test_all_commit_trees_mapper_non_existent_workspace() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("workspaces").join("nonexistent");
        let err = match mapper.get_entry(&path).await {
            Err(e) => e,
            Ok(_) => panic!("Expected NotFound error"),
        };
        assert!(matches!(err, JjError::NotFound));

        let path = Path::new("workspaces")
            .join("nonexistent")
            .join("file1.txt");
        let err = match mapper.get_entry(&path).await {
            Err(e) => e,
            Ok(_) => panic!("Expected NotFound error"),
        };
        assert!(matches!(err, JjError::NotFound));
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_mutable_commits_root() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper
            .get_entry(Path::new("mutable_commits"))
            .await
            .unwrap();
        let list = entry.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = list.collect().await;
        assert_eq!(entries.len(), 2);
        for e in entries {
            assert!(matches!(e.file_type, FileType::Symlink));
        }
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_mutable_commit_entry() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let commit_hex = commit_id.hex();
        let path = Path::new("mutable_commits").join(&commit_hex);
        let entry = mapper.get_entry(&path).await.unwrap();
        assert!(matches!(
            entry.file_type().await.unwrap(),
            FileType::Symlink
        ));
        assert_eq!(
            entry.read_link().await.unwrap(),
            PathBuf::from("../commits").join(&commit_hex)
        );

        let file_path = Path::new("mutable_commits")
            .join(&commit_hex)
            .join("file1.txt");
        let file_entry_res = mapper.get_entry(&file_path).await;
        assert!(matches!(file_entry_res, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_mutable_commits_immutable_not_found() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo: repo.clone() };

        let root_hex = repo.store().root_commit_id().hex();
        let path = Path::new("mutable_commits").join(&root_hex);
        let err = match mapper.get_entry(&path).await {
            Err(e) => e,
            Ok(_) => panic!("Expected NotFound for immutable commit"),
        };
        assert!(matches!(err, JjError::NotFound));
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_bookmarks_root() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;

        let mut tx = repo.start_transaction();
        tx.repo_mut().set_local_bookmark_target(
            jj_lib::ref_name::RefName::new("main"),
            jj_lib::op_store::RefTarget::normal(commit_id),
        );
        let repo = tx.commit("set bookmark").await.unwrap();
        let mapper = AllCommitsPathMapper { repo };

        let entry = mapper.get_entry(Path::new("bookmarks")).await.unwrap();
        let list = entry.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = list.collect().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "main");
        assert!(matches!(entries[0].file_type, FileType::Symlink));
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_bookmark_entry() {
        let (_temp_dir, repo, commit_id) = setup_test_repo().await;

        let commit_hex = commit_id.hex();
        let mut tx = repo.start_transaction();
        tx.repo_mut().set_local_bookmark_target(
            jj_lib::ref_name::RefName::new("feature-1"),
            jj_lib::op_store::RefTarget::normal(commit_id),
        );
        let repo = tx.commit("set bookmark").await.unwrap();
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("bookmarks").join("feature-1");
        let entry = mapper.get_entry(&path).await.unwrap();
        assert!(matches!(
            entry.file_type().await.unwrap(),
            FileType::Symlink
        ));
        assert_eq!(
            entry.read_link().await.unwrap(),
            PathBuf::from("../commits").join(&commit_hex)
        );

        let subpath = path.join("file1.txt");
        let err = mapper.get_entry(&subpath).await;
        assert!(matches!(err, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_all_commit_trees_mapper_bookmark_nonexistent() {
        let (_temp_dir, repo, _commit) = setup_test_repo().await;
        let mapper = AllCommitsPathMapper { repo };

        let path = Path::new("bookmarks").join("nonexistent");
        let err = mapper.get_entry(&path).await;
        assert!(matches!(err, Err(JjError::NotFound)));
    }
}
