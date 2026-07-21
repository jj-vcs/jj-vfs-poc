use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;
use itertools::Itertools;
use jj_lib::backend::CommitId;
use jj_lib::backend::TreeValue;
use jj_lib::commit::Commit;
use jj_lib::config::StackedConfig;
use jj_lib::merge::Merge;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo::StoreFactories;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::repo_path::RepoPathComponentBuf;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;

use crate::file::File;
use crate::file::FileType;

/// This trait defines the possible interactions with the jj repository.
pub trait JjRepo {
    type Commit: JjCommit;

    async fn get_commit(
        &self,
        commit_id: &CommitId,
    ) -> Result<Self::Commit, Box<dyn std::error::Error>>;

    fn commits(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = CommitId> + '_>>, Box<dyn std::error::Error>>;
}

// Represents a jj repository on the local filesystem (used for the proof of
// concept)
pub struct JjLibRepo {
    repo: Arc<ReadonlyRepo>,
}

impl JjLibRepo {
    pub async fn from_path(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let config = StackedConfig::with_defaults();
        let user_settings = UserSettings::from_config(config)?;

        let store_factories = StoreFactories::default();
        let working_copy_factories = default_working_copy_factories();

        let loaded_workspace = Workspace::load(
            &user_settings,
            path.as_ref(),
            &store_factories,
            &working_copy_factories,
        )?;
        let repo_loader = loaded_workspace.repo_loader();
        let repo = repo_loader.load_at_head().await?;

        Ok(Self { repo })
    }
}

impl JjRepo for JjLibRepo {
    type Commit = JjLibCommit;

    async fn get_commit(
        &self,
        commit_id: &CommitId,
    ) -> Result<Self::Commit, Box<dyn std::error::Error>> {
        let commit = self.repo.store().get_commit_async(commit_id).await?;

        Ok(JjLibCommit { commit })
    }

    fn commits(
        &self,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = CommitId> + '_>>, Box<dyn std::error::Error>>
    {
        use futures::StreamExt as _;

        let repo = self.repo.as_ref();
        let expression = jj_lib::revset::ResolvedRevsetExpression::all();
        let revset = expression.evaluate(repo)?;
        let stream = revset
            .stream()
            .filter_map(|commit_id_res| futures::future::ready(commit_id_res.ok()));
        Ok(Box::pin(stream))
    }
}

/// Represents a single commit from a jj repository
#[async_trait]
pub trait JjCommit {
    async fn list_directory(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<Box<dyn Iterator<Item = File>>, Box<dyn std::error::Error>>;

    async fn read_file(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>, Box<dyn std::error::Error>>;

    async fn file_size(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<u64, Box<dyn std::error::Error>>;
}

/// Implementation of JjCommit. Interacts with jj-lib.
pub struct JjLibCommit {
    commit: Commit,
}

// TODO: Implement proper error handling and use a custom error type throughout
#[async_trait]
impl JjCommit for JjLibCommit {
    async fn list_directory(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<Box<dyn Iterator<Item = File>>, Box<dyn std::error::Error>> {
        let repo_path = RepoPathBuf::from_relative_path(path)?;
        let root_tree = self.commit.tree();

        let trees = root_tree.trees().await?;
        let sub_trees = trees
            .sub_tree_recursive(&repo_path)
            .await?
            .ok_or_else(|| format!("Path {:?} not found", repo_path))?;

        let files: Vec<File> = sub_trees
            .iter()
            .flat_map(|tree| tree.entries_non_recursive())
            .map(|entry| entry.name().as_internal_str().to_string())
            .unique() // TODO: .skip of iterator may still have to iterate through all elements to eliminate
            // duplicates (no proper pagination).
            .map(|basename| {
                let component = RepoPathComponentBuf::new(&basename).unwrap();
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

                File::new(&basename, file_type)
            })
            .collect();

        Ok(Box::new(files.into_iter()))
    }

    async fn file_size(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut reader = self.read_file(path).await?;

        // TODO: we should be able to get the file size without having to read the
        // entire file (requires changes in jj-lib).
        let size = futures::io::copy(&mut reader, &mut futures::io::sink()).await?;
        Ok(size)
    }

    async fn read_file(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>, Box<dyn std::error::Error>> {
        let path_ref = path.as_ref();
        let repo_path = RepoPathBuf::from_relative_path(path_ref)?;
        let root_tree = self.commit.tree();
        let merged_val = root_tree.path_value(&repo_path).await?;

        let tree_value = merged_val
            .as_resolved()
            .ok_or("File has conflicts or is not fully resolved")? // TODO: Files with conflicts should also be readable
            .as_ref()
            .ok_or_else(|| format!("File not found at path {:?}", path_ref))?;

        let file_id = match tree_value {
            TreeValue::File { id, .. } => id,
            TreeValue::Tree(_) => return Err("Path is a directory".into()),
            TreeValue::Symlink(_) => return Err("Path is a symlink".into()),
            TreeValue::GitSubmodule(_) => return Err("Path is a Git submodule".into()),
        };

        let reader = self.commit.store().read_file(&repo_path, &file_id).await?;
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::AsyncReadExt as _;
    use jj_lib::backend::ChangeId;
    use jj_lib::backend::CopyId;
    use jj_lib::backend::MillisSinceEpoch;
    use jj_lib::backend::Signature;
    use jj_lib::backend::Timestamp;
    use jj_lib::backend::TreeValue;
    use jj_lib::backend::{self};
    use jj_lib::config::ConfigLayer;
    use jj_lib::config::ConfigSource;
    use jj_lib::config::StackedConfig;
    use jj_lib::merge::Merge;
    use jj_lib::repo::Repo;
    use jj_lib::repo_path::RepoPathBuf;
    use jj_lib::settings::UserSettings;
    use jj_lib::tree_builder::TreeBuilder;
    use jj_lib::workspace::Workspace;
    use pollster::FutureExt as _;

    use super::*;
    use crate::file::FileType;

    fn user_settings() -> UserSettings {
        let config_text = r#"
            user.name = "Test User"
            user.email = "test.user@example.com"
            operation.username = "test-username"
            operation.hostname = "host.example.com"
            debug.randomness-seed = 42
        "#;
        let mut config = StackedConfig::with_defaults();
        config.add_layer(ConfigLayer::parse(ConfigSource::User, config_text).unwrap());
        UserSettings::from_config(config).unwrap()
    }

    fn setup_test_repo() -> (tempfile::TempDir, JjLibCommit) {
        let settings = user_settings();
        let temp_dir = tempfile::tempdir().unwrap();
        let workspace_root = temp_dir.path().to_path_buf();

        // Initialize a simple workspace
        let (workspace, repo) = Workspace::init_simple(&settings, &workspace_root)
            .block_on()
            .unwrap();

        let store = repo.store().clone();

        // Create a tree with a file and a subdirectory containing a file
        let mut tree_builder = TreeBuilder::new(store.clone(), store.empty_tree_id().clone());

        let file1_path = RepoPathBuf::from_relative_path(Path::new("file1.txt")).unwrap();
        let file2_path = RepoPathBuf::from_relative_path(Path::new("dir/file2.txt")).unwrap();

        let file1_content = b"hello content 1";
        let file2_content = b"hello content 2";

        let file1_id = store
            .write_file(&file1_path, &mut &file1_content[..])
            .block_on()
            .unwrap();
        tree_builder.set(
            file1_path.clone(),
            TreeValue::File {
                id: file1_id,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        );

        let file2_id = store
            .write_file(&file2_path, &mut &file2_content[..])
            .block_on()
            .unwrap();
        tree_builder.set(
            file2_path.clone(),
            TreeValue::File {
                id: file2_id,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        );

        let tree_id = tree_builder.write_tree().block_on().unwrap();

        // Write a commit pointing to this tree
        let signature = Signature {
            name: "Test User".to_string(),
            email: "test.user@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit = backend::Commit {
            parents: vec![store.root_commit_id().clone()],
            predecessors: vec![],
            root_tree: Merge::resolved(tree_id),
            conflict_labels: Merge::resolved("".to_string()),
            change_id: ChangeId::from_hex("0000000000000000000000000000abcd"),
            description: "test commit".to_string(),
            author: signature.clone(),
            committer: signature,
            secure_sig: None,
        };
        let commit = store.write_commit(commit, None).block_on().unwrap();

        // Update the working copy commit ID for the workspace name
        let mut tx = repo.start_transaction();
        tx.repo_mut().add_head(&commit).block_on().unwrap();
        tx.repo_mut()
            .set_wc_commit(workspace.workspace_name().to_owned(), commit.id().clone())
            .unwrap();
        let _repo = tx.commit("set working copy").block_on().unwrap();

        // Now, load the JjRepo from the workspace root path
        let jj_repo = JjLibRepo::from_path(&workspace_root)
            .block_on()
            .unwrap()
            .get_commit(commit.id())
            .block_on()
            .unwrap();

        (temp_dir, jj_repo)
    }

    #[test]
    fn test_list_directory_root() {
        let (_temp_dir, jj_repo) = setup_test_repo();

        let root_files: Vec<File> = jj_repo
            .list_directory(Path::new(""))
            .block_on()
            .unwrap()
            .collect();
        assert_eq!(root_files.len(), 2);
        assert_eq!(root_files[0].name, "dir");
        assert_eq!(root_files[0].file_type, FileType::Directory);
        assert_eq!(root_files[1].name, "file1.txt");
        assert_eq!(root_files[1].file_type, FileType::File);
    }

    #[test]
    fn test_list_directory_sub() {
        let (_temp_dir, jj_repo) = setup_test_repo();

        let dir_files: Vec<File> = jj_repo
            .list_directory(Path::new("dir"))
            .block_on()
            .unwrap()
            .collect();
        assert_eq!(dir_files.len(), 1);
        let file2 = dir_files.iter().find(|f| f.name == "file2.txt").unwrap();
        assert_eq!(file2.file_type, FileType::File);
    }

    #[test]
    fn test_file_size() {
        let (_temp_dir, jj_repo) = setup_test_repo();

        let size = jj_repo
            .file_size(Path::new("file1.txt"))
            .block_on()
            .unwrap();
        assert_eq!(size, b"hello content 1".len() as u64);
    }

    #[test]
    fn test_read_file() {
        let (_temp_dir, jj_repo) = setup_test_repo();

        let mut reader = jj_repo
            .read_file(Path::new("file1.txt"))
            .block_on()
            .unwrap();
        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).block_on().unwrap();
        assert_eq!(contents, b"hello content 1");
    }

    #[test]
    fn test_repo_commits() {
        use futures::StreamExt as _;
        let (temp_dir, _) = setup_test_repo();
        let workspace_root = temp_dir.path().to_path_buf();

        let repo = JjLibRepo::from_path(&workspace_root).block_on().unwrap();
        let commits: Vec<CommitId> = repo.commits().unwrap().collect().block_on();

        // The repository has: root commit + initial workspace commit + our test commit,
        // so 3 in total.
        assert_eq!(commits.len(), 3);
    }

    #[test]
    fn test_get_commit() {
        use futures::StreamExt as _;
        let (temp_dir, _) = setup_test_repo();
        let workspace_root = temp_dir.path().to_path_buf();

        let repo = JjLibRepo::from_path(&workspace_root).block_on().unwrap();
        let commits: Vec<CommitId> = repo.commits().unwrap().collect().block_on();

        for commit_id in &commits {
            let commit = repo.get_commit(commit_id).block_on();
            assert!(commit.is_ok());
        }
    }
}
