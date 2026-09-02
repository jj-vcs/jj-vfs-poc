use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use futures::AsyncRead;
use jj_lib::backend::CopyId;
use jj_lib::backend::FileId;
use jj_lib::backend::TreeValue;
use jj_lib::merge::Merge;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::ref_name::WorkspaceNameBuf;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo;
use jj_lib::repo_path::RepoPathBuf;

use crate::jj_error::JjError;
use crate::jj_error::JjResult;
use crate::readonly_commit_tree_file::ReadonlyCommitTreeFile;
use crate::virtual_file::CreateFile;
use crate::virtual_file::DirectoryStream;
use crate::virtual_file::FileAttributes;
use crate::virtual_file::FileType;
use crate::virtual_file::VirtualFile;

pub struct WritableCommitTreeFile {
    repo: Arc<ReadonlyRepo>,
    workspace_name: WorkspaceNameBuf,
    path: PathBuf,
}

impl WritableCommitTreeFile {
    pub fn new(repo: Arc<ReadonlyRepo>, workspace_name: WorkspaceNameBuf, path: PathBuf) -> Self {
        Self {
            repo,
            workspace_name,
            path,
        }
    }

    async fn to_readonly(&self) -> JjResult<ReadonlyCommitTreeFile> {
        let wc_commit_id = self
            .repo
            .view()
            .get_wc_commit_id(&self.workspace_name)
            .cloned()
            .ok_or(JjError::NotFound)?;
        ReadonlyCommitTreeFile::new(&self.repo, wc_commit_id, self.path.clone()).await
    }

    async fn update_file(
        repo: Arc<ReadonlyRepo>,
        workspace_name: WorkspaceNameBuf,
        path: RepoPathBuf,
        value: Option<TreeValue>,
        transaction_description: String,
    ) -> JjResult<()> {
        tokio::task::spawn_blocking(move || {
            pollster::block_on(async move {
                let commit_id = repo
                    .view()
                    .get_wc_commit_id(&workspace_name)
                    .cloned()
                    .ok_or(JjError::NotFound)?;
                let commit = repo.store().get_commit_async(&commit_id).await?;

                let mut tree_builder = MergedTreeBuilder::new(commit.tree());
                tree_builder.set_or_remove(path, Merge::resolved(value));

                let new_tree = tree_builder.write_tree().await?;

                let mut tx = repo.start_transaction();

                let mut workspaces_to_update = Vec::new();
                for (workspace_name_it, wc_commit_id) in tx.repo().view().wc_commit_ids() {
                    if wc_commit_id == &commit_id {
                        workspaces_to_update.push(workspace_name_it.clone());
                    }
                }

                if workspaces_to_update.is_empty() {
                    return Err(JjError::Readonly);
                }

                let mut_repo = tx.repo_mut();

                let new_commit = mut_repo
                    .rewrite_commit(&commit)
                    .set_tree(new_tree)
                    .write()
                    .await?;

                for workspace_name_it in workspaces_to_update {
                    mut_repo
                        .set_wc_commit(workspace_name_it, new_commit.id().clone())
                        .map_err(|_| JjError::InvalidPath)?;
                }

                mut_repo.rebase_descendants().await.map_err(|e| {
                    JjError::IO(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

                tx.commit(transaction_description).await.map_err(|e| {
                    JjError::IO(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

                Ok(())
            })
        })
        .await
        .map_err(|e| JjError::IO(std::io::Error::other(format!("spawn_blocking failed: {e}"))))?
    }
}

#[async_trait]
impl VirtualFile for WritableCommitTreeFile {
    #[tracing::instrument(skip(self))]
    async fn read(&self) -> JjResult<Pin<Box<dyn AsyncRead + Send>>> {
        self.to_readonly().await?.read().await
    }

    #[tracing::instrument(skip(self))]
    async fn list(&self) -> JjResult<DirectoryStream> {
        self.to_readonly().await?.list().await
    }

    #[tracing::instrument(skip(self))]
    async fn read_link(&self) -> JjResult<PathBuf> {
        self.to_readonly().await?.read_link().await
    }

    #[tracing::instrument(skip(self))]
    async fn attributes(&self) -> JjResult<FileAttributes> {
        self.to_readonly().await?.attributes().await
    }

    #[tracing::instrument(skip(self))]
    async fn file_type(&self) -> JjResult<FileType> {
        self.to_readonly().await?.file_type().await
    }

    #[tracing::instrument(skip(self))]
    async fn create(&self, file: CreateFile) -> JjResult<FileAttributes> {
        if !matches!(self.file_type().await?, FileType::Directory) {
            return Err(JjError::NotADirectory);
        }

        let store = self.repo.store();
        let path = RepoPathBuf::from_relative_path(&self.path.join(file.name())).map_err(|_| JjError::InvalidPath)?;
        let (tree_value, size, file_type) = match file {
            CreateFile::File { executable, .. } => {
                let empty_content: &[u8] = &[];
                let file_id: FileId = store
                    .write_file(&path, &mut &empty_content[..])
                    .await?;
                (
                    TreeValue::File {
                        id: file_id,
                        executable,
                        copy_id: CopyId::placeholder(),
                    },
                    0,
                    FileType::File,
                )
            }
            CreateFile::Directory { .. } => (
                TreeValue::Tree(store.empty_tree_id().clone()),
                0,
                FileType::Directory,
            ),
            CreateFile::Symlink { target, .. } => {
                let symlink_id = store.write_symlink(&path, &target).await?;
                (
                    TreeValue::Symlink(symlink_id),
                    target.len() as u64,
                    FileType::Symlink,
                )
            }
        };

        let repo = self.repo.clone();
        let workspace_name = self.workspace_name.clone();

        WritableCommitTreeFile::update_file(
            repo,
            workspace_name,
            path,
            Some(tree_value),
            "Create empty file".to_string(),
        )
        .await?;

        Ok(FileAttributes {
            size,
            file_type,
            created: UNIX_EPOCH, // TODO: implement proper timestamps
            modified: UNIX_EPOCH,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn write(&self, _offset: u64, _data: &[u8]) -> JjResult<u32> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self) -> JjResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use futures::AsyncReadExt as _;
    use futures::StreamExt as _;
    use jj_lib::backend::TreeValue;
    use jj_lib::ref_name::WorkspaceNameBuf;
    use jj_lib::repo_path::RepoPathBuf;

    use super::*;
    use crate::test_helpers::setup_test_repo;
    use crate::virtual_file::CreateFile;
    use crate::virtual_file::DirectoryEntry;
    use crate::virtual_file::FileType;
    use crate::virtual_file::VirtualFile;

    #[tokio::test]
    async fn test_create_file() {
        let (_temp_dir, repo, initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from(""));
        let attrs = root
            .create(CreateFile::File {
                name: "new_file.txt".to_string(),
                executable: false,
            })
            .await
            .unwrap();

        assert_eq!(attrs.size, 0);
        assert!(matches!(attrs.file_type, FileType::File));
        assert_eq!(attrs.created, UNIX_EPOCH);
        assert_eq!(attrs.modified, UNIX_EPOCH);

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_wc_commit_id = reloaded_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();
        assert_ne!(new_wc_commit_id, initial_commit_id);

        let new_file = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id.clone(),
            PathBuf::from("new_file.txt"),
        )
        .await
        .unwrap();

        assert!(matches!(new_file.file_type().await.unwrap(), FileType::File));
        assert_eq!(new_file.attributes().await.unwrap().size, 0);

        let mut reader = new_file.read().await.unwrap();
        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).await.unwrap();
        assert_eq!(contents, b"");

        let reloaded_root = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id,
            PathBuf::from(""),
        )
        .await
        .unwrap();
        let stream = reloaded_root.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = stream.collect().await;
        assert!(entries.iter().any(|e| e.name == "new_file.txt" && matches!(e.file_type, FileType::File)));
    }

    #[tokio::test]
    async fn test_create_executable_file() {
        let (_temp_dir, repo, initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from(""));
        let attrs = root
            .create(CreateFile::File {
                name: "script.sh".to_string(),
                executable: true,
            })
            .await
            .unwrap();

        assert_eq!(attrs.size, 0);
        assert!(matches!(attrs.file_type, FileType::File));

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_wc_commit_id = reloaded_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();
        assert_ne!(new_wc_commit_id, initial_commit_id);

        let commit = reloaded_repo
            .store()
            .get_commit_async(&new_wc_commit_id)
            .await
            .unwrap();
        let repo_path = RepoPathBuf::from_relative_path(Path::new("script.sh")).unwrap();
        let tree_val = commit.tree().path_value(&repo_path).await.unwrap();
        assert!(matches!(
            tree_val.as_resolved(),
            Some(Some(TreeValue::File { executable: true, .. }))
        ));
    }

    #[tokio::test]
    async fn test_create_directory() {
        let (_temp_dir, repo, initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from(""));
        let attrs = root
            .create(CreateFile::Directory {
                name: "new_dir".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(attrs.size, 0);
        assert!(matches!(attrs.file_type, FileType::Directory));

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_wc_commit_id = reloaded_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();
        assert_ne!(new_wc_commit_id, initial_commit_id);

        let new_dir = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id.clone(),
            PathBuf::from("new_dir"),
        )
        .await
        .unwrap();
        assert!(matches!(new_dir.file_type().await.unwrap(), FileType::Directory));

        let stream = new_dir.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = stream.collect().await;
        assert!(entries.is_empty());

        let reloaded_root = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id,
            PathBuf::from(""),
        )
        .await
        .unwrap();
        let stream = reloaded_root.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = stream.collect().await;
        assert!(entries.iter().any(|e| e.name == "new_dir" && matches!(e.file_type, FileType::Directory)));
    }

    #[tokio::test]
    async fn test_create_symlink() {
        let (_temp_dir, repo, initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from(""));
        let attrs = root
            .create(CreateFile::Symlink {
                name: "symlink_test".to_string(),
                target: "file1.txt".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(attrs.size, "file1.txt".len() as u64);
        assert!(matches!(attrs.file_type, FileType::Symlink));

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_wc_commit_id = reloaded_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();
        assert_ne!(new_wc_commit_id, initial_commit_id);

        let new_symlink = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id,
            PathBuf::from("symlink_test"),
        )
        .await
        .unwrap();
        assert!(matches!(new_symlink.file_type().await.unwrap(), FileType::Symlink));
        assert_eq!(new_symlink.read_link().await.unwrap(), PathBuf::from("file1.txt"));
        assert_eq!(new_symlink.attributes().await.unwrap().size, "file1.txt".len() as u64);
    }

    #[tokio::test]
    async fn test_create_in_subdirectory() {
        let (_temp_dir, repo, _initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let dir = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from("dir"));
        let attrs = dir
            .create(CreateFile::File {
                name: "sub_file.txt".to_string(),
                executable: false,
            })
            .await
            .unwrap();

        assert_eq!(attrs.size, 0);
        assert!(matches!(attrs.file_type, FileType::File));

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_wc_commit_id = reloaded_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();

        let sub_file = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id.clone(),
            PathBuf::from("dir/sub_file.txt"),
        )
        .await
        .unwrap();
        assert!(matches!(sub_file.file_type().await.unwrap(), FileType::File));

        let dir_readonly = ReadonlyCommitTreeFile::new(
            &reloaded_repo,
            new_wc_commit_id,
            PathBuf::from("dir"),
        )
        .await
        .unwrap();
        let stream = dir_readonly.list().await.unwrap();
        let entries: Vec<DirectoryEntry> = stream.collect().await;
        assert!(entries.iter().any(|e| e.name == "file2.txt"));
        assert!(entries.iter().any(|e| e.name == "sub_file.txt"));
    }

    #[tokio::test]
    async fn test_create_in_newly_created_directory() {
        let (_temp_dir, repo, _initial_commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo.clone(), workspace_name.clone(), PathBuf::from(""));
        root.create(CreateFile::Directory {
            name: "nested_dir".to_string(),
        })
        .await
        .unwrap();

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let nested_dir = WritableCommitTreeFile::new(
            reloaded_repo.clone(),
            workspace_name.clone(),
            PathBuf::from("nested_dir"),
        );
        nested_dir
            .create(CreateFile::File {
                name: "nested_file.txt".to_string(),
                executable: false,
            })
            .await
            .unwrap();

        let final_repo = reloaded_repo.reload_at_head().await.unwrap();
        let final_wc_commit_id = final_repo
            .view()
            .get_wc_commit_id(&workspace_name)
            .cloned()
            .unwrap();

        let nested_file = ReadonlyCommitTreeFile::new(
            &final_repo,
            final_wc_commit_id,
            PathBuf::from("nested_dir/nested_file.txt"),
        )
        .await
        .unwrap();
        assert!(matches!(nested_file.file_type().await.unwrap(), FileType::File));
    }

    #[tokio::test]
    async fn test_create_on_file_fails() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let file = WritableCommitTreeFile::new(repo, workspace_name, PathBuf::from("file1.txt"));
        let err = match file
            .create(CreateFile::File {
                name: "child.txt".to_string(),
                executable: false,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };

        assert!(matches!(err, JjError::NotADirectory));
    }

    #[tokio::test]
    async fn test_create_on_symlink_fails() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let symlink = WritableCommitTreeFile::new(repo, workspace_name, PathBuf::from("symlink"));
        let err = match symlink
            .create(CreateFile::File {
                name: "child.txt".to_string(),
                executable: false,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };

        assert!(matches!(err, JjError::NotADirectory));
    }

    #[tokio::test]
    async fn test_create_on_nonexistent_directory_fails() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let non_existent = WritableCommitTreeFile::new(repo, workspace_name, PathBuf::from("no_such_dir"));
        let err = match non_existent
            .create(CreateFile::File {
                name: "file.txt".to_string(),
                executable: false,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };

        assert!(matches!(err, JjError::NotADirectory | JjError::NotFound));
    }

    #[tokio::test]
    async fn test_create_invalid_filename_fails() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let workspace_name = repo.view().wc_commit_ids().keys().next().unwrap().clone();

        let root = WritableCommitTreeFile::new(repo, workspace_name, PathBuf::from(""));
        let err = match root
            .create(CreateFile::File {
                name: "..".to_string(),
                executable: false,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };

        assert!(matches!(err, JjError::InvalidPath));
    }

    #[tokio::test]
    async fn test_create_unknown_workspace_fails() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;

        let root = WritableCommitTreeFile::new(
            repo,
            WorkspaceNameBuf::from("unknown_workspace"),
            PathBuf::from(""),
        );
        let err = match root
            .create(CreateFile::File {
                name: "test.txt".to_string(),
                executable: false,
            })
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("Expected an error"),
        };

        assert!(matches!(err, JjError::NotFound));
    }
}
