use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
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
    async fn create(&self, _file: CreateFile) -> JjResult<FileAttributes> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn write(&self, offset: u64, data: &[u8]) -> JjResult<u32> {
        let path = RepoPathBuf::from_relative_path(&self.path).map_err(|_| JjError::InvalidPath)?;

        let wc_commit_id = self
            .repo
            .view()
            .get_wc_commit_id(&self.workspace_name)
            .cloned()
            .ok_or(JjError::NotFound)?;
        let commit = self.repo.store().get_commit_async(&wc_commit_id).await?;

        let current_value = commit.tree().path_value(&path).await?;
        let resolved_val = current_value.as_resolved().ok_or(JjError::NotAFile)?;
        let (file_id, executable) = match resolved_val {
            Some(TreeValue::File { id, executable, .. }) => (id, *executable),
            Some(TreeValue::Tree(_)) => return Err(JjError::NotAFile),
            Some(TreeValue::Symlink(_)) => return Err(JjError::NotAFile),
            None => return Err(JjError::NotFound),
            _ => return Err(JjError::NotAFile),
        };

        let mut reader = self.repo.store().read_file(&path, file_id).await?;
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await?;

        let offset = usize::try_from(offset).map_err(|_| {
            JjError::IO(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Offset too large",
            ))
        })?;
        if offset + data.len() > content.len() {
            content.resize(offset + data.len(), 0);
        }
        content[offset..offset + data.len()].copy_from_slice(data);

        let new_file_id: FileId = self.repo.store().write_file(&path, &mut &content[..]).await?;

        let tree_value = TreeValue::File {
            id: new_file_id,
            executable,
            copy_id: CopyId::placeholder(),
        };

        let bytes_written = data.len() as u32;

        Self::update_file(
            self.repo.clone(),
            self.workspace_name.clone(),
            path,
            Some(tree_value),
            "Update file content".to_string(),
        )
        .await?;

        Ok(bytes_written)
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self) -> JjResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::test_helpers::setup_test_repo;
    use crate::virtual_file::VirtualFile;

    #[tokio::test]
    async fn test_write_file() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let commit_tree = WritableCommitTreeFile::new(
            repo.clone(),
            WorkspaceNameBuf::from("default"),
            PathBuf::from("file1.txt"),
        );

        let bytes_written = commit_tree.write(6, b"updated content").await.unwrap();
        assert_eq!(bytes_written, 15);

        let reloaded_repo = repo.reload_at_head().await.unwrap();
        let new_commit_tree = WritableCommitTreeFile::new(
            reloaded_repo,
            WorkspaceNameBuf::from("default"),
            PathBuf::from("file1.txt"),
        );

        let mut reader = new_commit_tree.read().await.unwrap();
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.unwrap();
        assert_eq!(content, b"hello updated content");
    }

    #[tokio::test]
    async fn test_write_file_not_found() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let commit_tree = WritableCommitTreeFile::new(
            repo,
            WorkspaceNameBuf::from("default"),
            PathBuf::from("nonexistent.txt"),
        );

        let res = commit_tree.write(0, b"data").await;
        assert!(matches!(res, Err(JjError::NotFound)));
    }

    #[tokio::test]
    async fn test_write_file_not_a_file() {
        let (_temp_dir, repo, _commit_id) = setup_test_repo().await;
        let commit_tree = WritableCommitTreeFile::new(
            repo,
            WorkspaceNameBuf::from("default"),
            PathBuf::from("dir"),
        );

        let res = commit_tree.write(0, b"data").await;
        assert!(matches!(res, Err(JjError::NotAFile)));
    }
}
