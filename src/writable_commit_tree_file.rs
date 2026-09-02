use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;
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

    #[allow(dead_code)] // TODO: this will be used in the future for create, write and delete function
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
    async fn write(&self, _offset: u64, _data: &[u8]) -> JjResult<u32> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self) -> JjResult<()> {
        todo!()
    }
}
