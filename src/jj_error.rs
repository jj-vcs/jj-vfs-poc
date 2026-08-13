use thiserror::Error;

#[derive(Error, Debug)]
pub enum JjError {
    #[error("Invalid path")]
    InvalidPath,

    #[error("Not found")]
    NotFound,

    #[error("Expected a directory")]
    NotADirectory,

    #[error("Expected a file")]
    NotAFile,

    #[error("Expected a symlink")]
    NotASymlink,

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("Underlying jj-lib error: {0}")]
    JjLibBackendError(#[from] jj_lib::backend::BackendError),
}
