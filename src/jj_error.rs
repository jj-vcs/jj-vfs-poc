use thiserror::Error;

#[derive(Error, Debug)]
pub enum JjError {
    #[error("Invalid path")]
    InvalidPath,

    #[error("Not found")]
    NotFound,

    #[error("Expected a directory, but found a file")]
    NotADirectory,

    #[error("Expected a file, but found a directory")]
    NotAFile,

    #[error("Underlying jj-lib error: {0}")]
    JjLibBackendError(#[from] jj_lib::backend::BackendError),

    #[error("{0}")]
    Other(String),
}
