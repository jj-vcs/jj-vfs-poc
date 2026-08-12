use fuser::Errno;
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

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("Underlying jj-lib error: {0}")]
    JjLibBackendError(#[from] jj_lib::backend::BackendError),
}

impl From<JjError> for Errno {
    fn from(value: JjError) -> Self {
        match value {
            JjError::NotFound => Errno::ENOENT,
            JjError::NotADirectory => Errno::ENOTDIR,
            JjError::NotAFile => Errno::EISDIR,
            _ => Errno::EIO,
        }
    }
}
