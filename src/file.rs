use ustr::Ustr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
}

/// Represents a file with a given filename and file type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub name: Ustr,
    pub file_type: FileType,
}

impl File {
    pub fn new(name: &str, file_type: FileType) -> Self {
        Self {
            name: Ustr::from(name),
            file_type,
        }
    }
}
