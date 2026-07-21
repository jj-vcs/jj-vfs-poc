use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;

use ustr::Ustr;

use crate::file::FileType;

const ROOT_INODE: u64 = 1;

struct Entry {
    parent: u64,
    name: Ustr,
    inode_type: InodeType,
}

impl Entry {
    fn new_directory(parent: u64, name: Ustr) -> Self {
        Self {
            parent,
            name,
            inode_type: InodeType::Directory {
                children: RwLock::new(HashMap::new()),
            },
        }
    }
}

#[derive(Debug)]
enum InodeType {
    File,
    Directory {
        children: RwLock<HashMap<Ustr, u64>>,
    },
}

pub struct Inodes {
    inodes: RwLock<HashMap<u64, Entry>>,
    next_inode: AtomicU64,
}

impl Inodes {
    pub fn new() -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INODE, Entry::new_directory(ROOT_INODE, Ustr::from("")));

        Self {
            inodes: RwLock::new(inodes),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }

    pub fn get_path(&self, mut ino: u64) -> PathBuf {
        let inodes = self.inodes.read().unwrap();
        let mut components = Vec::with_capacity(8);
        while ino != ROOT_INODE {
            let entry = inodes
                .get(&ino)
                .expect(&format!("File or directory with INO {} not found!", ino));
            components.push(entry.name);
            ino = entry.parent;
        }
        let total_len: usize = components.iter().map(|c| c.len()).sum::<usize>() + components.len();
        let mut path = PathBuf::with_capacity(total_len + 1);
        path.push("/");
        path.extend(components.iter().rev());
        path
    }

    pub fn get_child(&self, parent: u64, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let name_ustr = Ustr::from(name);
        let inodes = self.inodes.read().unwrap();
        let parent_entry = inodes
            .get(&parent)
            .ok_or_else(|| format!("Parent INO {} not found", parent))?;

        match &parent_entry.inode_type {
            InodeType::Directory { children } => children
                .read()
                .unwrap()
                .get(&name_ustr)
                .copied()
                .ok_or_else(|| {
                    format!("Child {} not found under parent INO {}", name, parent).into()
                }),
            _ => Err(format!("Parent INO {} is not a directory", parent).into()),
        }
    }

    // Requires an entire write-lock, even if no element is added.
    pub fn get_or_create_child(
        &self,
        parent: u64,
        name: &str,
        file_type: FileType,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let name_ustr = Ustr::from(name);
        let mut inodes = self.inodes.write().unwrap();

        let parent_entry = inodes
            .get(&parent)
            .ok_or_else(|| format!("Parent INO {} not found", parent))?;

        let children = match &parent_entry.inode_type {
            InodeType::Directory { children } => children,
            _ => return Err(format!("Parent INO {} is not a directory", parent).into()),
        };

        if let Some(child_ino) = children.read().unwrap().get(&name_ustr).copied() {
            let child_entry = inodes
                .get(&child_ino)
                .ok_or_else(|| format!("Child INO {} not found", child_ino))?;

            let existing_file_type = match &child_entry.inode_type {
                InodeType::File => FileType::File,
                InodeType::Directory { .. } => FileType::Directory,
            };

            if file_type != existing_file_type {
                // TODO: implement proper logging
                eprintln!(
                    "Warning: Type mismatch for existing entry '{}' under parent INO {}: expected \
                     {:?}, found {:?}",
                    name, parent, file_type, existing_file_type
                )
            }
            return Ok(child_ino);
        }

        let new_ino = self
            .next_inode
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        children.write().unwrap().insert(name_ustr, new_ino);

        inodes.insert(
            new_ino,
            Entry {
                parent,
                name: name_ustr,
                inode_type: match file_type {
                    FileType::File => InodeType::File,
                    FileType::Directory => InodeType::Directory {
                        children: RwLock::new(HashMap::new()),
                    },
                },
            },
        );

        Ok(new_ino)
    }

    pub fn get_parent(&self, ino: u64) -> Option<u64> {
        let inodes = self.inodes.read().unwrap();
        inodes.get(&ino).map(|entry| entry.parent)
    }

    pub fn get_file_type(&self, ino: u64) -> Option<FileType> {
        let inodes = self.inodes.read().unwrap();
        let entry = inodes.get(&ino)?;
        match &entry.inode_type {
            InodeType::File => Some(FileType::File),
            InodeType::Directory { .. } => Some(FileType::Directory),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_path() {
        let inodes = Inodes::new();

        // Root path
        assert_eq!(inodes.get_path(ROOT_INODE), PathBuf::from("/"));

        // Populate some inodes
        {
            let mut map = inodes.inodes.write().unwrap();
            map.insert(2, Entry::new_directory(ROOT_INODE, Ustr::from("foo")));
            map.insert(
                3,
                Entry {
                    parent: 2,
                    name: Ustr::from("bar.txt"),
                    inode_type: InodeType::File,
                },
            );
        }

        assert_eq!(inodes.get_path(2), PathBuf::from("/foo"));
        assert_eq!(inodes.get_path(3), PathBuf::from("/foo/bar.txt"));
    }

    #[test]
    fn test_get_child() {
        let inodes = Inodes::new();

        // Create "foo" using get_or_create_child
        let foo_ino = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::Directory)
            .unwrap();
        assert_eq!(foo_ino, 2);

        assert_eq!(inodes.get_child(ROOT_INODE, "foo").unwrap(), 2);

        let err = inodes.get_child(999, "bar");
        assert!(err.is_err());
    }

    #[test]
    fn test_get_or_create_child() {
        let inodes = Inodes::new();

        // Create "foo" under root as directory
        let foo_ino = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::Directory)
            .unwrap();
        assert_eq!(foo_ino, 2);
        assert_eq!(inodes.get_path(foo_ino), PathBuf::from("/foo"));

        // Get "foo" again, should return existing INO 2
        let foo_ino_again = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::Directory)
            .unwrap();
        assert_eq!(foo_ino_again, 2);

        // Create "bar.txt" under "foo" as file
        let bar_ino = inodes
            .get_or_create_child(foo_ino, "bar.txt", FileType::File)
            .unwrap();
        assert_eq!(bar_ino, 3);
        assert_eq!(inodes.get_path(bar_ino), PathBuf::from("/foo/bar.txt"));

        // Error cases
        // 1. Non-existent parent
        let err = inodes.get_or_create_child(999, "bar.txt", FileType::File);
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().to_string(), "Parent INO 999 not found");

        // 2. Parent is not a directory
        let err = inodes.get_or_create_child(bar_ino, "baz.txt", FileType::File);
        assert!(err.is_err());
        assert_eq!(
            err.unwrap_err().to_string(),
            format!("Parent INO {} is not a directory", bar_ino)
        );
    }

    #[test]
    fn test_get_or_create_child_type_mismatch() {
        let inodes = Inodes::new();

        // Create "foo" as a Directory
        let foo_ino = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::Directory)
            .unwrap();

        // Get or create "foo" again, but ask for a File. This should trigger the
        // warning but succeed returning foo_ino.
        let foo_ino_file = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::File)
            .unwrap();
        assert_eq!(foo_ino_file, foo_ino);
    }

    #[test]
    fn test_get_parent_and_file_type() {
        let inodes = Inodes::new();
        let foo_ino = inodes
            .get_or_create_child(ROOT_INODE, "foo", FileType::Directory)
            .unwrap();
        let bar_ino = inodes
            .get_or_create_child(foo_ino, "bar.txt", FileType::File)
            .unwrap();

        assert_eq!(inodes.get_parent(foo_ino), Some(ROOT_INODE));
        assert_eq!(inodes.get_parent(bar_ino), Some(foo_ino));
        assert_eq!(inodes.get_parent(999), None);

        assert_eq!(inodes.get_file_type(ROOT_INODE), Some(FileType::Directory));
        assert_eq!(inodes.get_file_type(foo_ino), Some(FileType::Directory));
        assert_eq!(inodes.get_file_type(bar_ino), Some(FileType::File));
        assert_eq!(inodes.get_file_type(999), None);
    }
}
