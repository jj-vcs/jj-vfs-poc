use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use fuser::INodeNo;
use futures::lock::Mutex;
use ustr::Ustr;

use crate::jj_error::JjError;

const ROOT_INODE: INodeNo = INodeNo(1);

struct Entry {
    parent: INodeNo,
    name: Ustr,
    children: HashMap<Ustr, INodeNo>,
}

pub struct InodeMap {
    inodes: Mutex<HashMap<INodeNo, Entry>>,
    next_inode: AtomicU64,
}

impl InodeMap {
    pub fn new() -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(
            ROOT_INODE,
            Entry {
                parent: ROOT_INODE,
                name: Ustr::from(""),
                children: HashMap::new(),
            },
        );

        Self {
            inodes: Mutex::new(inodes),
            next_inode: AtomicU64::new(ROOT_INODE.0 + 1),
        }
    }

    pub async fn get_path(&self, mut ino: INodeNo) -> Result<PathBuf, JjError> {
        let inodes = self.inodes.lock().await;
        let mut components = Vec::with_capacity(8);
        while ino != ROOT_INODE {
            let entry = inodes.get(&ino).ok_or(JjError::NotFound)?;
            components.push(entry.name);
            ino = entry.parent;
        }
        let total_len: usize = components.iter().map(|c| c.len()).sum::<usize>() + components.len();
        let mut path = PathBuf::with_capacity(total_len);
        path.extend(components.iter().rev());
        Ok(path)
    }

    pub async fn get_parent_ino(&self, ino: INodeNo) -> Result<INodeNo, JjError> {
        Ok(self
            .inodes
            .lock()
            .await
            .get(&ino)
            .ok_or(JjError::NotFound)?
            .parent)
    }

    pub async fn get_ino(&self, parent: INodeNo, name: &str) -> Result<INodeNo, JjError> {
        let name = Ustr::from(name);
        let mut inodes = self.inodes.lock().await;
        let children = &mut inodes.get_mut(&parent).ok_or(JjError::NotFound)?.children;
        match children.get(&name) {
            Some(&ino) => Ok(ino),
            None => {
                let ino = INodeNo(self.next_inode.fetch_add(1, Ordering::Relaxed));
                children.insert(name, ino);
                inodes.insert(
                    ino,
                    Entry {
                        parent,
                        name,
                        children: HashMap::new(),
                    },
                );
                Ok(ino)
            }
        }
    }
}

impl Default for InodeMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use pollster::FutureExt;

    use super::*;

    #[test]
    fn test_get_path() {
        let inodes = InodeMap::new();

        // Root path is empty
        assert_eq!(
            inodes.get_path(ROOT_INODE).block_on().unwrap(),
            PathBuf::from("")
        );

        // Manually populate some inodes for testing get_path
        {
            let mut map = inodes.inodes.lock().block_on();
            map.insert(
                INodeNo(2),
                Entry {
                    parent: ROOT_INODE,
                    name: Ustr::from("foo"),
                    children: HashMap::new(),
                },
            );
            map.insert(
                INodeNo(3),
                Entry {
                    parent: INodeNo(2),
                    name: Ustr::from("bar.txt"),
                    children: HashMap::new(),
                },
            );
        }

        assert_eq!(
            inodes.get_path(INodeNo(2)).block_on().unwrap(),
            PathBuf::from("foo")
        );
        assert_eq!(
            inodes.get_path(INodeNo(3)).block_on().unwrap(),
            PathBuf::from("foo/bar.txt")
        );
    }

    #[test]
    fn test_get_ino() {
        let inodes = InodeMap::new();

        // Create "foo" under root
        let foo_ino = inodes.get_ino(ROOT_INODE, "foo").block_on().unwrap();
        assert_eq!(foo_ino, INodeNo(2));
        assert_eq!(
            inodes.get_path(foo_ino).block_on().unwrap(),
            PathBuf::from("foo")
        );

        // Get "foo" again, should return existing INO 2
        let foo_ino_again = inodes.get_ino(ROOT_INODE, "foo").block_on().unwrap();
        assert_eq!(foo_ino_again, INodeNo(2));

        // Create "bar.txt" under "foo"
        let bar_ino = inodes.get_ino(foo_ino, "bar.txt").block_on().unwrap();
        assert_eq!(bar_ino, INodeNo(3));
        assert_eq!(
            inodes.get_path(bar_ino).block_on().unwrap(),
            PathBuf::from("foo/bar.txt")
        );

        // Error cases: Non-existent parent
        let err = inodes.get_ino(INodeNo(999), "bar.txt").block_on();
        assert!(matches!(err, Err(JjError::NotFound)));
    }

    #[test]
    fn test_inode_map_is_sync() {
        // This will yield a compile error if InodeMap is not Sync.
        fn assert_sync<T: Sync>() {}
        assert_sync::<InodeMap>();
    }

    #[test]
    fn test_inode_map_is_send() {
        // This will yield a compile error if InodeMap is not Send.
        fn assert_send<T: Send>() {}
        assert_send::<InodeMap>();
    }
}
