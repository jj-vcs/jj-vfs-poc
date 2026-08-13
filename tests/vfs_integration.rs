mod test_helpers;

use std::path::Path;
use std::sync::Arc;

use jj_lib::object_id::ObjectId as _;
use jjfsd::jj_filesystem::JjFilesystem;
use jjfsd::jj_filesystem::JjVfsState;
use jjfsd::path_mapper_all_commits::AllCommitsPathMapper;
use jjfsd::virtual_file::FileType;
use pollster::FutureExt as _;

#[test]
fn test_vfs_read_real_repo_files() {
    // 1. Set up a real test jj repository with commits and files
    let (_temp_dir, repo, commit_id) = test_helpers::setup_test_repo();

    // 2. Initialize the mapper and JjVfsState
    let mapper = AllCommitsPathMapper::new(repo);
    let fs = JjVfsState::new(mapper);

    // 3. Look up the commit directory (the commit hex is the first level of
    //    components in the path mapper)
    let commit_hex = commit_id.hex();

    // In JjVfsState, looking up a child of root (parent = 1) is done by name:
    let commit_dir_attr = fs
        .getattr(Path::new(&commit_hex))
        .block_on()
        .expect("Failed to lookup commit directory");

    // Verify it is indeed a directory (kind = Directory)
    assert!(matches!(commit_dir_attr.1, FileType::Directory));

    // 4. Look up "file1.txt" inside the commit directory
    let file1_attr = fs
        .getattr(Path::new(&format!("{}/file1.txt", commit_hex)))
        .block_on()
        .expect("Failed to lookup file1.txt");
    assert!(matches!(file1_attr.1, FileType::File));
    assert_eq!(file1_attr.0, 15); // "hello content 1" is 15 bytes

    // 5. Read the content of "file1.txt"
    let content = fs
        .read(Path::new(&format!("{}/file1.txt", commit_hex)), 0, 15)
        .block_on()
        .expect("Failed to read file1.txt");
    assert_eq!(&*content, b"hello content 1");

    // 6. Look up "dir" inside the commit directory
    let dir_attr = fs
        .getattr(Path::new(&format!("{}/dir", commit_hex)))
        .block_on()
        .expect("Failed to lookup dir");
    assert!(matches!(dir_attr.1, FileType::Directory));

    // 7. Look up "file2.txt" inside "dir"
    let file2_attr = fs
        .getattr(Path::new(&format!("{}/dir/file2.txt", commit_hex)))
        .block_on()
        .expect("Failed to lookup file2.txt");
    assert!(matches!(file2_attr.1, FileType::File));
    assert_eq!(file2_attr.0, 15); // "hello content 2" is 15 bytes

    // 8. Read the content of "file2.txt"
    let content2 = fs
        .read(Path::new(&format!("{}/dir/file2.txt", commit_hex)), 0, 15)
        .block_on()
        .expect("Failed to read file2.txt");
    assert_eq!(&*content2, b"hello content 2");
}

#[test]
fn test_vfs_mount() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let _guard = rt.enter();

    // 1. Set up a real test jj repository with commits and files
    let (_temp_dir, repo, commit_id) = test_helpers::setup_test_repo();

    // 2. Initialize the mapper and JjVfsState
    let mapper = AllCommitsPathMapper::new(repo);
    let fs = JjVfsState::new(mapper);

    // 3. Create a temporary mountpoint directory
    let mount_dir = tempfile::tempdir().expect("Failed to create tempdir");
    let mountpoint = mount_dir.path().to_path_buf();

    // 4. Mount the filesystem
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("jjfs_test".to_string()),
    ];

    let session = fuser::spawn_mount(
        jjfsd::fuse::JjFuse::new(Arc::new(fs), rt.handle().clone()),
        &mountpoint,
        &config,
    )
    .expect("Failed to mount filesystem");

    // 5. Verify the files are visible and readable via standard fs
    let commit_hex = commit_id.hex();
    let commit_dir = mountpoint.join(&commit_hex);

    // Wait a bit or retry to make sure FUSE has finished mounting and is ready.
    let mut success = false;
    for _ in 0..20 {
        if commit_dir.exists() {
            success = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(success, "Mount point did not become ready in time");

    // Check file1.txt
    let file1_path = commit_dir.join("file1.txt");
    assert!(file1_path.exists());
    let file1_content = std::fs::read_to_string(&file1_path).expect("Failed to read file1.txt");
    assert_eq!(file1_content, "hello content 1");

    // Check dir/file2.txt
    let file2_path = commit_dir.join("dir").join("file2.txt");
    assert!(file2_path.exists());
    let file2_content = std::fs::read_to_string(&file2_path).expect("Failed to read file2.txt");
    assert_eq!(file2_content, "hello content 2");

    // Explicitly unmount/drop session
    drop(session);
}
