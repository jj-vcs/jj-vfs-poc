use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cc_lib::cc_backend::CommitCloudBackend;
use cc_lib::cc_op_heads_store::CommitCloudOpHeadsStore;
use cc_lib::cc_op_store::CommitCloudOpStore;
use jj_lib::backend::BackendLoadError;
use jj_lib::config::StackedConfig;
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;
use jjfsd::path_mapper_all_commits::AllCommitsPathMapper;
use jjfsd::vfs::PathMappedVfs;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <MOUNTPOINT> <WORKSPACE_ROOT>", args[0]);
        std::process::exit(1);
    }
    let mountpoint = &args[1];
    let workspace_root = std::path::Path::new(&args[2]);

    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::FSName("jjfs".to_string()),
    ];

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let _guard = rt.enter();

    let settings = UserSettings::from_config(StackedConfig::with_defaults())
        .expect("Failed to load user settings");

    let loaded_workspace = load_commit_cloud_workspace(&settings, workspace_root);

    let repo_loader = loaded_workspace.repo_loader();
    let readonly_repo = rt
        .block_on(repo_loader.load_at_head())
        .expect("Failed to load repo at head");

    let mapper = AllCommitsPathMapper::new(readonly_repo);
    let filesystem = PathMappedVfs::new(mapper);

    println!("Mounting JjVfs at {}...", mountpoint);
    let _session = fuser::spawn_mount(
        jjfsd::fuse::JjFuse::new(Arc::new(filesystem), rt.handle().clone()),
        mountpoint,
        &config,
    )
    .expect("Failed to mount filesystem");

    rt.block_on(wait_for_shutdown());
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;

    let mut sigint = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
    let mut sighup = signal(SignalKind::hangup()).expect("failed to listen for SIGHUP");

    tokio::select! {
        _ = sigint.recv() => println!("Received SIGINT, unmounting..."),
        _ = sigterm.recv() => println!("Received SIGTERM, unmounting..."),
        _ = sighup.recv() => println!("Received SIGHUP, unmounting..."),
    }
}

// --- Helper Functions ---

fn get_config_path(store_path: &Path) -> PathBuf {
    let direct = store_path.join("config.toml");
    if direct.exists() {
        return direct;
    }
    if let Some(parent) = store_path.parent() {
        let in_store = parent.join("store").join("config.toml");
        if in_store.exists() {
            return in_store;
        }
        let in_parent = parent.join("config.toml");
        if in_parent.exists() {
            return in_parent;
        }
    }
    direct
}

fn parse_config(config_str: &str) -> Result<(String, String), std::io::Error> {
    let repo_id = config_str
        .lines()
        .find(|l| l.starts_with("repo_id ="))
        .and_then(|l| l.split('"').nth(1))
        .unwrap_or("default")
        .to_string();

    let server_url = config_str
        .lines()
        .find(|l| l.starts_with("server_url ="))
        .and_then(|l| l.split('"').nth(1))
        .unwrap_or("http://localhost:8080")
        .to_string();

    Ok((server_url, repo_id))
}

fn load_commit_cloud_workspace(settings: &UserSettings, workspace_root: &Path) -> Workspace {
    let mut store_factories = StoreFactories::default();

    store_factories.add_backend(CommitCloudBackend::name(), Box::new(|_settings, store_path| {
        let backend = CommitCloudBackend::load(store_path).map_err(BackendLoadError)?;
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    }));

    store_factories.add_op_store(CommitCloudOpStore::name(), Box::new(|_settings, store_path, _root_op_id| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendLoadError(e.into()))?;
        let (server_url, repo_id) = parse_config(&config_str)
            .map_err(|e| BackendLoadError(e.into()))?;
        let op_store = CommitCloudOpStore::new(repo_id, server_url);
        Ok(Box::new(op_store) as Box<dyn jj_lib::op_store::OpStore>)
    }));

    store_factories.add_op_heads_store(CommitCloudOpHeadsStore::name(), Box::new(|_settings, store_path| {
        let config_path = get_config_path(store_path);
        let config_str = fs::read_to_string(&config_path)
            .map_err(|e| BackendLoadError(e.into()))?;
        let (server_url, repo_id) = parse_config(&config_str)
            .map_err(|e| BackendLoadError(e.into()))?;
        let op_heads_store = CommitCloudOpHeadsStore::new(repo_id, server_url);
        Ok(Box::new(op_heads_store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
    }));

    Workspace::load(
        settings,
        workspace_root,
        &store_factories,
        &default_working_copy_factories(),
    )
    .expect("Failed to load workspace")
}
