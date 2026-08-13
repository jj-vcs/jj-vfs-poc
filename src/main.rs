use std::sync::Arc;

use jj_lib::config::StackedConfig;
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::default_working_copy_factories;
use jjfsd::path_mapper_all_commits::AllCommitsPathMapper;
use jjfsd::vfs::PathMappedVFS;

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

    let loaded_workspace = Workspace::load(
        &settings,
        workspace_root,
        &StoreFactories::default(),
        &default_working_copy_factories(),
    )
    .expect("Failed to load workspace");

    let repo_loader = loaded_workspace.repo_loader();
    let readonly_repo = rt
        .block_on(repo_loader.load_at_head())
        .expect("Failed to load repo at head");

    let mapper = AllCommitsPathMapper::new(readonly_repo);
    let filesystem = PathMappedVFS::new(mapper);

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
