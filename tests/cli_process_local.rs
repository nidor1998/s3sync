// Integration tests that drive the compiled `s3sync` binary against local directories only.
// These do not touch AWS and run as part of the normal `cargo test` run (they are not gated
// behind the `e2e_test` cfg).

use std::fs;
use std::process::Command;

fn s3sync_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3sync"));
    // Local-to-local runs need no AWS configuration; clear it for isolation.
    command.env_remove("AWS_PROFILE");
    command.env_remove("AWS_ACCESS_KEY_ID");
    command.env_remove("AWS_SECRET_ACCESS_KEY");
    command.env_remove("AWS_SESSION_TOKEN");
    command.env_remove("RUST_LOG");
    command
}

#[test]
fn auto_complete_shell_outputs_completion() {
    let output = s3sync_command()
        .args(["--auto-complete-shell", "bash"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("s3sync"));
}

#[test]
fn invalid_args_exit_with_error() {
    // --enable-versioning requires both storages to be S3; local paths make it invalid.
    let temp_dir = tempfile::tempdir().unwrap();
    let source = temp_dir.path().join("source");
    let target = temp_dir.path().join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();

    let output = s3sync_command()
        .arg("--allow-both-local-storage")
        .arg("--enable-versioning")
        .arg(source.to_str().unwrap())
        .arg(target.to_str().unwrap())
        .output()
        .unwrap();

    assert!(!output.status.success());
}

#[test]
fn local_to_local_sync_succeeds_with_tracing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let source = temp_dir.path().join("source");
    let target = temp_dir.path().join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();
    fs::write(source.join("data.dat"), b"hello world").unwrap();

    let output = s3sync_command()
        .arg("-v")
        .arg("--disable-color-tracing")
        .arg("--allow-both-local-storage")
        .arg(source.to_str().unwrap())
        .arg(target.to_str().unwrap())
        .output()
        .unwrap();

    assert!(output.status.success());
    let copied = fs::read(target.join("data.dat")).unwrap();
    assert_eq!(copied, b"hello world");
}

#[test]
fn local_to_local_sync_stderr_tracing_succeeds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let source = temp_dir.path().join("source");
    let target = temp_dir.path().join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();
    fs::write(source.join("data.dat"), b"data12").unwrap();

    let output = s3sync_command()
        .arg("-v")
        .arg("--aws-sdk-tracing")
        .arg("--allow-both-local-storage")
        .arg(source.to_str().unwrap())
        .arg(target.to_str().unwrap())
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(target.join("data.dat").exists());
}

#[test]
#[cfg(target_family = "unix")]
fn local_to_local_sync_warning_exit_code() {
    use std::os::unix::fs::PermissionsExt;

    assert!(
        !nix::unistd::geteuid().is_root(),
        "tests must not run as root"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let source = temp_dir.path().join("source");
    let target = temp_dir.path().join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&target).unwrap();

    // A readable file is synced successfully, while an unreadable subdirectory makes the
    // directory listing emit a SyncWarning, so s3sync exits with the warning exit code (3).
    fs::write(source.join("data.dat"), b"hello world").unwrap();
    let locked_subdir = source.join("locked");
    fs::create_dir(&locked_subdir).unwrap();
    fs::set_permissions(&locked_subdir, fs::Permissions::from_mode(0o000)).unwrap();

    let output = s3sync_command()
        .arg("--allow-both-local-storage")
        .arg(source.to_str().unwrap())
        .arg(target.to_str().unwrap())
        .output()
        .unwrap();

    // Restore permissions so the temp dir can be cleaned up.
    fs::set_permissions(&locked_subdir, fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(output.status.code(), Some(3));
}
