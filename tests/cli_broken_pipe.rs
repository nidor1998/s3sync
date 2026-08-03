//! Process-level broken-pipe regression tests. No AWS access is needed:
//! completions, help, and version output never leave the process.
//!
//! Each test hands the child a stdout whose read end is already closed, so
//! every stdout write in the child fails with `BrokenPipe` from the first
//! byte — exactly the `s3sync --auto-complete-shell bash | head 1` case,
//! where `head` fails to open the file `1` and exits without reading its
//! input (and the worst case of any consumer that stops reading early).
//! The binary must exit through its normal success path: never panic
//! ("failed to write completion file: ... Broken pipe") and never die of
//! SIGPIPE (which would surface here as `status.code() == None`).

use std::process::{Command, Stdio};

fn s3sync_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_s3sync"));
    // None of these paths need AWS configuration; clear it for isolation.
    command.env_remove("AWS_PROFILE");
    command.env_remove("AWS_ACCESS_KEY_ID");
    command.env_remove("AWS_SECRET_ACCESS_KEY");
    command.env_remove("AWS_SESSION_TOKEN");
    command.env_remove("RUST_LOG");
    command
}

/// Run `cmd` with a pre-closed stdout pipe and return (exit_code, stderr).
fn run_with_closed_stdout(cmd: &mut Command) -> (Option<i32>, String) {
    let (reader, writer) = std::io::pipe().expect("failed to create pipe");
    // Close the read end before the child even starts: with no readers left,
    // every stdout write in the child fails with EPIPE immediately.
    drop(reader);

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(writer))
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3sync binary");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn assert_exits_zero_without_panic(code: Option<i32>, stderr: &str, what: &str) {
    assert!(
        !stderr.contains("panicked"),
        "{what} must not panic on a closed stdout pipe; stderr: {stderr}"
    );
    assert_eq!(
        code,
        Some(0),
        "{what} must exit 0 on a closed stdout pipe (None = killed by \
         SIGPIPE); stderr: {stderr}"
    );
}

/// Completion scripts are rendered to a buffer and written pipe-safely —
/// clap_complete itself would panic ("failed to write completion file") if
/// its generator wrote straight into a closed stdout.
#[test]
fn completion_script_with_closed_stdout_exits_zero() {
    for shell in ["bash", "zsh", "fish"] {
        let (code, stderr) =
            run_with_closed_stdout(s3sync_command().args(["--auto-complete-shell", shell]));
        assert_exits_zero_without_panic(code, &stderr, &format!("--auto-complete-shell {shell}"));
    }
}

/// --help and --version are printed by clap, whose `Error::exit` swallows
/// write errors ("Swallow broken pipe errors" in clap itself). Pinned here
/// so a clap regression would be caught.
#[test]
fn help_and_version_with_closed_stdout_exit_zero() {
    let (code, stderr) = run_with_closed_stdout(s3sync_command().arg("--help"));
    assert_exits_zero_without_panic(code, &stderr, "--help");

    let (code, stderr) = run_with_closed_stdout(s3sync_command().arg("--version"));
    assert_exits_zero_without_panic(code, &stderr, "--version");
}
