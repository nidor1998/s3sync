# CLAUDE.md

## Git Workflow

- Always ask a person to review the code before committing it.
- Always verify that there are no errors using `cargo fmt` before committing to Git.
- Always verify that there are no errors using `cargo clippy --all-features` before committing to Git.

## Testing

- Never run e2e tests (`RUSTFLAGS="--cfg e2e_test" cargo test`). They hit real AWS and are for the user to run. Use `cargo check` / `cargo clippy` under the same `RUSTFLAGS` when you need to verify e2e code compiles.
