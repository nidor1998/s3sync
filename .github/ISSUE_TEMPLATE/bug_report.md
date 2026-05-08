---
name: Bug report
about: Report a bug in s3sync
title: "[Bug] "
labels: bug
---

## Contributing

- Bug reports are welcome, but responses are not guaranteed.
- Since this project is considered functionally complete, I will not accept any feature requests.
- If you find this project useful, feel free to fork and modify it as you wish.

🔒 I consider this project "complete" and will maintain it only minimally going forward.
However, I intend to keep the AWS SDK for Rust and other dependencies up to date monthly.

## Before opening an issue

Please read the [Scope and Non-Goals](https://github.com/nidor1998/s3sync/blob/main/README.md#scope) section of the README first. **Issues asking about behavior already documented in the README — including anything listed under Non-Goals — will be closed without further discussion.** This is not a rejection of your input; it is the project's documented scope.

This template is for bug reports only. **Feature requests filed here will be closed unconditionally.**

## Issue lifecycle

Issues with no activity for 30 days are labeled `stale` and closed 7 days later unless a new comment is added. Items labeled `pinned` or `security` are exempt. Closed issues can always be reopened.

## Prerequisites

- [ ] I have read the README (including Scope and Non-Goals) and confirmed this issue is not already documented.
- [ ] I have searched existing issues (open and closed) and this is not a duplicate.
- [ ] This is a reproducible bug report — not a feature request, question, or request for usage help.
- [ ] I have reproduced this on the latest release of s3sync.

## Describe the bug

A clear and concise description of what the bug is.

## To Reproduce

Please include as much of the information about the failed command as possible.

## Expected behavior

A clear and concise description of what you expected to happen.

## Environment

When verifying the reproducibility of a bug report, any report lacking information on the OS, s3sync version, and Storage will be closed without exception.

- OS: [e.g. macOS 14.5, Ubuntu 24.04, Windows 11]
- s3sync version: [output of `s3sync --version`] — **only the latest release is supported**. Issues filed against any other version will be closed automatically; please reproduce on the latest version before filing.
- Storage: [e.g. Amazon S3, MinIO, Cloudflare R2, Ceph RGW] — only Amazon S3 is supported. Issues against S3-compatible services generally cannot be supported and may be closed.

## Additional context

Add any other context about the problem here.
