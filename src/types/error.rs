use anyhow::Error;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum S3syncError {
    #[error("a object references a parent directory.")]
    DirectoryTraversalError,
    #[error("cancelled")]
    Cancelled,
    #[error("an error occurred while downloading an object")]
    DownloadForceRetryableError,
}

pub fn is_cancelled_error(e: &Error) -> bool {
    if let Some(err) = e.downcast_ref::<S3syncError>() {
        return *err == S3syncError::Cancelled;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    #[test]
    fn is_cancelled_error_test() {
        assert!(is_cancelled_error(&anyhow!(S3syncError::Cancelled)));
        assert!(!is_cancelled_error(&anyhow!(
            S3syncError::DirectoryTraversalError
        )));
    }
}
