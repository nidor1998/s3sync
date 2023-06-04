pub type PipelineCancellationToken = tokio_util::sync::CancellationToken;

pub fn create_pipeline_cancellation_token() -> PipelineCancellationToken {
    tokio_util::sync::CancellationToken::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_cancellation_token() {
        create_pipeline_cancellation_token();
    }
}
