## s3sync end-to-end tests

### Warning: These tests will create and delete AWS resources, which will result in costs on your AWS account.

These tests are designed to be run against a real AWS account.   
If any of the tests fail, it may leave items in your AWS account, such as S3 buckets.

### Running the tests against AWS
Before running the tests, you need to set up your AWS credentials.   
You can set up a profile with the AWS CLI using the following command:
```bash
aws configure --profile s3sync-e2e-test
```

To run all the tests, use the following command:
```bash
cargo test --all-features
```

### Notes
This test create and delete same S3 buckets, sometimes the tests will fail due to eventual consistency in AWS.(nevertheless, the tests will pass in the next run)