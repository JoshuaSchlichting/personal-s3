# Personal-S3

A personal S3 project using Terraform and a frontend served by Go.

`personal-s3` is a simple tool to sync a local directory to an S3 bucket. It is intended to be used as a personal cloud.

# Usage

## Terraform
```tf
cd terraform
terraform init
terraform apply -var="s3_users=[\"arn:aws:iam::123456789012:user/my-iam-user\"]"
```

## CLI
```
Usage of personal-s3:
  -bucket string
        name of the S3 bucket
  -cache
        use cache to skip files that have already been uploaded in lieu of HEAD-OBJECT calls to AWS
  -dir string
        directory to sync to S3 bucket
```