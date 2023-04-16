

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "private_cloud" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_policy" "user_access" {
  bucket = aws_s3_bucket.private_cloud.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*"
        ]
        Effect = "Deny"
        NotPrincipal = {
          AWS = var.s3_users
        }
        Resource = [
          "${aws_s3_bucket.private_cloud.arn}/*",
          "${aws_s3_bucket.private_cloud.arn}"
        ]
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = var.s3_users
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          #   "s3:DeleteObject",
          "s3:GetObjectAcl",
          "s3:PutObjectAcl",
          "s3:*MultipartUpload*"
        ]
        Resource = [
          "${aws_s3_bucket.private_cloud.arn}/*",
          "${aws_s3_bucket.private_cloud.arn}"
        ]
      }
    ]
  })
}

resource "aws_s3_bucket_public_access_block" "example" {
  bucket = aws_s3_bucket.private_cloud.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bucket_arn" {
  value = aws_s3_bucket.private_cloud.arn
}
