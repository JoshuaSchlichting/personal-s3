variable "bucket_name" {
  type = string
}

variable "s3_users" {
  type    = list(string)
  default = []
}