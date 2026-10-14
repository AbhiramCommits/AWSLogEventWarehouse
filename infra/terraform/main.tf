terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30"
    }
  }
}

provider "aws" {
  region                      = var.region
  access_key                  = var.access_key
  secret_key                  = var.secret_key
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
  s3_use_path_style           = var.s3_use_path_style

  default_tags {
    tags = {
      project = "aws-log-event-warehouse"
      managed = "terraform"
    }
  }
}

resource "aws_s3_bucket" "raw" {
  bucket        = var.raw_bucket
  force_destroy = true
}

resource "aws_s3_bucket" "curated" {
  bucket        = var.curated_bucket
  force_destroy = true
}

resource "aws_kinesis_stream" "events" {
  name        = var.stream_name
  shard_count = var.stream_shard_count

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  retention_period = 24
}

resource "aws_iam_role" "firehose" {
  name = "${var.firehose_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose_s3" {
  name = "${var.firehose_name}-s3"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject",
        "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
        Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords",
        "kinesis:ListShards"]
        Resource = [aws_kinesis_stream.events.arn]
      }
    ]
  })
}

# The Firehose and Glue control planes are LocalStack "pro" features. Locally
# they are provisioned by boto3 (scripts/bootstrap_local_aws.py) against the
# moto sidecar, so the Terraform resources are gated behind `use_localstack`.
# Against real AWS (use_localstack = false, remove localstack_override.tf) they
# are created here exactly as written.
resource "aws_kinesis_firehose_delivery_stream" "raw" {
  count       = var.use_localstack ? 0 : 1
  name        = var.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.raw.arn
    buffering_size      = var.firehose_buffer_mb
    buffering_interval  = var.firehose_buffer_seconds
    compression_format  = "GZIP"
    prefix              = "raw/events/source=app_log/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "raw/errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"
  }
}

resource "aws_glue_catalog_database" "logwarehouse" {
  count = var.use_localstack ? 0 : 1
  name  = var.glue_database
}

resource "aws_cloudwatch_log_group" "pipeline" {
  name              = var.log_group_name
  retention_in_days = 7
}

resource "aws_sns_topic" "alarms" {
  name = var.alarm_topic
}
