variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "access_key" {
  description = "Static access key (fake for LocalStack)."
  type        = string
  default     = "test"
}

variable "secret_key" {
  description = "Static secret key (fake for LocalStack)."
  type        = string
  default     = "test"
}

variable "use_localstack" {
  description = "Skip AWS credential/metadata validation (true for LocalStack)."
  type        = bool
  default     = true
}

variable "s3_use_path_style" {
  description = "Use path-style S3 addressing (required by LocalStack)."
  type        = bool
  default     = true
}

variable "raw_bucket" {
  description = "Name of the S3 raw-zone bucket."
  type        = string
  default     = "logwarehouse-raw"
}

variable "curated_bucket" {
  description = "Name of the S3 curated-zone bucket."
  type        = string
  default     = "logwarehouse-curated"
}

variable "stream_name" {
  description = "Kinesis Data Stream name."
  type        = string
  default     = "log-events-stream"
}

variable "stream_shard_count" {
  description = "Number of Kinesis shards (2 for local development)."
  type        = number
  default     = 2
}

variable "firehose_name" {
  description = "Firehose delivery stream name."
  type        = string
  default     = "log-events-firehose"
}

variable "firehose_buffer_mb" {
  description = "Firehose S3 buffering size in MB."
  type        = number
  default     = 5
}

variable "firehose_buffer_seconds" {
  description = "Firehose S3 buffering interval in seconds."
  type        = number
  default     = 60
}

variable "glue_database" {
  description = "Glue Data Catalog database name."
  type        = string
  default     = "logwarehouse"
}

variable "log_group_name" {
  description = "CloudWatch log group for pipeline logs."
  type        = string
  default     = "/logwarehouse/pipeline"
}

variable "alarm_topic" {
  description = "SNS topic name for pipeline alarms."
  type        = string
  default     = "pipeline-alarms"
}
