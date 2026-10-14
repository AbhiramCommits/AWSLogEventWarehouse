output "raw_bucket" {
  description = "S3 raw-zone bucket name."
  value       = aws_s3_bucket.raw.bucket
}

output "curated_bucket" {
  description = "S3 curated-zone bucket name."
  value       = aws_s3_bucket.curated.bucket
}

output "stream_name" {
  description = "Kinesis Data Stream name."
  value       = aws_kinesis_stream.events.name
}

output "stream_arn" {
  description = "Kinesis Data Stream ARN."
  value       = aws_kinesis_stream.events.arn
}

output "firehose_name" {
  description = "Firehose delivery stream name."
  value       = var.firehose_name
}

output "glue_database" {
  description = "Glue Data Catalog database name."
  value       = var.glue_database
}

output "log_group_name" {
  description = "CloudWatch log group name."
  value       = aws_cloudwatch_log_group.pipeline.name
}

output "alarm_topic_arn" {
  description = "SNS topic ARN used by pipeline alarms."
  value       = aws_sns_topic.alarms.arn
}
