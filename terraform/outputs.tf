output "s3_data_lake_bucket_id" {
  description = "ID of the provisioned S3 Data Lake bucket"
  value       = aws_s3_bucket.lakehouse_storage.id
}

output "s3_data_lake_bucket_arn" {
  description = "ARN of the provisioned S3 Data Lake bucket"
  value       = aws_s3_bucket.lakehouse_storage.arn
}

output "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database"
  value       = aws_glue_catalog_database.mobility_catalog.name
}
