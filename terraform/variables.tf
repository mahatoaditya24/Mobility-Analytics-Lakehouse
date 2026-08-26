variable "aws_region" {
  description = "Target AWS deployment region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment identifier (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project identifier tag"
  type        = string
  default     = "mobility-lakehouse"
}

variable "raw_retention_days" {
  description = "Retention period for raw streaming Kafka archive in S3 Bronze"
  type        = number
  default     = 90
}
