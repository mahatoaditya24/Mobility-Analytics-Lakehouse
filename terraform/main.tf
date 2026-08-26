# =============================================================================
# Terraform Infrastructure Blueprint: Smart City Mobility Real-Time Lakehouse
# Provisions S3 Data Lake, EMR Spark Compute, and Glue Data Catalog
# =============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# =============================================================================
# 1. S3 DATA LAKE STORAGE BUCKETS (Bronze, Silver, Gold, Quarantine DLQ)
# =============================================================================

resource "aws_s3_bucket" "lakehouse_storage" {
  bucket        = "${var.project_name}-${var.environment}-storage"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "lakehouse_versioning" {
  bucket = aws_s3_bucket.lakehouse_storage.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse_encryption" {
  bucket = aws_s3_bucket.lakehouse_storage.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse_lifecycle" {
  bucket = aws_s3_bucket.lakehouse_storage.id

  rule {
    id     = "archive-bronze-telemetry"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = var.raw_retention_days
      storage_class = "GLACIER"
    }
  }
}

# =============================================================================
# 2. AWS GLUE METASTORE / CATALOG DATABASE
# =============================================================================

resource "aws_glue_catalog_database" "mobility_catalog" {
  name        = "mobility_lakehouse_${var.environment}"
  description = "Central schema registry and Hive Metastore for Smart City Telemetry Lakehouse"
}

# =============================================================================
# 3. IAM ROLES & POLICIES FOR SPARK CLUSTER EXECUTION
# =============================================================================

resource "aws_iam_role" "emr_execution_role" {
  name = "${var.project_name}-emr-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_s3_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
