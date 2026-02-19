# Terraform Configuration for Fleet Analytics Infrastructure
# AWS Provider and Resources

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "fleet-terraform-state"
    key    = "fleet-analytics/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "FleetAnalytics"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# Variables
variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  default     = "production"
}

variable "project_name" {
  description = "Project name"
  default     = "fleet-analytics"
}

# ============================================
# KINESIS STREAMS
# ============================================

resource "aws_kinesis_stream" "gps_events" {
  name             = "${var.project_name}-gps-events"
  shard_count      = 4
  retention_period = 24
  
  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "IncomingRecords",
    "OutgoingRecords"
  ]
  
  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_kinesis_stream" "delivery_events" {
  name             = "${var.project_name}-delivery-events"
  shard_count      = 2
  retention_period = 24
  
  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes"
  ]
}

# ============================================
# S3 DATA LAKE
# ============================================

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  rule {
    id     = "raw-data-lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "raw/"
    }
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 365
    }
  }
  
  rule {
    id     = "processed-data-lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "processed/"
    }
    
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }
}

# ============================================
# KINESIS FIREHOSE (S3 Delivery)
# ============================================

resource "aws_kinesis_firehose_delivery_stream" "gps_to_s3" {
  name        = "${var.project_name}-gps-firehose"
  destination = "extended_s3"
  
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.gps_events.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }
  
  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.data_lake.arn
    prefix              = "raw/gps/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "errors/gps/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_size      = 64
    buffering_interval  = 60
    compression_format  = "GZIP"
    
    data_format_conversion_configuration {
      enabled = true
      
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      
      output_format_configuration {
        serializer {
          parquet_ser_de {
            compression = "SNAPPY"
          }
        }
      }
      
      schema_configuration {
        database_name = aws_glue_catalog_database.fleet_db.name
        table_name    = aws_glue_catalog_table.gps_events.name
        role_arn      = aws_iam_role.firehose_role.arn
      }
    }
  }
}

# ============================================
# DYNAMODB (Real-time Metrics)
# ============================================

resource "aws_dynamodb_table" "realtime_metrics" {
  name           = "${var.project_name}-realtime-metrics"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "pk"
  range_key      = "sk"
  
  attribute {
    name = "pk"
    type = "S"
  }
  
  attribute {
    name = "sk"
    type = "S"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = true
  }
}

resource "aws_dynamodb_table" "delivery_metrics" {
  name           = "${var.project_name}-delivery-metrics"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "pk"
  range_key      = "sk"
  
  attribute {
    name = "pk"
    type = "S"
  }
  
  attribute {
    name = "sk"
    type = "S"
  }
  
  global_secondary_index {
    name            = "date-index"
    hash_key        = "sk"
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# ============================================
# GLUE CATALOG
# ============================================

resource "aws_glue_catalog_database" "fleet_db" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "gps_events" {
  name          = "gps_events"
  database_name = aws_glue_catalog_database.fleet_db.name
  
  table_type = "EXTERNAL_TABLE"
  
  parameters = {
    "classification" = "parquet"
  }
  
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/raw/gps/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    
    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "vehicle_id"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
    columns {
      name = "latitude"
      type = "double"
    }
    columns {
      name = "longitude"
      type = "double"
    }
    columns {
      name = "speed_mph"
      type = "double"
    }
    columns {
      name = "heading"
      type = "int"
    }
    columns {
      name = "fuel_level"
      type = "double"
    }
    columns {
      name = "engine_status"
      type = "string"
    }
  }
  
  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
}

# ============================================
# LAMBDA FUNCTIONS
# ============================================

resource "aws_lambda_function" "gps_processor" {
  function_name = "${var.project_name}-gps-processor"
  role          = aws_iam_role.lambda_role.arn
  handler       = "gps_processor.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  memory_size   = 256
  
  filename         = "lambda_functions.zip"
  source_code_hash = filebase64sha256("lambda_functions.zip")
  
  environment {
    variables = {
      DYNAMODB_TABLE = aws_dynamodb_table.realtime_metrics.name
      FIREHOSE_STREAM = aws_kinesis_firehose_delivery_stream.gps_to_s3.name
    }
  }
}

resource "aws_lambda_event_source_mapping" "gps_kinesis" {
  event_source_arn  = aws_kinesis_stream.gps_events.arn
  function_name     = aws_lambda_function.gps_processor.arn
  starting_position = "LATEST"
  batch_size        = 100
  
  maximum_batching_window_in_seconds = 5
}

# ============================================
# IAM ROLES
# ============================================

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams"
        ]
        Resource = [
          aws_kinesis_stream.gps_events.arn,
          aws_kinesis_stream.delivery_events.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem"
        ]
        Resource = [
          aws_dynamodb_table.realtime_metrics.arn,
          aws_dynamodb_table.delivery_metrics.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "firehose:PutRecord",
          "firehose:PutRecordBatch"
        ]
        Resource = aws_kinesis_firehose_delivery_stream.gps_to_s3.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role" "firehose_role" {
  name = "${var.project_name}-firehose-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "${var.project_name}-firehose-policy"
  role = aws_iam_role.firehose_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream"
        ]
        Resource = aws_kinesis_stream.gps_events.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTableVersion",
          "glue:GetTableVersions"
        ]
        Resource = "*"
      }
    ]
  })
}

# ============================================
# OUTPUTS
# ============================================

output "kinesis_gps_stream_arn" {
  value = aws_kinesis_stream.gps_events.arn
}

output "kinesis_delivery_stream_arn" {
  value = aws_kinesis_stream.delivery_events.arn
}

output "s3_data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "dynamodb_realtime_table" {
  value = aws_dynamodb_table.realtime_metrics.name
}

output "lambda_gps_processor_arn" {
  value = aws_lambda_function.gps_processor.arn
}
