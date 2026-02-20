"""
Create Glue database and tables for Fleet Analytics.
"""

import boto3
import json

REGION = "us-east-1"
S3_BUCKET = "fleet-analytics-data-lake-054375299485"
DATABASE_NAME = "fleet_analytics"

glue = boto3.client('glue', region_name=REGION)


def create_database():
    """Create Glue database."""
    try:
        glue.create_database(
            DatabaseInput={
                'Name': DATABASE_NAME,
                'Description': 'Fleet Analytics Data Lake'
            }
        )
        print(f"Created database: {DATABASE_NAME}")
    except glue.exceptions.AlreadyExistsException:
        print(f"Database {DATABASE_NAME} already exists")


def create_gps_table():
    """Create GPS telemetry table."""
    try:
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                'Name': 'raw_gps',
                'Description': 'Raw GPS telemetry data',
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'event_id', 'Type': 'string'},
                        {'Name': 'vehicle_id', 'Type': 'string'},
                        {'Name': 'timestamp', 'Type': 'string'},
                        {'Name': 'latitude', 'Type': 'double'},
                        {'Name': 'longitude', 'Type': 'double'},
                        {'Name': 'speed_mph', 'Type': 'double'},
                        {'Name': 'heading', 'Type': 'int'},
                        {'Name': 'fuel_level', 'Type': 'double'},
                        {'Name': 'engine_status', 'Type': 'string'},
                        {'Name': 'odometer', 'Type': 'int'}
                    ],
                    'Location': f's3://{S3_BUCKET}/raw/gps/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'PartitionKeys': [
                    {'Name': 'year', 'Type': 'int'},
                    {'Name': 'month', 'Type': 'int'},
                    {'Name': 'day', 'Type': 'int'}
                ],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'parquet.compression': 'SNAPPY'
                }
            }
        )
        print("Created table: raw_gps")
    except glue.exceptions.AlreadyExistsException:
        print("Table raw_gps already exists")


def create_delivery_table():
    """Create delivery events table."""
    try:
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                'Name': 'raw_delivery',
                'Description': 'Raw delivery event data',
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'delivery_id', 'Type': 'string'},
                        {'Name': 'vehicle_id', 'Type': 'string'},
                        {'Name': 'timestamp', 'Type': 'string'},
                        {'Name': 'status', 'Type': 'string'},
                        {'Name': 'customer_id', 'Type': 'string'},
                        {'Name': 'package_count', 'Type': 'int'},
                        {'Name': 'delivery_time_minutes', 'Type': 'int'},
                        {'Name': 'distance_miles', 'Type': 'double'},
                        {'Name': 'tip_amount', 'Type': 'double'}
                    ],
                    'Location': f's3://{S3_BUCKET}/raw/delivery/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'PartitionKeys': [
                    {'Name': 'year', 'Type': 'int'},
                    {'Name': 'month', 'Type': 'int'},
                    {'Name': 'day', 'Type': 'int'}
                ],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'parquet.compression': 'SNAPPY'
                }
            }
        )
        print("Created table: raw_delivery")
    except glue.exceptions.AlreadyExistsException:
        print("Table raw_delivery already exists")


def add_partitions():
    """Add partitions for the uploaded data."""
    dates = [
        (2024, 1, 13),
        (2024, 1, 14),
        (2024, 1, 15)
    ]
    
    for table, prefix in [('raw_gps', 'gps'), ('raw_delivery', 'delivery')]:
        partitions = []
        for year, month, day in dates:
            partitions.append({
                'Values': [str(year), str(month), str(day)],
                'StorageDescriptor': {
                    'Columns': [],  # Inherit from table
                    'Location': f's3://{S3_BUCKET}/raw/{prefix}/year={year}/month={month:02d}/day={day:02d}/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            })
        
        try:
            glue.batch_create_partition(
                DatabaseName=DATABASE_NAME,
                TableName=table,
                PartitionInputList=partitions
            )
            print(f"Added {len(partitions)} partitions to {table}")
        except Exception as e:
            print(f"Error adding partitions to {table}: {e}")


def main():
    print("Creating Glue resources...")
    create_database()
    create_gps_table()
    create_delivery_table()
    add_partitions()
    print("\nGlue setup complete!")
    
    # List tables
    response = glue.get_tables(DatabaseName=DATABASE_NAME)
    print(f"\nTables in {DATABASE_NAME}:")
    for table in response['TableList']:
        print(f"  - {table['Name']}")


if __name__ == "__main__":
    main()
