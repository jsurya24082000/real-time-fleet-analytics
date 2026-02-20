"""
Generate sample fleet data and upload to S3 with partitioning.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
import uuid
import io
import os

# Configuration
S3_BUCKET = "fleet-analytics-data-lake-054375299485"
REGION = "us-east-1"

s3 = boto3.client('s3', region_name=REGION)


def generate_gps_data(num_records: int, date: datetime) -> pd.DataFrame:
    """Generate GPS telemetry data."""
    vehicles = [f"VH-{i:04d}" for i in range(1, 51)]  # 50 vehicles
    
    records = []
    for _ in range(num_records):
        ts = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        records.append({
            "event_id": str(uuid.uuid4()),
            "vehicle_id": random.choice(vehicles),
            "timestamp": ts.isoformat(),
            "latitude": round(random.uniform(25.0, 48.0), 6),
            "longitude": round(random.uniform(-125.0, -70.0), 6),
            "speed_mph": round(random.uniform(0, 75), 1),
            "heading": random.randint(0, 359),
            "fuel_level": round(random.uniform(10, 100), 1),
            "engine_status": random.choice(["ON", "OFF", "IDLE"]),
            "odometer": random.randint(10000, 150000)
        })
    
    return pd.DataFrame(records)


def generate_delivery_data(num_records: int, date: datetime) -> pd.DataFrame:
    """Generate delivery event data."""
    vehicles = [f"VH-{i:04d}" for i in range(1, 51)]
    statuses = ["PENDING", "IN_TRANSIT", "DELIVERED", "FAILED"]
    
    records = []
    for _ in range(num_records):
        ts = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        records.append({
            "delivery_id": str(uuid.uuid4()),
            "vehicle_id": random.choice(vehicles),
            "timestamp": ts.isoformat(),
            "status": random.choice(statuses),
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "package_count": random.randint(1, 10),
            "delivery_time_minutes": random.randint(5, 120) if random.random() > 0.3 else None,
            "distance_miles": round(random.uniform(1, 50), 2),
            "tip_amount": round(random.uniform(0, 20), 2) if random.random() > 0.5 else 0
        })
    
    return pd.DataFrame(records)


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to S3."""
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue()
    )
    print(f"Uploaded: s3://{S3_BUCKET}/{s3_key} ({len(df)} records)")


def main():
    # Generate data for 3 days
    base_date = datetime(2024, 1, 13)
    
    total_gps = 0
    total_delivery = 0
    
    for day_offset in range(3):
        date = base_date + timedelta(days=day_offset)
        year = date.year
        month = date.month
        day = date.day
        
        print(f"\nGenerating data for {date.date()}...")
        
        # GPS data - 10K records per day
        gps_df = generate_gps_data(10000, date)
        gps_key = f"raw/gps/year={year}/month={month:02d}/day={day:02d}/gps_data.parquet"
        upload_parquet_to_s3(gps_df, gps_key)
        total_gps += len(gps_df)
        
        # Delivery data - 2K records per day
        delivery_df = generate_delivery_data(2000, date)
        delivery_key = f"raw/delivery/year={year}/month={month:02d}/day={day:02d}/delivery_data.parquet"
        upload_parquet_to_s3(delivery_df, delivery_key)
        total_delivery += len(delivery_df)
    
    print(f"\n=== Upload Complete ===")
    print(f"Total GPS records: {total_gps:,}")
    print(f"Total Delivery records: {total_delivery:,}")
    print(f"S3 Bucket: s3://{S3_BUCKET}/")


if __name__ == "__main__":
    main()
