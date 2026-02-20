"""
Test CDC incremental logic with real S3 data.
Measures actual cost reduction from partition pruning.
"""

import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import time
import json
from datetime import datetime

S3_BUCKET = "fleet-analytics-data-lake-054375299485"
REGION = "us-east-1"

s3 = boto3.client('s3', region_name=REGION)


class CDCProcessor:
    """
    Change Data Capture processor with watermark tracking.
    """
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.watermark_key = f"watermarks/{job_name}.json"
    
    def get_watermark(self) -> dict:
        """Get current watermark from S3."""
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=self.watermark_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except s3.exceptions.NoSuchKey:
            return {"high_watermark": None, "last_processed": None}
    
    def set_watermark(self, high_watermark: str):
        """Update watermark in S3."""
        watermark_data = {
            "high_watermark": high_watermark,
            "last_processed": datetime.utcnow().isoformat(),
            "job_name": self.job_name
        }
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=self.watermark_key,
            Body=json.dumps(watermark_data, indent=2),
            ContentType='application/json'
        )
        return watermark_data
    
    def list_partitions(self, prefix: str) -> list:
        """List all partitions in S3 path."""
        partitions = []
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                partitions.append(common_prefix['Prefix'])
        
        return partitions
    
    def get_incremental_partitions(self, prefix: str, watermark: str) -> list:
        """
        Get only partitions newer than watermark.
        This is the core CDC logic - filter before reading.
        """
        all_partitions = []
        
        # Get year partitions
        year_prefixes = self.list_partitions(prefix)
        
        for year_prefix in year_prefixes:
            # Get month partitions
            month_prefixes = self.list_partitions(year_prefix)
            
            for month_prefix in month_prefixes:
                # Get day partitions
                day_prefixes = self.list_partitions(month_prefix)
                all_partitions.extend(day_prefixes)
        
        if not watermark:
            return all_partitions
        
        # Filter partitions by watermark
        watermark_date = datetime.fromisoformat(watermark.replace('Z', '+00:00')).date()
        
        incremental_partitions = []
        for partition in all_partitions:
            # Extract date from partition path
            parts = partition.rstrip('/').split('/')
            year = month = day = None
            for part in parts:
                if part.startswith('year='):
                    year = int(part.split('=')[1])
                elif part.startswith('month='):
                    month = int(part.split('=')[1])
                elif part.startswith('day='):
                    day = int(part.split('=')[1])
            
            if year and month and day:
                partition_date = datetime(year, month, day).date()
                if partition_date > watermark_date:
                    incremental_partitions.append(partition)
        
        return incremental_partitions


def read_partition(prefix: str) -> tuple:
    """Read parquet files from partition, return (df, bytes_read)."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_bytes = 0
    dfs = []
    
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            total_bytes += obj['Size']
            data = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
            df = pd.read_parquet(io.BytesIO(data['Body'].read()))
            dfs.append(df)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True), total_bytes
    return pd.DataFrame(), 0


def main():
    print("="*60)
    print("CDC INCREMENTAL PROCESSING TEST")
    print("="*60)
    
    cdc = CDCProcessor("fleet_gps_etl")
    
    # Get current watermark
    current_watermark = cdc.get_watermark()
    print(f"\nCurrent watermark: {current_watermark}")
    
    # =========================================================================
    # TEST 1: Full scan (no watermark)
    # =========================================================================
    print("\n" + "-"*60)
    print("TEST 1: FULL SCAN (No Watermark)")
    print("-"*60)
    
    start_time = time.time()
    
    all_partitions = cdc.get_incremental_partitions("raw/gps/", None)
    print(f"Partitions to process: {len(all_partitions)}")
    
    full_scan_bytes = 0
    full_scan_records = 0
    
    for partition in all_partitions:
        df, bytes_read = read_partition(partition)
        full_scan_bytes += bytes_read
        full_scan_records += len(df)
        print(f"  {partition}: {len(df)} records, {bytes_read:,} bytes")
    
    full_scan_time = time.time() - start_time
    
    print(f"\nFull scan results:")
    print(f"  Total partitions: {len(all_partitions)}")
    print(f"  Total records: {full_scan_records:,}")
    print(f"  Total bytes read: {full_scan_bytes:,}")
    print(f"  Time: {full_scan_time:.2f}s")
    
    # =========================================================================
    # TEST 2: Set watermark to day 13 (simulate previous run)
    # =========================================================================
    print("\n" + "-"*60)
    print("TEST 2: SET WATERMARK (Simulate Previous Run)")
    print("-"*60)
    
    # Set watermark to Jan 13 (so only Jan 14 and 15 should be processed)
    watermark = "2024-01-13T23:59:59Z"
    cdc.set_watermark(watermark)
    print(f"Set watermark to: {watermark}")
    
    # =========================================================================
    # TEST 3: Incremental scan (with watermark)
    # =========================================================================
    print("\n" + "-"*60)
    print("TEST 3: INCREMENTAL SCAN (With Watermark)")
    print("-"*60)
    
    start_time = time.time()
    
    incremental_partitions = cdc.get_incremental_partitions("raw/gps/", watermark)
    print(f"Partitions to process: {len(incremental_partitions)}")
    
    incremental_bytes = 0
    incremental_records = 0
    
    for partition in incremental_partitions:
        df, bytes_read = read_partition(partition)
        incremental_bytes += bytes_read
        incremental_records += len(df)
        print(f"  {partition}: {len(df)} records, {bytes_read:,} bytes")
    
    incremental_time = time.time() - start_time
    
    print(f"\nIncremental scan results:")
    print(f"  Total partitions: {len(incremental_partitions)}")
    print(f"  Total records: {incremental_records:,}")
    print(f"  Total bytes read: {incremental_bytes:,}")
    print(f"  Time: {incremental_time:.2f}s")
    
    # =========================================================================
    # COST REDUCTION ANALYSIS
    # =========================================================================
    print("\n" + "="*60)
    print("COST REDUCTION ANALYSIS")
    print("="*60)
    
    partition_reduction = (1 - len(incremental_partitions) / len(all_partitions)) * 100
    bytes_reduction = (1 - incremental_bytes / full_scan_bytes) * 100
    records_reduction = (1 - incremental_records / full_scan_records) * 100
    time_reduction = (1 - incremental_time / full_scan_time) * 100
    
    print(f"\n{'Metric':<25} {'Full Scan':<15} {'Incremental':<15} {'Reduction':<10}")
    print("-"*65)
    print(f"{'Partitions':<25} {len(all_partitions):<15} {len(incremental_partitions):<15} {partition_reduction:.1f}%")
    print(f"{'Records':<25} {full_scan_records:<15,} {incremental_records:<15,} {records_reduction:.1f}%")
    print(f"{'Bytes Read':<25} {full_scan_bytes:<15,} {incremental_bytes:<15,} {bytes_reduction:.1f}%")
    print(f"{'Time (seconds)':<25} {full_scan_time:<15.2f} {incremental_time:<15.2f} {time_reduction:.1f}%")
    
    # Athena cost estimate ($5 per TB scanned)
    full_scan_cost = (full_scan_bytes / 1e12) * 5
    incremental_cost = (incremental_bytes / 1e12) * 5
    
    print(f"\nAthena Cost Estimate (at $5/TB):")
    print(f"  Full scan: ${full_scan_cost:.6f}")
    print(f"  Incremental: ${incremental_cost:.6f}")
    print(f"  Savings: {bytes_reduction:.1f}%")
    
    # Update watermark to latest
    new_watermark = "2024-01-15T23:59:59Z"
    cdc.set_watermark(new_watermark)
    print(f"\nUpdated watermark to: {new_watermark}")
    
    # Save results
    results = {
        "test_date": datetime.utcnow().isoformat(),
        "full_scan": {
            "partitions": len(all_partitions),
            "records": full_scan_records,
            "bytes": full_scan_bytes,
            "time_seconds": full_scan_time
        },
        "incremental_scan": {
            "partitions": len(incremental_partitions),
            "records": incremental_records,
            "bytes": incremental_bytes,
            "time_seconds": incremental_time
        },
        "reduction": {
            "partitions_pct": partition_reduction,
            "records_pct": records_reduction,
            "bytes_pct": bytes_reduction,
            "time_pct": time_reduction
        }
    }
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="metrics/cdc_test_results.json",
        Body=json.dumps(results, indent=2),
        ContentType='application/json'
    )
    print(f"\nResults saved to s3://{S3_BUCKET}/metrics/cdc_test_results.json")


if __name__ == "__main__":
    main()
