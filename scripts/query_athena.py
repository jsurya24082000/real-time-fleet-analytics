"""
Query Athena to verify Glue tables work.
"""

import boto3
import time

REGION = "us-east-1"
DATABASE = "fleet_analytics"
S3_BUCKET = "fleet-analytics-data-lake-054375299485"
OUTPUT_LOCATION = f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client('athena', region_name=REGION)


def run_query(query: str, description: str) -> dict:
    """Run Athena query and return results."""
    print(f"\n{'='*60}")
    print(f"Query: {description}")
    print(f"{'='*60}")
    print(f"SQL: {query[:100]}...")
    
    # Start query
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    
    query_id = response['QueryExecutionId']
    print(f"Query ID: {query_id}")
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        print(f"  Status: {state}...")
        time.sleep(2)
    
    if state != 'SUCCEEDED':
        error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query FAILED: {error}")
        return None
    
    # Get results
    results = athena.get_query_results(QueryExecutionId=query_id)
    
    # Parse results
    rows = results['ResultSet']['Rows']
    if len(rows) > 1:
        headers = [col['VarCharValue'] for col in rows[0]['Data']]
        print(f"\nResults ({len(rows)-1} rows):")
        print("-" * 60)
        
        for row in rows[1:6]:  # First 5 data rows
            values = [col.get('VarCharValue', 'NULL') for col in row['Data']]
            for h, v in zip(headers, values):
                print(f"  {h}: {v}")
            print("-" * 30)
    
    # Get stats
    stats = status['QueryExecution']['Statistics']
    print(f"\nQuery Stats:")
    print(f"  Data Scanned: {stats['DataScannedInBytes'] / 1024:.2f} KB")
    print(f"  Execution Time: {stats['EngineExecutionTimeInMillis']} ms")
    
    return {
        'query_id': query_id,
        'state': state,
        'rows': len(rows) - 1,
        'data_scanned_kb': stats['DataScannedInBytes'] / 1024,
        'execution_time_ms': stats['EngineExecutionTimeInMillis']
    }


def main():
    results = []
    
    # Query 1: Count GPS records
    r = run_query(
        "SELECT COUNT(*) as total_records FROM raw_gps",
        "Count GPS Records"
    )
    if r:
        results.append(r)
    
    # Query 2: Count by day (partition pruning test)
    r = run_query(
        """
        SELECT year, month, day, COUNT(*) as records
        FROM raw_gps
        GROUP BY year, month, day
        ORDER BY year, month, day
        """,
        "GPS Records by Day"
    )
    if r:
        results.append(r)
    
    # Query 3: Single partition query (should scan less data)
    r = run_query(
        """
        SELECT COUNT(*) as records, AVG(speed_mph) as avg_speed
        FROM raw_gps
        WHERE year = 2024 AND month = 1 AND day = 15
        """,
        "Single Day Query (Partition Pruning)"
    )
    if r:
        results.append(r)
    
    # Query 4: Delivery stats
    r = run_query(
        """
        SELECT status, COUNT(*) as count, AVG(distance_miles) as avg_distance
        FROM raw_delivery
        GROUP BY status
        """,
        "Delivery Status Summary"
    )
    if r:
        results.append(r)
    
    # Query 5: Vehicle activity
    r = run_query(
        """
        SELECT vehicle_id, COUNT(*) as gps_events, AVG(speed_mph) as avg_speed
        FROM raw_gps
        GROUP BY vehicle_id
        ORDER BY gps_events DESC
        LIMIT 10
        """,
        "Top 10 Vehicles by Activity"
    )
    if r:
        results.append(r)
    
    # Summary
    print("\n" + "="*60)
    print("QUERY SUMMARY")
    print("="*60)
    total_scanned = sum(r['data_scanned_kb'] for r in results)
    total_time = sum(r['execution_time_ms'] for r in results)
    print(f"Total Queries: {len(results)}")
    print(f"Total Data Scanned: {total_scanned:.2f} KB")
    print(f"Total Execution Time: {total_time} ms")
    
    # Partition pruning comparison
    full_scan = results[0]['data_scanned_kb'] if results else 0
    single_day = results[2]['data_scanned_kb'] if len(results) > 2 else 0
    if full_scan > 0:
        reduction = (1 - single_day / full_scan) * 100
        print(f"\nPartition Pruning Reduction: {reduction:.1f}%")
        print(f"  Full scan: {full_scan:.2f} KB")
        print(f"  Single partition: {single_day:.2f} KB")


if __name__ == "__main__":
    main()
