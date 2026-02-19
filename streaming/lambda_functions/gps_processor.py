"""
AWS Lambda Function: GPS Event Processor
Processes GPS events from Kinesis and performs light transformations.
"""

import json
import base64
import boto3
from datetime import datetime
from typing import Dict, List, Any
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
firehose = boto3.client('firehose')

# Configuration
DYNAMODB_TABLE = 'fleet-realtime-metrics'
S3_BUCKET = 'fleet-data-lake'
FIREHOSE_STREAM = 'fleet-gps-firehose'


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Process GPS events from Kinesis.
    
    Args:
        event: Kinesis event containing GPS records
        context: Lambda context
        
    Returns:
        Processing result
    """
    logger.info(f"Processing {len(event['Records'])} records")
    
    processed_records = []
    failed_records = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            payload = base64.b64decode(record['kinesis']['data'])
            gps_event = json.loads(payload)
            
            # Enrich the event
            enriched_event = enrich_gps_event(gps_event)
            
            # Update real-time metrics in DynamoDB
            update_realtime_metrics(enriched_event)
            
            # Forward to Firehose for S3 storage
            forward_to_firehose(enriched_event)
            
            processed_records.append(enriched_event)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            failed_records.append({
                'record': record,
                'error': str(e)
            })
    
    logger.info(f"Processed: {len(processed_records)}, Failed: {len(failed_records)}")
    
    return {
        'statusCode': 200,
        'body': {
            'processed': len(processed_records),
            'failed': len(failed_records)
        }
    }


def enrich_gps_event(event: Dict) -> Dict:
    """
    Enrich GPS event with derived fields.
    
    Args:
        event: Raw GPS event
        
    Returns:
        Enriched event
    """
    enriched = event.copy()
    
    # Parse timestamp
    timestamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
    
    # Add time dimensions
    enriched['event_date'] = timestamp.strftime('%Y-%m-%d')
    enriched['event_hour'] = timestamp.hour
    enriched['day_of_week'] = timestamp.strftime('%A')
    enriched['is_weekend'] = timestamp.weekday() >= 5
    
    # Categorize speed
    speed = event.get('speed_mph', 0)
    if speed == 0:
        enriched['speed_category'] = 'STOPPED'
    elif speed < 25:
        enriched['speed_category'] = 'SLOW'
    elif speed < 45:
        enriched['speed_category'] = 'NORMAL'
    elif speed < 65:
        enriched['speed_category'] = 'FAST'
    else:
        enriched['speed_category'] = 'VERY_FAST'
        
    # Fuel status
    fuel = event.get('fuel_level', 1.0)
    if fuel < 0.1:
        enriched['fuel_status'] = 'CRITICAL'
    elif fuel < 0.25:
        enriched['fuel_status'] = 'LOW'
    elif fuel < 0.5:
        enriched['fuel_status'] = 'MEDIUM'
    else:
        enriched['fuel_status'] = 'GOOD'
        
    # Add processing metadata
    enriched['processed_at'] = datetime.utcnow().isoformat() + 'Z'
    enriched['lambda_version'] = '1.0.0'
    
    return enriched


def update_realtime_metrics(event: Dict):
    """
    Update real-time metrics in DynamoDB.
    
    Args:
        event: Enriched GPS event
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        
        vehicle_id = event['vehicle_id']
        
        # Update vehicle's latest position
        table.put_item(
            Item={
                'pk': f"VEHICLE#{vehicle_id}",
                'sk': 'LATEST',
                'vehicle_id': vehicle_id,
                'latitude': str(event['latitude']),
                'longitude': str(event['longitude']),
                'speed_mph': str(event['speed_mph']),
                'heading': event['heading'],
                'fuel_level': str(event['fuel_level']),
                'engine_status': event['engine_status'],
                'last_updated': event['timestamp'],
                'ttl': int(datetime.utcnow().timestamp()) + 86400  # 24 hour TTL
            }
        )
        
        # Update fleet-wide aggregates
        table.update_item(
            Key={
                'pk': 'FLEET_METRICS',
                'sk': event['event_date']
            },
            UpdateExpression='''
                SET total_events = if_not_exists(total_events, :zero) + :one,
                    active_vehicles = if_not_exists(active_vehicles, :empty_set),
                    last_updated = :now
                ADD active_vehicles :vehicle_set
            ''',
            ExpressionAttributeValues={
                ':zero': 0,
                ':one': 1,
                ':empty_set': set(),
                ':vehicle_set': {vehicle_id},
                ':now': event['timestamp']
            }
        )
        
    except Exception as e:
        logger.error(f"DynamoDB update failed: {e}")
        # Don't fail the whole batch for DynamoDB errors


def forward_to_firehose(event: Dict):
    """
    Forward event to Kinesis Firehose for S3 storage.
    
    Args:
        event: Enriched GPS event
    """
    try:
        firehose.put_record(
            DeliveryStreamName=FIREHOSE_STREAM,
            Record={
                'Data': json.dumps(event).encode('utf-8') + b'\n'
            }
        )
    except Exception as e:
        logger.error(f"Firehose forward failed: {e}")


def calculate_geohash(lat: float, lon: float, precision: int = 6) -> str:
    """
    Calculate geohash for location-based queries.
    
    Args:
        lat: Latitude
        lon: Longitude
        precision: Geohash precision
        
    Returns:
        Geohash string
    """
    # Simplified geohash implementation
    BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz'
    
    lat_range = (-90.0, 90.0)
    lon_range = (-180.0, 180.0)
    
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    is_lon = True
    
    while len(geohash) < precision:
        if is_lon:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                ch |= bits[bit]
                lon_range = (mid, lon_range[1])
            else:
                lon_range = (lon_range[0], mid)
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch |= bits[bit]
                lat_range = (mid, lat_range[1])
            else:
                lat_range = (lat_range[0], mid)
                
        is_lon = not is_lon
        
        if bit < 4:
            bit += 1
        else:
            geohash.append(BASE32[ch])
            bit = 0
            ch = 0
            
    return ''.join(geohash)
