"""
AWS Lambda Function: Delivery Event Processor
Processes delivery events from Kinesis and calculates KPIs.
"""

import json
import base64
import boto3
from datetime import datetime
from typing import Dict, List, Any
from decimal import Decimal
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
firehose = boto3.client('firehose')

# Configuration
DYNAMODB_TABLE = 'fleet-delivery-metrics'
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:ACCOUNT_ID:delivery-alerts'
FIREHOSE_STREAM = 'fleet-delivery-firehose'

# Alert thresholds
DELAY_ALERT_THRESHOLD_MINUTES = 30
FAILED_DELIVERY_ALERT_THRESHOLD = 3


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Process delivery events from Kinesis.
    
    Args:
        event: Kinesis event containing delivery records
        context: Lambda context
        
    Returns:
        Processing result
    """
    logger.info(f"Processing {len(event['Records'])} delivery records")
    
    processed_records = []
    alerts_sent = 0
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            payload = base64.b64decode(record['kinesis']['data'])
            delivery_event = json.loads(payload)
            
            # Enrich the event
            enriched_event = enrich_delivery_event(delivery_event)
            
            # Update delivery metrics
            update_delivery_metrics(enriched_event)
            
            # Check for alerts
            if check_and_send_alerts(enriched_event):
                alerts_sent += 1
            
            # Forward to Firehose for S3 storage
            forward_to_firehose(enriched_event)
            
            processed_records.append(enriched_event)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
    
    logger.info(f"Processed: {len(processed_records)}, Alerts: {alerts_sent}")
    
    return {
        'statusCode': 200,
        'body': {
            'processed': len(processed_records),
            'alerts_sent': alerts_sent
        }
    }


def enrich_delivery_event(event: Dict) -> Dict:
    """
    Enrich delivery event with derived fields and KPIs.
    
    Args:
        event: Raw delivery event
        
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
    enriched['is_business_hours'] = 9 <= timestamp.hour <= 17
    
    # Categorize delay
    delay = event.get('delay_minutes', 0)
    if delay <= 0:
        enriched['delay_category'] = 'ON_TIME'
    elif delay <= 15:
        enriched['delay_category'] = 'SLIGHT_DELAY'
    elif delay <= 30:
        enriched['delay_category'] = 'MODERATE_DELAY'
    elif delay <= 60:
        enriched['delay_category'] = 'SIGNIFICANT_DELAY'
    else:
        enriched['delay_category'] = 'SEVERE_DELAY'
        
    # Package size category
    weight = event.get('package_weight_lbs', 0)
    if weight < 1:
        enriched['package_size'] = 'SMALL'
    elif weight < 5:
        enriched['package_size'] = 'MEDIUM'
    elif weight < 20:
        enriched['package_size'] = 'LARGE'
    else:
        enriched['package_size'] = 'EXTRA_LARGE'
        
    # Delivery success flag
    event_type = event.get('event_type', '')
    enriched['is_successful'] = event_type == 'DELIVERED'
    enriched['is_failed'] = event_type == 'FAILED_ATTEMPT'
    enriched['is_returned'] = event_type == 'RETURNED'
    
    # Add processing metadata
    enriched['processed_at'] = datetime.utcnow().isoformat() + 'Z'
    
    return enriched


def update_delivery_metrics(event: Dict):
    """
    Update delivery metrics in DynamoDB.
    
    Args:
        event: Enriched delivery event
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        
        delivery_id = event['delivery_id']
        event_date = event['event_date']
        
        # Update delivery tracking
        table.put_item(
            Item={
                'pk': f"DELIVERY#{delivery_id}",
                'sk': event['event_type'],
                'delivery_id': delivery_id,
                'order_id': event['order_id'],
                'vehicle_id': event['vehicle_id'],
                'driver_id': event['driver_id'],
                'customer_id': event['customer_id'],
                'event_type': event['event_type'],
                'timestamp': event['timestamp'],
                'latitude': Decimal(str(event['latitude'])),
                'longitude': Decimal(str(event['longitude'])),
                'delay_minutes': event.get('delay_minutes', 0),
                'delay_category': event['delay_category'],
                'ttl': int(datetime.utcnow().timestamp()) + 604800  # 7 day TTL
            }
        )
        
        # Update daily aggregates for completed deliveries
        if event['event_type'] == 'DELIVERED':
            table.update_item(
                Key={
                    'pk': 'DAILY_METRICS',
                    'sk': event_date
                },
                UpdateExpression='''
                    SET total_deliveries = if_not_exists(total_deliveries, :zero) + :one,
                        total_delay_minutes = if_not_exists(total_delay_minutes, :zero) + :delay,
                        on_time_count = if_not_exists(on_time_count, :zero) + :on_time,
                        last_updated = :now
                ''',
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':one': 1,
                    ':delay': event.get('delay_minutes', 0),
                    ':on_time': 1 if event['delay_category'] == 'ON_TIME' else 0,
                    ':now': event['timestamp']
                }
            )
            
        # Update failed delivery count
        if event['event_type'] == 'FAILED_ATTEMPT':
            table.update_item(
                Key={
                    'pk': 'DAILY_METRICS',
                    'sk': event_date
                },
                UpdateExpression='''
                    SET failed_attempts = if_not_exists(failed_attempts, :zero) + :one,
                        last_updated = :now
                ''',
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':one': 1,
                    ':now': event['timestamp']
                }
            )
            
        # Update driver performance
        if event['driver_id'] and event['event_type'] == 'DELIVERED':
            table.update_item(
                Key={
                    'pk': f"DRIVER#{event['driver_id']}",
                    'sk': event_date
                },
                UpdateExpression='''
                    SET deliveries_completed = if_not_exists(deliveries_completed, :zero) + :one,
                        total_delay = if_not_exists(total_delay, :zero) + :delay,
                        last_updated = :now
                ''',
                ExpressionAttributeValues={
                    ':zero': 0,
                    ':one': 1,
                    ':delay': event.get('delay_minutes', 0),
                    ':now': event['timestamp']
                }
            )
        
    except Exception as e:
        logger.error(f"DynamoDB update failed: {e}")


def check_and_send_alerts(event: Dict) -> bool:
    """
    Check for alert conditions and send notifications.
    
    Args:
        event: Enriched delivery event
        
    Returns:
        True if an alert was sent
    """
    alert_sent = False
    
    try:
        # Alert for severe delays
        if event.get('delay_minutes', 0) > DELAY_ALERT_THRESHOLD_MINUTES:
            send_alert(
                subject=f"Delivery Delay Alert - {event['delivery_id']}",
                message=f"""
                Delivery {event['delivery_id']} is delayed by {event['delay_minutes']} minutes.
                
                Order ID: {event['order_id']}
                Customer: {event['customer_id']}
                Driver: {event['driver_id']}
                Vehicle: {event['vehicle_id']}
                
                Current Status: {event['event_type']}
                """
            )
            alert_sent = True
            
        # Alert for failed deliveries
        if event['event_type'] == 'FAILED_ATTEMPT':
            send_alert(
                subject=f"Failed Delivery - {event['delivery_id']}",
                message=f"""
                Delivery attempt failed for {event['delivery_id']}.
                
                Order ID: {event['order_id']}
                Customer: {event['customer_id']}
                Reason: {event.get('notes', 'Unknown')}
                
                Location: ({event['latitude']}, {event['longitude']})
                """
            )
            alert_sent = True
            
    except Exception as e:
        logger.error(f"Alert sending failed: {e}")
        
    return alert_sent


def send_alert(subject: str, message: str):
    """
    Send alert via SNS.
    
    Args:
        subject: Alert subject
        message: Alert message
    """
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"Alert sent: {subject}")
    except Exception as e:
        logger.error(f"SNS publish failed: {e}")


def forward_to_firehose(event: Dict):
    """
    Forward event to Kinesis Firehose for S3 storage.
    
    Args:
        event: Enriched delivery event
    """
    try:
        # Convert Decimal to float for JSON serialization
        serializable_event = json.loads(
            json.dumps(event, default=str)
        )
        
        firehose.put_record(
            DeliveryStreamName=FIREHOSE_STREAM,
            Record={
                'Data': json.dumps(serializable_event).encode('utf-8') + b'\n'
            }
        )
    except Exception as e:
        logger.error(f"Firehose forward failed: {e}")
