"""
AWS Kinesis Producer
Sends GPS and delivery events to AWS Kinesis streams.
"""

import json
import time
import boto3
from typing import Dict, List, Optional, Union
from botocore.exceptions import ClientError
from dataclasses import dataclass
import logging

from .gps_simulator import GPSSimulator, GPSEvent
from .delivery_simulator import DeliverySimulator, DeliveryEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class KinesisConfig:
    """Configuration for Kinesis streams."""
    gps_stream_name: str = "fleet-gps-events"
    delivery_stream_name: str = "fleet-delivery-events"
    region_name: str = "us-east-1"
    batch_size: int = 100
    max_retries: int = 3


class KinesisProducer:
    """
    Produces events to AWS Kinesis streams.
    
    Usage:
        producer = KinesisProducer()
        producer.start_streaming(duration_seconds=3600)
    """
    
    def __init__(self, config: Optional[KinesisConfig] = None, 
                 use_localstack: bool = False):
        """
        Initialize the Kinesis producer.
        
        Args:
            config: Kinesis configuration
            use_localstack: Use LocalStack for local testing
        """
        self.config = config or KinesisConfig()
        
        # Initialize boto3 client
        if use_localstack:
            self.kinesis_client = boto3.client(
                'kinesis',
                endpoint_url='http://localhost:4566',
                region_name=self.config.region_name,
                aws_access_key_id='test',
                aws_secret_access_key='test'
            )
        else:
            self.kinesis_client = boto3.client(
                'kinesis',
                region_name=self.config.region_name
            )
            
        self.gps_simulator = None
        self.delivery_simulator = None
        
    def create_streams(self):
        """Create Kinesis streams if they don't exist."""
        streams = [
            self.config.gps_stream_name,
            self.config.delivery_stream_name
        ]
        
        for stream_name in streams:
            try:
                self.kinesis_client.describe_stream(StreamName=stream_name)
                logger.info(f"Stream {stream_name} already exists")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.info(f"Creating stream {stream_name}...")
                    self.kinesis_client.create_stream(
                        StreamName=stream_name,
                        ShardCount=2
                    )
                    # Wait for stream to become active
                    waiter = self.kinesis_client.get_waiter('stream_exists')
                    waiter.wait(StreamName=stream_name)
                    logger.info(f"Stream {stream_name} created")
                else:
                    raise
                    
    def _put_records(self, stream_name: str, records: List[Dict]) -> Dict:
        """
        Put records to a Kinesis stream with retry logic.
        
        Args:
            stream_name: Target stream name
            records: List of records to send
            
        Returns:
            Response from Kinesis
        """
        kinesis_records = []
        for record in records:
            # Use vehicle_id or delivery_id as partition key for ordering
            partition_key = record.get('vehicle_id') or record.get('delivery_id') or 'default'
            kinesis_records.append({
                'Data': json.dumps(record).encode('utf-8'),
                'PartitionKey': partition_key
            })
            
        for attempt in range(self.config.max_retries):
            try:
                response = self.kinesis_client.put_records(
                    StreamName=stream_name,
                    Records=kinesis_records
                )
                
                failed_count = response.get('FailedRecordCount', 0)
                if failed_count > 0:
                    logger.warning(f"{failed_count} records failed, retrying...")
                    # Retry failed records
                    failed_records = []
                    for i, result in enumerate(response['Records']):
                        if 'ErrorCode' in result:
                            failed_records.append(kinesis_records[i])
                    kinesis_records = failed_records
                else:
                    return response
                    
            except ClientError as e:
                logger.error(f"Error putting records: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
                    
        return response
        
    def send_gps_events(self, events: List[GPSEvent]) -> int:
        """
        Send GPS events to Kinesis.
        
        Args:
            events: List of GPS events
            
        Returns:
            Number of successfully sent records
        """
        if not events:
            return 0
            
        records = [event.to_dict() for event in events]
        
        # Send in batches
        total_sent = 0
        for i in range(0, len(records), self.config.batch_size):
            batch = records[i:i + self.config.batch_size]
            response = self._put_records(self.config.gps_stream_name, batch)
            total_sent += len(batch) - response.get('FailedRecordCount', 0)
            
        return total_sent
        
    def send_delivery_events(self, events: List[DeliveryEvent]) -> int:
        """
        Send delivery events to Kinesis.
        
        Args:
            events: List of delivery events
            
        Returns:
            Number of successfully sent records
        """
        if not events:
            return 0
            
        records = [event.to_dict() for event in events]
        
        # Send in batches
        total_sent = 0
        for i in range(0, len(records), self.config.batch_size):
            batch = records[i:i + self.config.batch_size]
            response = self._put_records(self.config.delivery_stream_name, batch)
            total_sent += len(batch) - response.get('FailedRecordCount', 0)
            
        return total_sent
        
    def start_streaming(self, 
                        duration_seconds: int = 3600,
                        num_vehicles: int = 100,
                        orders_per_hour: int = 500,
                        interval_seconds: float = 1.0):
        """
        Start streaming events to Kinesis.
        
        Args:
            duration_seconds: How long to stream
            num_vehicles: Number of vehicles to simulate
            orders_per_hour: Orders per hour to simulate
            interval_seconds: Interval between batches
        """
        logger.info("Initializing simulators...")
        self.gps_simulator = GPSSimulator(num_vehicles=num_vehicles)
        self.delivery_simulator = DeliverySimulator(
            orders_per_hour=orders_per_hour,
            num_vehicles=num_vehicles
        )
        
        logger.info(f"Starting streaming for {duration_seconds} seconds...")
        start_time = time.time()
        
        total_gps = 0
        total_delivery = 0
        
        try:
            while (time.time() - start_time) < duration_seconds:
                # Generate and send GPS events
                gps_events = self.gps_simulator.generate_batch(batch_size=50)
                sent = self.send_gps_events(gps_events)
                total_gps += sent
                
                # Generate and send delivery events
                delivery_events = self.delivery_simulator.generate_batch(batch_size=20)
                sent = self.send_delivery_events(delivery_events)
                total_delivery += sent
                
                elapsed = int(time.time() - start_time)
                logger.info(f"[{elapsed}s] GPS: {total_gps}, Deliveries: {total_delivery}")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            
        logger.info(f"Streaming complete. GPS: {total_gps}, Deliveries: {total_delivery}")
        
    def get_stream_metrics(self) -> Dict:
        """Get metrics for the Kinesis streams."""
        metrics = {}
        
        for stream_name in [self.config.gps_stream_name, self.config.delivery_stream_name]:
            try:
                response = self.kinesis_client.describe_stream_summary(
                    StreamName=stream_name
                )
                summary = response['StreamDescriptionSummary']
                metrics[stream_name] = {
                    'status': summary['StreamStatus'],
                    'shard_count': summary['OpenShardCount'],
                    'retention_hours': summary['RetentionPeriodHours']
                }
            except ClientError:
                metrics[stream_name] = {'status': 'NOT_FOUND'}
                
        return metrics


def main():
    """Run the Kinesis producer in standalone mode."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kinesis Event Producer")
    parser.add_argument("--vehicles", type=int, default=100, help="Number of vehicles")
    parser.add_argument("--orders-per-hour", type=int, default=500, help="Orders per hour")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--interval", type=float, default=1.0, help="Batch interval")
    parser.add_argument("--localstack", action="store_true", help="Use LocalStack")
    parser.add_argument("--create-streams", action="store_true", help="Create streams first")
    
    args = parser.parse_args()
    
    producer = KinesisProducer(use_localstack=args.localstack)
    
    if args.create_streams:
        producer.create_streams()
        
    producer.start_streaming(
        duration_seconds=args.duration,
        num_vehicles=args.vehicles,
        orders_per_hour=args.orders_per_hour,
        interval_seconds=args.interval
    )


if __name__ == "__main__":
    main()
