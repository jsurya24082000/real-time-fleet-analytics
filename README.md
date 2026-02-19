# Real-Time Fleet & Delivery Analytics Platform

A scalable AWS big data pipeline for real-time transportation analytics, featuring GPS tracking, delivery monitoring, and business intelligence dashboards.

## Performance Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| **Event Throughput** | 10,000+ events/sec | Kinesis stream capacity |
| **Processing Latency** | <5 seconds | End-to-end streaming latency |
| **Data Freshness** | Real-time | Dashboard refresh rate |
| **Historical Analysis** | 90 days | Data retention in warehouse |

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Sources   │     │  Streaming      │     │  Storage        │
│                 │     │                 │     │                 │
│  GPS Devices    │────▶│  AWS Kinesis    │────▶│  S3 Data Lake   │
│  Delivery Apps  │     │  Lambda         │     │  Redshift DW    │
│  Order Systems  │     │  Spark Streaming│     │  DynamoDB       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │  BI Layer       │
                                               │                 │
                                               │  QuickSight     │
                                               │  Power BI       │
                                               └─────────────────┘
```

## Project Structure

```
real-time-fleet-analytics/
├── ingestion/              # Data producers & Kinesis setup
│   ├── gps_simulator.py    # GPS event generator
│   ├── delivery_simulator.py
│   └── kinesis_producer.py
├── streaming/              # Real-time processing
│   ├── lambda_functions/   # AWS Lambda handlers
│   └── spark_streaming/    # Spark Streaming jobs
├── storage/                # Data lake & warehouse
│   ├── s3_schemas/         # S3 partition schemas
│   ├── redshift_ddl/       # Redshift table definitions
│   └── dynamodb_schemas/   # DynamoDB table configs
├── etl/                    # Batch ETL jobs
│   ├── spark_jobs/         # PySpark ETL scripts
│   └── star_schema/        # Dimensional modeling
├── dashboards/             # BI visualizations
│   ├── quicksight/         # QuickSight configs
│   └── metrics/            # KPI definitions
├── infrastructure/         # IaC templates
│   ├── cloudformation/     # AWS CloudFormation
│   └── terraform/          # Terraform configs
├── tests/                  # Unit & integration tests
└── docs/                   # Documentation
```

## Key Features

### Data Ingestion
- **GPS Event Streaming**: Real-time vehicle location tracking
- **Delivery Events**: Order pickup, transit, delivery status
- **Order Integration**: Customer orders and routing data

### Streaming Processing
- **AWS Kinesis**: High-throughput event ingestion
- **Lambda Functions**: Lightweight transformations
- **Spark Streaming**: Complex event processing on EMR

### Storage Layer
- **S3 Data Lake**: Raw and processed data (Parquet format)
- **Redshift**: Star schema data warehouse
- **DynamoDB**: Real-time metrics and lookups

### ETL Pipeline
- Data cleansing and deduplication
- Aggregation and summarization
- Star schema dimensional modeling

### Business Intelligence
- **KPIs Tracked**:
  - Average delivery time
  - Delayed shipments percentage
  - Route efficiency score
  - Cost per mile
  - Fleet utilization rate

## Quick Start

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.9+
- Apache Spark 3.x
- AWS CLI configured

### Installation

```bash
# Clone repository
git clone https://github.com/Karthikaa-Mikkilineni/real-time-fleet-analytics.git
cd real-time-fleet-analytics

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
aws configure

# Deploy infrastructure
cd infrastructure/terraform
terraform init
terraform apply
```

### Running the Pipeline

```bash
# Start GPS simulator (local testing)
python ingestion/gps_simulator.py --vehicles 100 --interval 1

# Deploy Lambda functions
cd streaming/lambda_functions
./deploy.sh

# Submit Spark streaming job to EMR
spark-submit --master yarn streaming/spark_streaming/fleet_processor.py
```

## Data Schema

### GPS Events
```json
{
  "vehicle_id": "V001",
  "timestamp": "2024-01-15T10:30:00Z",
  "latitude": 40.7128,
  "longitude": -74.0060,
  "speed_mph": 45.5,
  "heading": 180,
  "fuel_level": 0.75
}
```

### Delivery Events
```json
{
  "delivery_id": "D12345",
  "order_id": "O98765",
  "vehicle_id": "V001",
  "event_type": "DELIVERED",
  "timestamp": "2024-01-15T14:30:00Z",
  "location": {"lat": 40.7589, "lon": -73.9851}
}
```

## Star Schema Design

### Fact Tables
- `fact_deliveries` - Delivery metrics
- `fact_vehicle_trips` - Trip-level aggregations
- `fact_route_performance` - Route efficiency metrics

### Dimension Tables
- `dim_vehicle` - Fleet information
- `dim_driver` - Driver details
- `dim_customer` - Customer data
- `dim_location` - Geographic hierarchy
- `dim_date` - Date dimension
- `dim_time` - Time dimension

## Technologies Used

| Layer | Technology |
|-------|------------|
| Ingestion | Python, Boto3, AWS Kinesis |
| Streaming | AWS Lambda, Spark Streaming, EMR |
| Storage | S3, Redshift, DynamoDB |
| ETL | PySpark, AWS Glue |
| BI | QuickSight, Power BI |
| Infrastructure | Terraform, CloudFormation |

## License

MIT License
