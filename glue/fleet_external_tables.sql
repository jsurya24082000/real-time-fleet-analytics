-- =============================================================================
-- AWS Glue/Hive External Tables for Fleet Analytics
-- =============================================================================
-- 
-- Creates partitioned external tables on S3 data lake.
-- 
-- Why external tables:
-- - Schema-on-read: Data stays in S3, Glue/Athena reads it
-- - Cost-effective: No data duplication
-- - Partition pruning: Only scan relevant partitions
-- - Compatible with Spark, Presto, Athena, Redshift Spectrum
--
-- Partitioning strategy:
-- - event_date: Primary partition for time-based queries
-- - Enables efficient date range queries
-- - Reduces scan costs by 90%+ for typical queries
-- =============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS fleet_analytics
COMMENT 'Fleet Analytics Data Lake'
LOCATION 's3://fleet-data-lake/';

-- =============================================================================
-- RAW LAYER - Landing zone for ingested data
-- =============================================================================

-- Raw GPS events
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.raw_gps_events (
    event_id STRING COMMENT 'Unique event identifier',
    vehicle_id STRING COMMENT 'Vehicle identifier',
    timestamp STRING COMMENT 'Event timestamp (ISO 8601)',
    latitude DOUBLE COMMENT 'GPS latitude',
    longitude DOUBLE COMMENT 'GPS longitude',
    speed_mph DOUBLE COMMENT 'Vehicle speed in MPH',
    heading INT COMMENT 'Vehicle heading (0-360 degrees)',
    fuel_level DOUBLE COMMENT 'Fuel level (0.0-1.0)',
    engine_status STRING COMMENT 'Engine status (ON, OFF, IDLE)',
    odometer_miles DOUBLE COMMENT 'Odometer reading in miles'
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/raw/gps/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY',
    'projection.enabled' = 'true',
    'projection.event_date.type' = 'date',
    'projection.event_date.range' = '2024-01-01,NOW',
    'projection.event_date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://fleet-data-lake/raw/gps/event_date=${event_date}'
);

-- Raw delivery events
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.raw_delivery_events (
    event_id STRING COMMENT 'Unique event identifier',
    delivery_id STRING COMMENT 'Delivery identifier',
    order_id STRING COMMENT 'Order identifier',
    vehicle_id STRING COMMENT 'Vehicle identifier',
    driver_id STRING COMMENT 'Driver identifier',
    event_type STRING COMMENT 'Event type (PICKED_UP, IN_TRANSIT, DELIVERED, FAILED)',
    timestamp STRING COMMENT 'Event timestamp (ISO 8601)',
    latitude DOUBLE COMMENT 'Event latitude',
    longitude DOUBLE COMMENT 'Event longitude',
    customer_id STRING COMMENT 'Customer identifier',
    package_weight_lbs DOUBLE COMMENT 'Package weight in pounds',
    estimated_delivery_time STRING COMMENT 'Estimated delivery time',
    actual_delivery_time STRING COMMENT 'Actual delivery time',
    delay_minutes INT COMMENT 'Delay in minutes (negative = early)',
    notes STRING COMMENT 'Delivery notes'
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/raw/deliveries/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY',
    'projection.enabled' = 'true',
    'projection.event_date.type' = 'date',
    'projection.event_date.range' = '2024-01-01,NOW',
    'projection.event_date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://fleet-data-lake/raw/deliveries/event_date=${event_date}'
);

-- =============================================================================
-- PROCESSED LAYER - Cleaned and enriched data
-- =============================================================================

-- Processed GPS events with derived columns
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.fact_gps_events (
    event_id STRING,
    vehicle_id STRING,
    event_timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_mph DOUBLE,
    heading INT,
    fuel_level DOUBLE,
    engine_status STRING,
    odometer_miles DOUBLE,
    -- Derived columns
    date_key INT COMMENT 'Date key (YYYYMMDD)',
    time_key INT COMMENT 'Time key (HHMM)',
    speed_category STRING COMMENT 'Speed bucket (STOPPED, SLOW, NORMAL, FAST)',
    fuel_status STRING COMMENT 'Fuel status (CRITICAL, LOW, MEDIUM, GOOD)',
    is_moving BOOLEAN COMMENT 'Vehicle is moving',
    geohash STRING COMMENT 'Geohash for location grouping',
    processed_at TIMESTAMP COMMENT 'ETL processing timestamp'
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/processed/gps/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY',
    'projection.enabled' = 'true',
    'projection.event_date.type' = 'date',
    'projection.event_date.range' = '2024-01-01,NOW',
    'projection.event_date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://fleet-data-lake/processed/gps/event_date=${event_date}'
);

-- Processed delivery events
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.fact_deliveries (
    event_id STRING,
    delivery_id STRING,
    order_id STRING,
    vehicle_id STRING,
    driver_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    customer_id STRING,
    package_weight_lbs DOUBLE,
    delay_minutes INT,
    -- Derived columns
    date_key INT,
    time_key INT,
    is_on_time BOOLEAN COMMENT 'Delivery was on time',
    is_successful BOOLEAN COMMENT 'Delivery was successful',
    delay_category STRING COMMENT 'Delay bucket',
    package_size STRING COMMENT 'Package size category',
    processed_at TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/processed/deliveries/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY',
    'projection.enabled' = 'true',
    'projection.event_date.type' = 'date',
    'projection.event_date.range' = '2024-01-01,NOW',
    'projection.event_date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://fleet-data-lake/processed/deliveries/event_date=${event_date}'
);

-- =============================================================================
-- AGGREGATED LAYER - Pre-computed metrics
-- =============================================================================

-- Daily fleet summary
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.agg_daily_summary (
    summary_date DATE COMMENT 'Summary date',
    total_gps_events BIGINT COMMENT 'Total GPS events',
    active_vehicles INT COMMENT 'Unique active vehicles',
    avg_speed DOUBLE COMMENT 'Average speed (MPH)',
    avg_fuel_level DOUBLE COMMENT 'Average fuel level',
    total_deliveries INT COMMENT 'Total deliveries',
    successful_deliveries INT COMMENT 'Successful deliveries',
    on_time_deliveries INT COMMENT 'On-time deliveries',
    avg_delay_minutes DOUBLE COMMENT 'Average delay (minutes)',
    active_drivers INT COMMENT 'Unique active drivers',
    on_time_delivery_pct DECIMAL(5,2) COMMENT 'On-time delivery percentage',
    delivery_success_rate DECIMAL(5,2) COMMENT 'Delivery success rate',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/aggregated/daily_summary/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY'
);

-- Hourly fleet metrics (for dashboards)
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.agg_hourly_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_events BIGINT,
    active_vehicles INT,
    avg_speed DOUBLE,
    median_speed DOUBLE,
    avg_fuel_level DOUBLE,
    critical_fuel_count INT,
    moving_vehicles INT,
    idle_vehicles INT,
    fleet_utilization_pct DECIMAL(5,2)
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/aggregated/hourly_metrics/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY'
);

-- Vehicle trip summaries
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.fact_vehicle_trips (
    vehicle_id STRING,
    trip_date DATE,
    gps_events INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    avg_speed_mph DOUBLE,
    max_speed_mph DOUBLE,
    min_fuel_level DOUBLE,
    max_fuel_level DOUBLE,
    idle_events INT,
    moving_events INT,
    duration_minutes DOUBLE,
    fuel_consumed DOUBLE,
    idle_time_pct DOUBLE
)
PARTITIONED BY (date_key INT)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/processed/vehicle_trips/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY'
);

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- Vehicle dimension
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.dim_vehicles (
    vehicle_id STRING,
    vehicle_type STRING,
    make STRING,
    model STRING,
    year INT,
    license_plate STRING,
    capacity_lbs DOUBLE,
    fuel_type STRING,
    home_depot STRING,
    status STRING,
    effective_start TIMESTAMP,
    effective_end TIMESTAMP,
    is_current BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/dimensions/vehicles/'
TBLPROPERTIES ('classification' = 'parquet');

-- Driver dimension
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.dim_drivers (
    driver_id STRING,
    driver_name STRING,
    license_number STRING,
    hire_date DATE,
    home_depot STRING,
    status STRING,
    rating DOUBLE,
    effective_start TIMESTAMP,
    effective_end TIMESTAMP,
    is_current BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/dimensions/drivers/'
TBLPROPERTIES ('classification' = 'parquet');

-- Date dimension
CREATE EXTERNAL TABLE IF NOT EXISTS fleet_analytics.dim_date (
    date_key INT,
    full_date DATE,
    day_of_week INT,
    day_name STRING,
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name STRING,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://fleet-data-lake/dimensions/date/'
TBLPROPERTIES ('classification' = 'parquet');

-- =============================================================================
-- PARTITION MANAGEMENT
-- =============================================================================

-- Repair partitions (run after new data arrives)
-- MSCK REPAIR TABLE fleet_analytics.raw_gps_events;
-- MSCK REPAIR TABLE fleet_analytics.raw_delivery_events;
-- MSCK REPAIR TABLE fleet_analytics.fact_gps_events;
-- MSCK REPAIR TABLE fleet_analytics.fact_deliveries;
-- MSCK REPAIR TABLE fleet_analytics.agg_daily_summary;

-- Add specific partition (more efficient than MSCK REPAIR)
-- ALTER TABLE fleet_analytics.fact_gps_events 
-- ADD IF NOT EXISTS PARTITION (event_date='2024-01-15');

-- =============================================================================
-- SAMPLE QUERIES
-- =============================================================================

-- Daily delivery performance
-- SELECT 
--     event_date,
--     COUNT(*) as total_deliveries,
--     SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time,
--     ROUND(AVG(delay_minutes), 2) as avg_delay,
--     ROUND(100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_pct
-- FROM fleet_analytics.fact_deliveries
-- WHERE event_date >= '2024-01-01'
-- GROUP BY event_date
-- ORDER BY event_date;

-- Fleet utilization by hour
-- SELECT 
--     date_format(event_timestamp, '%Y-%m-%d %H:00') as hour,
--     COUNT(DISTINCT vehicle_id) as active_vehicles,
--     ROUND(AVG(speed_mph), 2) as avg_speed,
--     SUM(CASE WHEN is_moving THEN 1 ELSE 0 END) as moving_count
-- FROM fleet_analytics.fact_gps_events
-- WHERE event_date = '2024-01-15'
-- GROUP BY date_format(event_timestamp, '%Y-%m-%d %H:00')
-- ORDER BY hour;
