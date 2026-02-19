-- Redshift Data Warehouse Schema for Fleet Analytics
-- Star Schema Design

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
DISTSTYLE ALL
SORTKEY (full_date);

-- Time Dimension
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT PRIMARY KEY,
    hour INT,
    minute INT,
    hour_12 INT,
    am_pm VARCHAR(2),
    time_of_day VARCHAR(20),  -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN
)
DISTSTYLE ALL;

-- Vehicle Dimension
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key INT IDENTITY(1,1) PRIMARY KEY,
    vehicle_id VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50),
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    capacity_lbs DECIMAL(10,2),
    fuel_type VARCHAR(20),
    license_plate VARCHAR(20),
    home_depot_id VARCHAR(20),
    status VARCHAR(20),
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE
)
DISTSTYLE KEY
DISTKEY (vehicle_id)
SORTKEY (vehicle_id, is_current);

-- Driver Dimension
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key INT IDENTITY(1,1) PRIMARY KEY,
    driver_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE,
    license_type VARCHAR(20),
    license_expiry DATE,
    home_depot_id VARCHAR(20),
    status VARCHAR(20),
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE
)
DISTSTYLE KEY
DISTKEY (driver_id)
SORTKEY (driver_id, is_current);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),  -- Residential, Business
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    signup_date DATE,
    is_current BOOLEAN DEFAULT TRUE
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id);

-- Location Dimension (Geographic Hierarchy)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    geohash VARCHAR(12),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    region VARCHAR(50),
    timezone VARCHAR(50)
)
DISTSTYLE ALL
SORTKEY (geohash);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact: Deliveries
CREATE TABLE IF NOT EXISTS fact_deliveries (
    delivery_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    delivery_id VARCHAR(30) NOT NULL,
    order_id VARCHAR(30) NOT NULL,
    
    -- Dimension Keys
    date_key INT REFERENCES dim_date(date_key),
    time_key INT REFERENCES dim_time(time_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    pickup_location_key INT REFERENCES dim_location(location_key),
    delivery_location_key INT REFERENCES dim_location(location_key),
    
    -- Delivery Metrics
    estimated_delivery_time TIMESTAMP,
    actual_delivery_time TIMESTAMP,
    delay_minutes INT,
    package_weight_lbs DECIMAL(10,2),
    package_count INT,
    
    -- Status
    delivery_status VARCHAR(30),
    is_on_time BOOLEAN,
    is_successful BOOLEAN,
    attempt_number INT,
    
    -- Timestamps
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (delivery_id)
SORTKEY (date_key, delivery_id);

-- Fact: Vehicle Trips
CREATE TABLE IF NOT EXISTS fact_vehicle_trips (
    trip_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    trip_id VARCHAR(30) NOT NULL,
    
    -- Dimension Keys
    date_key INT REFERENCES dim_date(date_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    start_location_key INT REFERENCES dim_location(location_key),
    end_location_key INT REFERENCES dim_location(location_key),
    
    -- Trip Metrics
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_minutes INT,
    distance_miles DECIMAL(10,2),
    fuel_consumed_gallons DECIMAL(10,2),
    
    -- Performance Metrics
    avg_speed_mph DECIMAL(5,2),
    max_speed_mph DECIMAL(5,2),
    idle_time_minutes INT,
    stop_count INT,
    deliveries_completed INT,
    
    -- Cost Metrics
    fuel_cost DECIMAL(10,2),
    cost_per_mile DECIMAL(10,4),
    
    -- Timestamps
    created_at TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (vehicle_key)
SORTKEY (date_key, trip_id);

-- Fact: Route Performance (Aggregated)
CREATE TABLE IF NOT EXISTS fact_route_performance (
    route_perf_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Dimension Keys
    date_key INT REFERENCES dim_date(date_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    
    -- Route Metrics
    total_distance_miles DECIMAL(10,2),
    total_duration_minutes INT,
    planned_stops INT,
    completed_stops INT,
    failed_stops INT,
    
    -- Efficiency Metrics
    route_efficiency_score DECIMAL(5,2),  -- 0-100
    on_time_percentage DECIMAL(5,2),
    avg_stop_duration_minutes DECIMAL(5,2),
    
    -- Cost Metrics
    total_fuel_cost DECIMAL(10,2),
    cost_per_delivery DECIMAL(10,2),
    cost_per_mile DECIMAL(10,4),
    
    -- Timestamps
    created_at TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (date_key)
SORTKEY (date_key, vehicle_key);

-- Fact: GPS Events (High Volume - Consider External Table)
CREATE TABLE IF NOT EXISTS fact_gps_events (
    gps_event_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    event_id VARCHAR(50) NOT NULL,
    
    -- Dimension Keys
    date_key INT REFERENCES dim_date(date_key),
    time_key INT REFERENCES dim_time(time_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    location_key INT REFERENCES dim_location(location_key),
    
    -- GPS Data
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    speed_mph DECIMAL(5,2),
    heading INT,
    
    -- Vehicle Status
    engine_status VARCHAR(10),
    fuel_level DECIMAL(5,3),
    odometer_miles DECIMAL(12,2),
    
    -- Timestamps
    event_timestamp TIMESTAMP,
    created_at TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (vehicle_key)
SORTKEY (date_key, event_timestamp)
-- Consider using Redshift Spectrum for this high-volume table
;

-- ============================================
-- AGGREGATE TABLES (Pre-computed for BI)
-- ============================================

-- Daily Fleet Summary
CREATE TABLE IF NOT EXISTS agg_daily_fleet_summary (
    summary_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    summary_date DATE,
    
    -- Fleet Metrics
    total_vehicles INT,
    active_vehicles INT,
    total_drivers INT,
    active_drivers INT,
    
    -- Delivery Metrics
    total_deliveries INT,
    successful_deliveries INT,
    failed_deliveries INT,
    on_time_deliveries INT,
    avg_delivery_time_minutes DECIMAL(10,2),
    
    -- Distance & Fuel
    total_miles_driven DECIMAL(12,2),
    total_fuel_consumed DECIMAL(10,2),
    avg_mpg DECIMAL(5,2),
    
    -- Cost Metrics
    total_fuel_cost DECIMAL(12,2),
    total_operational_cost DECIMAL(12,2),
    cost_per_delivery DECIMAL(10,2),
    cost_per_mile DECIMAL(10,4),
    
    -- Performance KPIs
    fleet_utilization_pct DECIMAL(5,2),
    on_time_delivery_pct DECIMAL(5,2),
    delivery_success_rate DECIMAL(5,2),
    avg_route_efficiency DECIMAL(5,2),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (summary_date);

-- ============================================
-- VIEWS FOR BI DASHBOARDS
-- ============================================

-- View: Current Fleet Status
CREATE OR REPLACE VIEW vw_current_fleet_status AS
SELECT 
    v.vehicle_id,
    v.vehicle_type,
    v.make,
    v.model,
    d.first_name || ' ' || d.last_name AS driver_name,
    g.latitude,
    g.longitude,
    g.speed_mph,
    g.engine_status,
    g.fuel_level,
    g.event_timestamp AS last_update
FROM dim_vehicle v
LEFT JOIN (
    SELECT vehicle_key, driver_key, latitude, longitude, speed_mph, 
           engine_status, fuel_level, event_timestamp,
           ROW_NUMBER() OVER (PARTITION BY vehicle_key ORDER BY event_timestamp DESC) as rn
    FROM fact_gps_events
    WHERE date_key = (SELECT MAX(date_key) FROM dim_date WHERE full_date <= CURRENT_DATE)
) g ON v.vehicle_key = g.vehicle_key AND g.rn = 1
LEFT JOIN dim_driver d ON g.driver_key = d.driver_key
WHERE v.is_current = TRUE;

-- View: Delivery Performance Dashboard
CREATE OR REPLACE VIEW vw_delivery_performance AS
SELECT 
    dd.full_date,
    dd.day_name,
    COUNT(*) AS total_deliveries,
    SUM(CASE WHEN fd.is_successful THEN 1 ELSE 0 END) AS successful_deliveries,
    SUM(CASE WHEN fd.is_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
    AVG(fd.delay_minutes) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN fd.is_on_time THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2) AS on_time_pct,
    ROUND(SUM(CASE WHEN fd.is_successful THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2) AS success_rate
FROM fact_deliveries fd
JOIN dim_date dd ON fd.date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - 30
GROUP BY dd.full_date, dd.day_name
ORDER BY dd.full_date;

-- View: Driver Leaderboard
CREATE OR REPLACE VIEW vw_driver_leaderboard AS
SELECT 
    d.driver_id,
    d.first_name || ' ' || d.last_name AS driver_name,
    COUNT(fd.delivery_key) AS total_deliveries,
    SUM(CASE WHEN fd.is_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
    ROUND(AVG(fd.delay_minutes), 2) AS avg_delay,
    ROUND(SUM(CASE WHEN fd.is_on_time THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS on_time_pct
FROM dim_driver d
JOIN fact_deliveries fd ON d.driver_key = fd.driver_key
JOIN dim_date dd ON fd.date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - 7
  AND d.is_current = TRUE
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY on_time_pct DESC, total_deliveries DESC;
