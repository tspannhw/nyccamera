-- NYC Camera Snowflake Setup Script
-- ==================================
-- Run this script to set up the required Snowflake objects

-- Create database and schema
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DEMO;

-- Create standard table for NYC Camera data
CREATE OR REPLACE TABLE DEMO.DEMO.NYC_CAMERA_DATA (
    uuid STRING,
    camera_id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    direction_of_travel STRING,
    roadway_name STRING,
    video_url STRING,
    image_url STRING,
    disabled BOOLEAN,
    blocked BOOLEAN,
    image_timestamp TIMESTAMP_NTZ,
    ingest_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    hostname STRING,
    ip_address STRING
);

-- Create Iceberg table (requires external volume)
-- Update EXTERNAL_VOLUME to your volume name
CREATE OR REPLACE ICEBERG TABLE DEMO.DEMO.NYC_CAMERA_ICEBERG (
    uuid STRING,
    camera_id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    direction_of_travel STRING,
    roadway_name STRING,
    video_url STRING,
    image_url STRING,
    disabled BOOLEAN,
    blocked BOOLEAN,
    image_timestamp TIMESTAMP_NTZ,
    ingest_timestamp TIMESTAMP_NTZ,
    hostname STRING,
    ip_address STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'  -- Update this to your volume
BASE_LOCATION = 'nyc_camera/';

-- Create stage for camera images
CREATE OR REPLACE STAGE DEMO.DEMO.NYC_CAMERA_IMAGES
    DIRECTORY = (ENABLE = TRUE);

-- Create stored procedure for Cortex AI image analysis
CREATE OR REPLACE PROCEDURE DEMO.DEMO.ANALYZE_NYC_CAMERA_IMAGE(IMAGE_NAME STRING)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
    result VARIANT;
BEGIN
    ALTER STAGE DEMO.DEMO.NYC_CAMERA_IMAGES REFRESH;
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
        'Analyze this traffic camera image. Describe traffic conditions, weather, visibility, and any notable observations. Respond in JSON format.',
        TO_FILE('@DEMO.DEMO.NYC_CAMERA_IMAGES', :IMAGE_NAME)) INTO :result;
    
    RETURN result;
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('error', SQLSTATE || ' - ' || SQLERRM);
END;
$$;

-- Create view to sync data to Iceberg table
CREATE OR REPLACE TASK DEMO.DEMO.SYNC_CAMERA_TO_ICEBERG
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
AS
INSERT INTO DEMO.DEMO.NYC_CAMERA_ICEBERG
SELECT * FROM DEMO.DEMO.NYC_CAMERA_DATA
WHERE ingest_timestamp >= DATEADD('minute', -6, CURRENT_TIMESTAMP())
AND uuid NOT IN (SELECT uuid FROM DEMO.DEMO.NYC_CAMERA_ICEBERG);

-- Sample queries

-- Total camera count
SELECT COUNT(DISTINCT camera_id) as total_cameras
FROM DEMO.DEMO.NYC_CAMERA_DATA;

-- Cameras by roadway
SELECT roadway_name, COUNT(DISTINCT camera_id) as camera_count
FROM DEMO.DEMO.NYC_CAMERA_DATA
WHERE roadway_name IS NOT NULL
GROUP BY roadway_name
ORDER BY camera_count DESC
LIMIT 20;

-- Recent captures
SELECT camera_id, name, roadway_name, direction_of_travel, 
       latitude, longitude, image_timestamp
FROM DEMO.DEMO.NYC_CAMERA_DATA
WHERE image_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
QUALIFY ROW_NUMBER() OVER (PARTITION BY camera_id ORDER BY image_timestamp DESC) = 1
ORDER BY image_timestamp DESC;

-- Cameras by direction
SELECT direction_of_travel, COUNT(DISTINCT camera_id) as camera_count
FROM DEMO.DEMO.NYC_CAMERA_DATA
GROUP BY direction_of_travel
ORDER BY camera_count DESC;

-- Hourly capture pattern
SELECT DATE_TRUNC('hour', image_timestamp) as capture_hour,
       COUNT(*) as capture_count,
       COUNT(DISTINCT camera_id) as active_cameras
FROM DEMO.DEMO.NYC_CAMERA_DATA
WHERE image_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY capture_hour
ORDER BY capture_hour;

-- Check Snowpipe Streaming channel status
SHOW CHANNELS IN SCHEMA DEMO.DEMO;

-- Check pipe status
SHOW PIPES IN SCHEMA DEMO.DEMO;
