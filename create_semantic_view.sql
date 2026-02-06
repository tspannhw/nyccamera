-- ============================================================================
-- NYC CAMERA DATA - SEMANTIC VIEW DEPLOYMENT
-- ============================================================================
-- Creates a Snowflake Semantic View from YAML specification
-- Enables natural language queries via Cortex Analyst
-- ============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;

-- ============================================================================
-- OPTION 1: CREATE FROM YAML FILE (Recommended)
-- ============================================================================
-- First, upload the YAML file to a stage, then create the semantic view

-- Create a stage for semantic model files if it doesn't exist
CREATE STAGE IF NOT EXISTS DEMO.DEMO.SEMANTIC_MODELS;

-- Upload the YAML file using SnowSQL or Snowsight:
-- PUT file:///path/to/nyc_camera_semantic_view.yaml @DEMO.DEMO.SEMANTIC_MODELS;

-- Create semantic view from staged YAML file:
-- CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
-- FROM '@DEMO.DEMO.SEMANTIC_MODELS/nyc_camera_semantic_view.yaml';

-- ============================================================================
-- OPTION 2: CREATE USING SQL DDL (Current Approach)
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
TABLES (
    CAMERAS AS (
        SELECT
            uuid,
            camera_id,
            name,
            roadway_name,
            direction_of_travel,
            latitude,
            longitude,
            video_url,
            image_url,
            disabled,
            blocked,
            hostname,
            ip_address,
            image_timestamp,
            ingest_timestamp
        FROM DEMO.DEMO.NYC_CAMERA_DATA
    )
    PRIMARY KEY (uuid)
    
    -- Dimensions (categorical/descriptive columns)
    DIMENSION camera_id
        COMMENT 'Unique identifier for the camera from 511NY'
        SYNONYMS ('camera identifier', 'cam id')
    
    DIMENSION camera_name AS name
        COMMENT 'Human-readable name of the camera, usually includes location'
        SYNONYMS ('name', 'location', 'camera location')
    
    DIMENSION roadway AS roadway_name
        COMMENT 'Name of the roadway where camera is located (e.g., I-495, BQE)'
        SYNONYMS ('roadway', 'highway', 'road', 'route', 'expressway')
    
    DIMENSION direction AS direction_of_travel
        COMMENT 'Direction the camera faces (Northbound, Southbound, Eastbound, Westbound)'
        SYNONYMS ('direction', 'facing', 'travel direction')
    
    DIMENSION video_link AS video_url
        COMMENT 'URL to the live video stream'
    
    DIMENSION image_link AS image_url
        COMMENT 'URL to the camera snapshot image'
    
    DIMENSION is_disabled AS disabled
        COMMENT 'Whether the camera is currently disabled'
        SYNONYMS ('disabled', 'offline')
    
    DIMENSION is_blocked AS blocked
        COMMENT 'Whether the camera view is blocked'
        SYNONYMS ('blocked', 'obstructed')
    
    DIMENSION server_hostname AS hostname
        COMMENT 'Hostname of the ingestion server'
    
    DIMENSION server_ip AS ip_address
        COMMENT 'IP address of the ingestion server'
    
    DIMENSION captured_at AS image_timestamp
        COMMENT 'Timestamp when the camera image was captured'
        SYNONYMS ('capture time', 'captured', 'when captured', 'snapshot time')
    
    DIMENSION ingested_at AS ingest_timestamp
        COMMENT 'Timestamp when the record was ingested into Snowflake'
        SYNONYMS ('ingested', 'loaded', 'when loaded')
    
    -- Facts (numeric columns)
    FACT latitude
        COMMENT 'Latitude coordinate of camera location'
    
    FACT longitude
        COMMENT 'Longitude coordinate of camera location'
    
    -- Metrics (aggregations)
    METRIC record_count AS COUNT(uuid)
        COMMENT 'Total number of camera capture records'
        SYNONYMS ('record count', 'captures', 'total captures')
    
    METRIC camera_count AS COUNT(DISTINCT camera_id)
        COMMENT 'Number of unique cameras'
        SYNONYMS ('camera count', 'number of cameras', 'distinct cameras')
    
    METRIC roadway_count AS COUNT(DISTINCT roadway)
        COMMENT 'Number of unique roadways'
        SYNONYMS ('roadway count', 'number of roadways')
);

-- ============================================================================
-- ADD VERIFIED QUERIES (VQRs)
-- ============================================================================

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'How many cameras are there?'
AS 'SELECT COUNT(DISTINCT camera_id) as camera_count FROM DEMO.DEMO.NYC_CAMERA_DATA';

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'How many cameras are on each roadway?'
AS 'SELECT roadway_name, COUNT(DISTINCT camera_id) as camera_count FROM DEMO.DEMO.NYC_CAMERA_DATA WHERE roadway_name IS NOT NULL GROUP BY roadway_name ORDER BY camera_count DESC';

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'What are the most recent camera captures?'
AS 'SELECT camera_id, name, roadway_name, image_timestamp FROM DEMO.DEMO.NYC_CAMERA_DATA ORDER BY image_timestamp DESC LIMIT 20';

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'How many cameras face each direction?'
AS 'SELECT direction_of_travel, COUNT(DISTINCT camera_id) as camera_count FROM DEMO.DEMO.NYC_CAMERA_DATA WHERE direction_of_travel IS NOT NULL GROUP BY direction_of_travel ORDER BY camera_count DESC';

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'What cameras are on I-495?'
AS 'SELECT DISTINCT camera_id, name, direction_of_travel, latitude, longitude FROM DEMO.DEMO.NYC_CAMERA_DATA WHERE roadway_name LIKE ''%I-495%'' OR roadway_name LIKE ''%495%''';

ALTER SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
ADD VERIFIED QUERY 'How many cameras were active in the last hour?'
AS 'SELECT COUNT(DISTINCT camera_id) as active_cameras FROM DEMO.DEMO.NYC_CAMERA_DATA WHERE image_timestamp >= DATEADD(''hour'', -1, CURRENT_TIMESTAMP())';

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT USAGE ON SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW TO ROLE PUBLIC;

-- ============================================================================
-- VERIFY DEPLOYMENT
-- ============================================================================

DESCRIBE SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW;

-- ============================================================================
-- TEST QUERIES WITH CORTEX ANALYST
-- ============================================================================

/*
-- Test natural language queries:

-- Basic count
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW',
    'How many unique cameras are there?'
);

-- Grouping
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW',
    'Show me camera count by roadway'
);

-- Filtering
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW',
    'Find all cameras on the BQE'
);

-- Time-based
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW',
    'What cameras captured images today?'
);
*/

SELECT 'NYC Camera Semantic View ready for Cortex Analyst!' AS status;
