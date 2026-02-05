-- =============================================================================
-- NYC Traffic Analytics - SQL Semantic Views
-- =============================================================================
-- Blade Runner Surveillance System - Semantic Layer
-- Data streamed via Snowpipe Streaming High Speed v2 REST API
-- =============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;

-- -----------------------------------------------------------------------------
-- SEMANTIC VIEW 1: NYC Camera Surveillance Data
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW NYC_CAMERA_SEMANTIC_VIEW

  TABLES (
    cameras AS DEMO.DEMO.NYC_CAMERA_DATA
      PRIMARY KEY (uuid)
      WITH SYNONYMS ('traffic cameras', 'surveillance', 'camera feeds')
      COMMENT = 'NYC traffic camera data from 511NY API streamed via Snowpipe v2'
  )

  DIMENSIONS (
    cameras.camera_id AS camera_id
      WITH SYNONYMS = ('cam id', 'camera identifier')
      COMMENT = 'Unique camera identifier from 511NY',
    cameras.camera_name AS name
      WITH SYNONYMS = ('location', 'camera location', 'name')
      COMMENT = 'Human-readable camera name and location',
    cameras.roadway AS roadway_name
      WITH SYNONYMS = ('highway', 'road', 'route', 'expressway')
      COMMENT = 'Name of the roadway (e.g., I-495, BQE)',
    cameras.direction AS direction_of_travel
      WITH SYNONYMS = ('facing', 'travel direction')
      COMMENT = 'Direction camera faces (Northbound, Southbound, etc.)',
    cameras.latitude AS latitude
      COMMENT = 'GPS latitude coordinate',
    cameras.longitude AS longitude
      COMMENT = 'GPS longitude coordinate',
    cameras.capture_date AS DATE(image_timestamp)
      COMMENT = 'Date of camera capture',
    cameras.capture_hour AS DATE_TRUNC('hour', image_timestamp)
      COMMENT = 'Hour of camera capture',
    cameras.is_disabled AS disabled
      COMMENT = 'Whether camera is disabled',
    cameras.is_blocked AS blocked
      COMMENT = 'Whether camera view is blocked'
  )

  METRICS (
    cameras.total_records AS COUNT(*)
      WITH SYNONYMS = ('record count', 'captures', 'total captures')
      COMMENT = 'Total number of camera capture records',
    cameras.unique_cameras AS COUNT(DISTINCT camera_id)
      WITH SYNONYMS = ('camera count', 'number of cameras')
      COMMENT = 'Number of unique cameras',
    cameras.unique_roadways AS COUNT(DISTINCT roadway_name)
      WITH SYNONYMS = ('roadway count', 'number of roadways')
      COMMENT = 'Number of unique roadways',
    cameras.avg_latitude AS AVG(latitude)
      COMMENT = 'Average latitude of cameras',
    cameras.avg_longitude AS AVG(longitude)
      COMMENT = 'Average longitude of cameras'
  )

  COMMENT = 'Semantic view for NYC traffic camera surveillance data';


-- -----------------------------------------------------------------------------
-- SEMANTIC VIEW 2: NYC Traffic Events (Incidents & Construction)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW

  TABLES (
    events AS DEMO.DEMO.NYC_TRAFFIC_EVENTS
      PRIMARY KEY (uuid)
      WITH SYNONYMS ('traffic incidents', 'accidents', 'construction', 'road events')
      COMMENT = 'NYC traffic events including incidents and construction from 511NY API'
  )

  DIMENSIONS (
    events.event_id AS event_id
      WITH SYNONYMS = ('incident id', 'event identifier')
      COMMENT = 'Unique event identifier from 511NY',
    events.event_type AS event_type
      WITH SYNONYMS = ('incident type', 'type')
      COMMENT = 'Type of event (incident, construction, special event)',
    events.event_subtype AS event_subtype
      WITH SYNONYMS = ('subtype', 'category')
      COMMENT = 'Subtype of the traffic event',
    events.severity_level AS severity
      WITH SYNONYMS = ('impact', 'severity')
      COMMENT = 'Severity level of the event',
    events.roadway AS roadway_name
      WITH SYNONYMS = ('highway', 'road', 'route')
      COMMENT = 'Name of affected roadway',
    events.direction AS direction
      WITH SYNONYMS = ('travel direction')
      COMMENT = 'Direction of affected traffic',
    events.description_text AS description
      WITH SYNONYMS = ('details', 'info')
      COMMENT = 'Detailed description of the event',
    events.primary_location AS location
      WITH SYNONYMS = ('address')
      COMMENT = 'Primary location description',
    events.latitude AS latitude
      COMMENT = 'GPS latitude coordinate',
    events.longitude AS longitude
      COMMENT = 'GPS longitude coordinate',
    events.event_date AS DATE(event_timestamp)
      WITH SYNONYMS = ('date', 'incident date')
      COMMENT = 'Date of the event',
    events.event_hour AS DATE_TRUNC('hour', event_timestamp)
      COMMENT = 'Hour of the event'
  )

  METRICS (
    events.total_events AS COUNT(*)
      WITH SYNONYMS = ('event count', 'total incidents')
      COMMENT = 'Total number of traffic events',
    events.unique_events AS COUNT(DISTINCT event_id)
      WITH SYNONYMS = ('distinct events')
      COMMENT = 'Number of unique events'
  )

  COMMENT = 'Semantic view for NYC traffic events including incidents and construction';


-- -----------------------------------------------------------------------------
-- SEMANTIC VIEW 3: NYC Traffic Speeds & Congestion
-- -----------------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW

  TABLES (
    speeds AS DEMO.DEMO.NYC_TRAFFIC_SPEEDS
      PRIMARY KEY (uuid)
      WITH SYNONYMS ('traffic flow', 'speed data', 'congestion')
      COMMENT = 'Real-time traffic speed data from 511NY API'
  )

  FACTS (
    speeds.speed AS current_speed
      WITH SYNONYMS = ('velocity', 'mph')
      COMMENT = 'Current traffic speed in mph',
    speeds.reference_speed AS free_flow_speed
      WITH SYNONYMS = ('max speed', 'optimal speed')
      COMMENT = 'Free flow speed under normal conditions',
    speeds.segment_travel_time AS travel_time
      WITH SYNONYMS = ('duration')
      COMMENT = 'Travel time for segment in seconds',
    speeds.congestion_ratio AS current_speed / NULLIF(free_flow_speed, 0)
      COMMENT = 'Ratio of current to free flow speed (1.0 = normal)'
  )

  DIMENSIONS (
    speeds.segment_id AS segment_id
      WITH SYNONYMS = ('segment', 'road segment')
      COMMENT = 'Road segment identifier',
    speeds.link_id AS link_id
      WITH SYNONYMS = ('link', 'link identifier')
      COMMENT = 'Link identifier for the segment',
    speeds.roadway AS roadway_name
      WITH SYNONYMS = ('highway', 'road', 'route')
      COMMENT = 'Name of the roadway',
    speeds.direction AS direction
      WITH SYNONYMS = ('travel direction', 'heading')
      COMMENT = 'Direction of traffic flow',
    speeds.start_location AS from_location
      WITH SYNONYMS = ('from', 'start')
      COMMENT = 'Starting location of segment',
    speeds.end_location AS to_location
      WITH SYNONYMS = ('to', 'end')
      COMMENT = 'Ending location of segment',
    speeds.traffic_date AS DATE(traffic_timestamp)
      WITH SYNONYMS = ('date')
      COMMENT = 'Date of speed measurement',
    speeds.traffic_hour AS DATE_TRUNC('hour', traffic_timestamp)
      COMMENT = 'Hour of speed measurement'
  )

  METRICS (
    speeds.total_readings AS COUNT(*)
      WITH SYNONYMS = ('reading count', 'measurements')
      COMMENT = 'Total number of speed readings',
    speeds.avg_current_speed AS AVG(current_speed)
      WITH SYNONYMS = ('average speed')
      COMMENT = 'Average current speed across segments',
    speeds.avg_free_flow AS AVG(free_flow_speed)
      COMMENT = 'Average free flow speed',
    speeds.min_speed AS MIN(current_speed)
      WITH SYNONYMS = ('slowest')
      COMMENT = 'Minimum recorded speed',
    speeds.max_speed AS MAX(current_speed)
      WITH SYNONYMS = ('fastest')
      COMMENT = 'Maximum recorded speed',
    speeds.avg_travel_time AS AVG(travel_time)
      COMMENT = 'Average travel time',
    speeds.avg_congestion AS AVG(speeds.congestion_ratio)
      WITH SYNONYMS = ('congestion level')
      COMMENT = 'Average congestion ratio'
  )

  COMMENT = 'Semantic view for real-time NYC traffic speeds and congestion analysis';


-- -----------------------------------------------------------------------------
-- Verify Semantic Views
-- -----------------------------------------------------------------------------
SHOW SEMANTIC VIEWS IN SCHEMA DEMO.DEMO;

-- Show dimensions and metrics for each semantic view
SHOW SEMANTIC DIMENSIONS IN NYC_CAMERA_SEMANTIC_VIEW;
SHOW SEMANTIC METRICS IN NYC_CAMERA_SEMANTIC_VIEW;

SHOW SEMANTIC DIMENSIONS IN NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW;
SHOW SEMANTIC METRICS IN NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW;

SHOW SEMANTIC DIMENSIONS IN NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW;
SHOW SEMANTIC FACTS IN NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW;
SHOW SEMANTIC METRICS IN NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW;


-- =============================================================================
-- Example Queries Using Semantic Views
-- =============================================================================

-- Query camera semantic view
SELECT * FROM SEMANTIC_VIEW(
  NYC_CAMERA_SEMANTIC_VIEW
  DIMENSIONS cameras.roadway, cameras.direction
  METRICS cameras.unique_cameras, cameras.total_records
) ORDER BY unique_cameras DESC LIMIT 10;

-- Query traffic events semantic view  
SELECT * FROM SEMANTIC_VIEW(
  NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW
  DIMENSIONS events.event_type, events.roadway
  METRICS events.total_events, events.unique_events
) ORDER BY total_events DESC LIMIT 10;

-- Query traffic speeds semantic view
SELECT * FROM SEMANTIC_VIEW(
  NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW
  DIMENSIONS speeds.roadway, speeds.direction
  METRICS speeds.avg_current_speed, speeds.avg_congestion, speeds.total_readings
) ORDER BY avg_congestion ASC LIMIT 20;
