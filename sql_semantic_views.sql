-- ============================================================================
-- NYC TRAFFIC DATA PLATFORM - SQL SEMANTIC VIEWS, CORTEX SEARCH, AGENT & MCP
-- ============================================================================
-- Complete deployment script for NYC Traffic Intelligence Platform
-- Created: 2026-02-05
-- ============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;

-- ============================================================================
-- 1. NYC CAMERA SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW
  TABLES (
    cameras AS DEMO.DEMO.NYC_CAMERA_DATA PRIMARY KEY (uuid)
  )
  FACTS (
    cameras.uuid AS uuid,
    cameras.latitude AS latitude,
    cameras.longitude AS longitude
  )
  DIMENSIONS (
    cameras.camera_id AS camera_id,
    cameras.camera_name AS name,
    cameras.roadway AS roadway_name,
    cameras.direction AS direction_of_travel,
    cameras.video_link AS video_url,
    cameras.image_link AS image_url,
    cameras.is_disabled AS disabled,
    cameras.is_blocked AS blocked,
    cameras.captured_at AS image_timestamp,
    cameras.ingested_at AS ingest_timestamp,
    cameras.server_hostname AS hostname,
    cameras.server_ip AS ip_address
  )
  METRICS (
    cameras.camera_count AS COUNT(DISTINCT cameras.camera_id),
    cameras.record_count AS COUNT(cameras.uuid),
    cameras.roadway_count AS COUNT(DISTINCT cameras.roadway)
  );

-- ============================================================================
-- 2. NYC WEATHER SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_WEATHER_SEMANTIC_VIEW
  TABLES (
    weather AS DEMO.DEMO.NOAAWEATHER
  )
  FACTS (
    weather.temp_f AS temp_f,
    weather.temp_c AS temp_c,
    weather.relative_humidity AS relative_humidity,
    weather.wind_mph AS wind_mph,
    weather.wind_degrees AS wind_degrees,
    weather.visibility_mi AS visibility_mi,
    weather.pressure_in AS pressure_in,
    weather.dewpoint_f AS dewpoint_f,
    weather.latitude AS latitude,
    weather.longitude AS longitude
  )
  DIMENSIONS (
    weather.station_id AS station_id,
    weather.station_location AS location,
    weather.conditions AS weather,
    weather.wind_direction AS wind_dir,
    weather.temperature_display AS temperature_string,
    weather.dewpoint_display AS dewpoint_string,
    weather.wind_display AS wind_string,
    weather.observation_time AS observation_time,
    weather.last_updated AS updated_at
  )
  METRICS (
    weather.avg_temperature AS AVG(weather.temp_f),
    weather.avg_wind_speed AS AVG(weather.wind_mph),
    weather.avg_visibility AS AVG(weather.visibility_mi),
    weather.station_count AS COUNT(DISTINCT weather.station_id)
  );

-- ============================================================================
-- 3. NYC AIR QUALITY SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_AIR_QUALITY_SEMANTIC_VIEW
  TABLES (
    aq AS DEMO.DEMO.AQ
  )
  FACTS (
    aq.aqi AS aqi,
    aq.latitude AS latitude,
    aq.longitude AS longitude
  )
  DIMENSIONS (
    aq.date_observed AS dateobserved,
    aq.hour_observed AS hourobserved,
    aq.timezone AS localtimezone,
    aq.reporting_area AS reportingarea,
    aq.state AS statecode,
    aq.pollutant AS parametername,
    aq.category_number AS categorynumber,
    aq.category AS categoryname
  )
  METRICS (
    aq.avg_aqi AS AVG(aq.aqi),
    aq.max_aqi AS MAX(aq.aqi),
    aq.min_aqi AS MIN(aq.aqi),
    aq.reading_count AS COUNT(aq.aqi)
  );

-- ============================================================================
-- 4. NYC TRAFFIC EVENTS SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW
  TABLES (
    events AS DEMO.DEMO.NYC_TRAFFIC_EVENTS PRIMARY KEY (uuid)
  )
  FACTS (
    events.uuid AS uuid,
    events.latitude AS latitude,
    events.longitude AS longitude
  )
  DIMENSIONS (
    events.event_id AS event_id,
    events.event_type AS event_type,
    events.event_subtype AS event_subtype,
    events.severity AS severity,
    events.roadway AS roadway_name,
    events.direction AS direction,
    events.details AS description,
    events.area AS location,
    events.start_date AS start_date,
    events.planned_end AS planned_end_date,
    events.updated AS last_updated,
    events.event_time AS event_timestamp,
    events.ingested_at AS ingest_timestamp
  )
  METRICS (
    events.event_count AS COUNT(events.uuid),
    events.roadway_count AS COUNT(DISTINCT events.roadway)
  );

-- ============================================================================
-- 5. NYC TRAFFIC SPEEDS SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW
  TABLES (
    speeds AS DEMO.DEMO.NYC_TRAFFIC_SPEEDS PRIMARY KEY (uuid)
  )
  FACTS (
    speeds.uuid AS uuid,
    speeds.current_speed AS current_speed,
    speeds.free_flow_speed AS free_flow_speed,
    speeds.travel_time AS travel_time
  )
  DIMENSIONS (
    speeds.segment_id AS segment_id,
    speeds.link_id AS link_id,
    speeds.roadway AS roadway_name,
    speeds.direction AS direction,
    speeds.from_location AS from_location,
    speeds.to_location AS to_location,
    speeds.data_as_of AS data_as_of,
    speeds.measured_at AS traffic_timestamp,
    speeds.ingested_at AS ingest_timestamp
  )
  METRICS (
    speeds.avg_current_speed AS AVG(speeds.current_speed),
    speeds.avg_free_flow_speed AS AVG(speeds.free_flow_speed),
    speeds.avg_travel_time AS AVG(speeds.travel_time),
    speeds.segment_count AS COUNT(DISTINCT speeds.segment_id)
  );

-- ============================================================================
-- 6. NYC TRANSIT ALERTS SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW DEMO.DEMO.NYC_TRANSIT_ALERTS_SEMANTIC_VIEW
  TABLES (
    alerts AS DEMO.DEMO.TRANSIT_ALERTS PRIMARY KEY (uuid)
  )
  FACTS (
    alerts.uuid AS uuid
  )
  DIMENSIONS (
    alerts.alert_title AS title,
    alerts.alert_description AS description,
    alerts.alert_link AS link,
    alerts.alert_guid AS guid,
    alerts.published_date AS pubdate,
    alerts.service_type AS servicename,
    alerts.transit_agency AS companyname
  )
  METRICS (
    alerts.alert_count AS COUNT(alerts.uuid),
    alerts.service_count AS COUNT(DISTINCT alerts.service_type)
  );

-- ============================================================================
-- 7. RAG TABLE FOR CORTEX SEARCH (44K+ records)
-- ============================================================================

CREATE OR REPLACE TABLE DEMO.DEMO.NYC_ALERTS_RAG AS
SELECT 
    uuid,
    'transit_alert' AS alert_type,
    title,
    description AS content,
    link AS source_url,
    pubdate AS published_date,
    servicename AS category,
    companyname AS source_name,
    NULL AS severity,
    CURRENT_TIMESTAMP() AS indexed_at
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE title IS NOT NULL AND description IS NOT NULL
UNION ALL
SELECT 
    UUID_STRING() AS uuid,
    'travel_advisory' AS alert_type,
    title,
    description AS content,
    link AS source_url,
    published AS published_date,
    category,
    author AS source_name,
    CASE 
        WHEN category LIKE '%Level 4%' THEN 'Critical'
        WHEN category LIKE '%Level 3%' THEN 'High'
        WHEN category LIKE '%Level 2%' THEN 'Moderate'
        ELSE 'Low'
    END AS severity,
    CURRENT_TIMESTAMP() AS indexed_at
FROM DEMO.DEMO.TRAVELADVISORIES
WHERE title IS NOT NULL AND description IS NOT NULL
UNION ALL
SELECT
    uuid,
    'service_alert' AS alert_type,
    CONCAT(alerttext, ' - ', routeid) AS title,
    CONCAT(descriptiontext, ' Route: ', routeid, ' Cause: ', cause, ' Effect: ', effect) AS content,
    NULL AS source_url,
    activeperiodstart AS published_date,
    cause AS category,
    'NYC MTA' AS source_name,
    effect AS severity,
    CURRENT_TIMESTAMP() AS indexed_at
FROM DEMO.DEMO.SERVICEALERTSNYC
WHERE alerttext IS NOT NULL OR descriptiontext IS NOT NULL;

-- ============================================================================
-- 8. CORTEX SEARCH SERVICE
-- ============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE DEMO.DEMO.NYC_ALERTS_SEARCH
  ON content
  ATTRIBUTES alert_type, category, severity, source_name
  WAREHOUSE = INGEST
  TARGET_LAG = '1 hour'
  AS (
    SELECT 
      uuid,
      alert_type,
      title,
      content,
      source_url,
      published_date,
      category,
      source_name,
      severity
    FROM DEMO.DEMO.NYC_ALERTS_RAG
    WHERE content IS NOT NULL AND LENGTH(content) > 10
  );

-- ============================================================================
-- 9. CORTEX AGENT
-- ============================================================================

CREATE OR REPLACE AGENT DEMO.DEMO.NYC_TRAFFIC_INTELLIGENCE_AGENT
  COMMENT = 'NYC Traffic Intelligence Agent - Combines traffic cameras, events, speeds, weather, air quality data with alert search'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are the NYC Traffic Intelligence Agent. Use the appropriate tools based on the query type: Use traffic_camera_data for camera locations and roadway coverage. Use traffic_events_data for incidents, construction, and closures. Use traffic_speeds_data for current speeds and congestion analysis. Use weather_data for weather conditions affecting travel. Use air_quality_data for AQI and pollution levels. Use alert_search for searching transit alerts, service disruptions, and travel advisories. Always provide actionable insights for commuters and travelers.",
      "response": "Provide clear, concise responses with relevant data. Include specific roadways, locations, and times when available. For alerts, summarize key impacts on travel. Use tables or lists for multiple results."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "traffic_camera_data",
          "description": "Query NYC traffic camera data - camera locations, roadways, coverage, and capture statistics"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "traffic_events_data",
          "description": "Query NYC traffic events - incidents, construction, road closures, severity, and affected roadways"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "traffic_speeds_data",
          "description": "Query NYC traffic speed data - current speeds, free flow speeds, congestion ratios, and travel times"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "weather_data",
          "description": "Query weather conditions - temperature, wind, visibility, precipitation from NOAA weather stations"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "air_quality_data",
          "description": "Query air quality data - AQI values, pollutant types, health categories from EPA AirNow"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_search",
          "name": "alert_search",
          "description": "Search transit alerts, service disruptions, and travel advisories using natural language"
        }
      }
    ],
    "tool_resources": {
      "traffic_camera_data": {
        "semantic_view": "DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "INGEST"
        }
      },
      "traffic_events_data": {
        "semantic_view": "DEMO.DEMO.NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "INGEST"
        }
      },
      "traffic_speeds_data": {
        "semantic_view": "DEMO.DEMO.NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "INGEST"
        }
      },
      "weather_data": {
        "semantic_view": "DEMO.DEMO.NYC_WEATHER_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "INGEST"
        }
      },
      "air_quality_data": {
        "semantic_view": "DEMO.DEMO.NYC_AIR_QUALITY_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "INGEST"
        }
      },
      "alert_search": {
        "search_service": "DEMO.DEMO.NYC_ALERTS_SEARCH",
        "max_results": 10,
        "columns": ["title", "content", "category", "alert_type", "severity", "source_name"]
      }
    }
  }
  $$;

-- ============================================================================
-- 10. SNOWFLAKE MANAGED MCP SERVER
-- ============================================================================

CREATE OR REPLACE MCP SERVER DEMO.DEMO.NYC_TRAFFIC_MCP_SERVER
FROM SPECIFICATION $$
tools:
  - name: "traffic-camera-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_CAMERA_SEMANTIC_VIEW"
    title: "Traffic Camera Data Analyst"
    description: "Query NYC traffic camera data including camera locations, roadways, directions, and capture statistics."

  - name: "traffic-events-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW"
    title: "Traffic Events Analyst"
    description: "Query NYC traffic events including incidents, construction, road closures, and their severity."

  - name: "traffic-speeds-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW"
    title: "Traffic Speeds Analyst"
    description: "Query NYC traffic speed data including current speeds, free flow speeds, congestion ratios."

  - name: "weather-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_WEATHER_SEMANTIC_VIEW"
    title: "Weather Conditions Analyst"
    description: "Query NOAA weather station data including temperature, wind speed, visibility, and conditions."

  - name: "air-quality-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_AIR_QUALITY_SEMANTIC_VIEW"
    title: "Air Quality Analyst"
    description: "Query EPA Air Quality Index data including AQI values, pollutant types, and health categories."

  - name: "transit-alerts-analyst"
    type: "CORTEX_ANALYST_MESSAGE"
    identifier: "DEMO.DEMO.NYC_TRANSIT_ALERTS_SEMANTIC_VIEW"
    title: "Transit Alerts Analyst"
    description: "Query transit service alerts including NJ Transit bus, rail, and light rail notifications."

  - name: "alerts-search"
    type: "CORTEX_SEARCH_SERVICE_QUERY"
    identifier: "DEMO.DEMO.NYC_ALERTS_SEARCH"
    title: "Alert Search"
    description: "Search transit alerts, service disruptions, and travel advisories using natural language."

  - name: "traffic-intelligence-agent"
    type: "CORTEX_AGENT_RUN"
    identifier: "DEMO.DEMO.NYC_TRAFFIC_INTELLIGENCE_AGENT"
    title: "Traffic Intelligence Agent"
    description: "Comprehensive NYC traffic intelligence agent combining all data sources."

  - name: "execute-sql"
    type: "SYSTEM_EXECUTE_SQL"
    title: "Execute SQL"
    description: "Execute custom SQL queries against the NYC traffic data tables."
$$;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT SELECT ON SEMANTIC VIEW NYC_CAMERA_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW NYC_WEATHER_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW NYC_AIR_QUALITY_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW NYC_TRANSIT_ALERTS_SEMANTIC_VIEW TO ROLE PUBLIC;
GRANT USAGE ON CORTEX SEARCH SERVICE NYC_ALERTS_SEARCH TO ROLE PUBLIC;
GRANT USAGE ON AGENT NYC_TRAFFIC_INTELLIGENCE_AGENT TO ROLE PUBLIC;
GRANT USAGE ON MCP SERVER NYC_TRAFFIC_MCP_SERVER TO ROLE PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SHOW SEMANTIC VIEWS IN SCHEMA DEMO.DEMO LIKE 'NYC%';
SHOW CORTEX SEARCH SERVICES IN SCHEMA DEMO.DEMO;
SHOW AGENTS IN SCHEMA DEMO.DEMO;
SHOW MCP SERVERS IN SCHEMA DEMO.DEMO;

SELECT 'NYC_ALERTS_RAG' AS table_name, COUNT(*) AS row_count FROM DEMO.DEMO.NYC_ALERTS_RAG;
