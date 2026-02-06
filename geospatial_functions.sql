-- ============================================================================
-- NYC TRAFFIC DATA PLATFORM - GEOSPATIAL FUNCTIONS AND VIEWS
-- ============================================================================
-- Geospatial capabilities using ST_POINT, ST_DISTANCE, ST_DWITHIN, H3, HAVERSINE
-- ============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;

-- ============================================================================
-- GEOSPATIAL VIEWS WITH GEOGRAPHY POINTS
-- ============================================================================

CREATE OR REPLACE VIEW DEMO.DEMO.WEATHER_STATIONS_GEO AS
SELECT 
    station_id,
    location,
    weather,
    temp_f,
    temp_c,
    relative_humidity,
    wind_mph,
    wind_dir,
    visibility_mi,
    pressure_in,
    dewpoint_f,
    observation_time,
    latitude,
    longitude,
    ST_MAKEPOINT(longitude, latitude) AS geo_point,
    H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_cell_7,
    H3_LATLNG_TO_CELL(latitude, longitude, 9) AS h3_cell_9,
    updated_at
FROM DEMO.DEMO.NOAAWEATHER
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

CREATE OR REPLACE VIEW DEMO.DEMO.AIR_QUALITY_MONITORS_GEO AS
SELECT 
    reportingarea,
    statecode,
    parametername AS pollutant,
    aqi,
    categoryname AS category,
    categorynumber,
    dateobserved,
    hourobserved,
    localtimezone,
    latitude,
    longitude,
    ST_MAKEPOINT(longitude, latitude) AS geo_point,
    H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_cell_7,
    H3_LATLNG_TO_CELL(latitude, longitude, 9) AS h3_cell_9
FROM DEMO.DEMO.AQ
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- ============================================================================
-- H3 HEXAGONAL GRID JOIN VIEW
-- ============================================================================

CREATE OR REPLACE VIEW DEMO.DEMO.ENVIRONMENTAL_DATA_H3_JOINED AS
WITH weather_h3 AS (
    SELECT 
        h3_cell_7,
        AVG(temp_f) AS avg_temp_f,
        AVG(wind_mph) AS avg_wind_mph,
        AVG(visibility_mi) AS avg_visibility_mi,
        COUNT(DISTINCT station_id) AS weather_station_count
    FROM DEMO.DEMO.WEATHER_STATIONS_GEO
    GROUP BY h3_cell_7
),
aq_h3 AS (
    SELECT 
        h3_cell_7,
        AVG(aqi) AS avg_aqi,
        MAX(aqi) AS max_aqi,
        MODE(category) AS typical_category,
        COUNT(*) AS aq_reading_count
    FROM DEMO.DEMO.AIR_QUALITY_MONITORS_GEO
    GROUP BY h3_cell_7
)
SELECT 
    COALESCE(w.h3_cell_7, a.h3_cell_7) AS h3_cell,
    H3_CELL_TO_BOUNDARY(COALESCE(w.h3_cell_7, a.h3_cell_7)) AS h3_boundary,
    H3_CELL_TO_POINT(COALESCE(w.h3_cell_7, a.h3_cell_7)) AS h3_center,
    w.avg_temp_f,
    w.avg_wind_mph,
    w.avg_visibility_mi,
    w.weather_station_count,
    a.avg_aqi,
    a.max_aqi,
    a.typical_category AS aqi_category,
    a.aq_reading_count
FROM weather_h3 w
FULL OUTER JOIN aq_h3 a ON w.h3_cell_7 = a.h3_cell_7;

-- ============================================================================
-- GEOSPATIAL FUNCTIONS
-- ============================================================================

-- Distance calculator in miles
CREATE OR REPLACE FUNCTION DEMO.DEMO.CALC_DISTANCE_MILES(
    lat1 FLOAT, lon1 FLOAT,
    lat2 FLOAT, lon2 FLOAT
)
RETURNS FLOAT
AS
$$
    HAVERSINE(lat1, lon1, lat2, lon2) / 1609.34
$$;

-- Find nearest weather station to any location
CREATE OR REPLACE FUNCTION DEMO.DEMO.GET_NEAREST_WEATHER_STATION(
    input_lat FLOAT,
    input_lon FLOAT
)
RETURNS TABLE (
    station_id VARCHAR,
    location VARCHAR,
    weather VARCHAR,
    temp_f FLOAT,
    wind_mph FLOAT,
    visibility_mi FLOAT,
    distance_miles FLOAT
)
AS
$$
    SELECT 
        station_id,
        location,
        weather,
        temp_f,
        wind_mph,
        visibility_mi,
        ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles
    FROM DEMO.DEMO.WEATHER_STATIONS_GEO
    ORDER BY ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat))
    LIMIT 1
$$;

-- Find all weather stations within radius
CREATE OR REPLACE FUNCTION DEMO.DEMO.GET_WEATHER_STATIONS_WITHIN_RADIUS(
    input_lat FLOAT,
    input_lon FLOAT,
    radius_miles FLOAT
)
RETURNS TABLE (
    station_id VARCHAR,
    location VARCHAR,
    weather VARCHAR,
    temp_f FLOAT,
    wind_mph FLOAT,
    visibility_mi FLOAT,
    distance_miles FLOAT
)
AS
$$
    SELECT 
        station_id,
        location,
        weather,
        temp_f,
        wind_mph,
        visibility_mi,
        ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles
    FROM DEMO.DEMO.WEATHER_STATIONS_GEO
    WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(input_lon, input_lat), radius_miles * 1609.34)
    ORDER BY distance_miles
$$;

-- Find nearest air quality monitor
CREATE OR REPLACE FUNCTION DEMO.DEMO.GET_NEAREST_AIR_QUALITY(
    input_lat FLOAT,
    input_lon FLOAT
)
RETURNS TABLE (
    reportingarea VARCHAR,
    statecode VARCHAR,
    pollutant VARCHAR,
    aqi NUMBER,
    category VARCHAR,
    distance_miles FLOAT
)
AS
$$
    SELECT 
        reportingarea,
        statecode,
        pollutant,
        aqi,
        category,
        ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles
    FROM DEMO.DEMO.AIR_QUALITY_MONITORS_GEO
    ORDER BY ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat))
    LIMIT 1
$$;

-- Comprehensive environmental conditions at location
CREATE OR REPLACE FUNCTION DEMO.DEMO.GET_ENVIRONMENTAL_CONDITIONS_AT_LOCATION(
    input_lat FLOAT,
    input_lon FLOAT,
    search_radius_miles FLOAT DEFAULT 50
)
RETURNS TABLE (
    data_type VARCHAR,
    source_name VARCHAR,
    key_metric VARCHAR,
    metric_value VARCHAR,
    distance_miles FLOAT,
    geo_point GEOGRAPHY
)
AS
$$
    WITH weather_data AS (
        SELECT 
            'WEATHER' AS data_type,
            station_id || ' - ' || location AS source_name,
            'Temperature (°F)' AS key_metric,
            temp_f::VARCHAR AS metric_value,
            ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles,
            geo_point
        FROM DEMO.DEMO.WEATHER_STATIONS_GEO
        WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(input_lon, input_lat), search_radius_miles * 1609.34)
        QUALIFY ROW_NUMBER() OVER (ORDER BY distance_miles) <= 3
        UNION ALL
        SELECT 
            'WEATHER' AS data_type,
            station_id || ' - ' || location AS source_name,
            'Conditions' AS key_metric,
            weather AS metric_value,
            ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles,
            geo_point
        FROM DEMO.DEMO.WEATHER_STATIONS_GEO
        WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(input_lon, input_lat), search_radius_miles * 1609.34)
        QUALIFY ROW_NUMBER() OVER (ORDER BY distance_miles) <= 3
    ),
    aq_data AS (
        SELECT 
            'AIR_QUALITY' AS data_type,
            reportingarea || ', ' || statecode AS source_name,
            'AQI (' || pollutant || ')' AS key_metric,
            aqi::VARCHAR || ' - ' || category AS metric_value,
            ST_DISTANCE(geo_point, ST_MAKEPOINT(input_lon, input_lat)) / 1609.34 AS distance_miles,
            geo_point
        FROM DEMO.DEMO.AIR_QUALITY_MONITORS_GEO
        WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(input_lon, input_lat), search_radius_miles * 1609.34)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY pollutant ORDER BY distance_miles) <= 2
    )
    SELECT * FROM weather_data
    UNION ALL
    SELECT * FROM aq_data
    ORDER BY data_type, distance_miles
$$;

-- ============================================================================
-- STORED PROCEDURE FOR FULL LOCATION ANALYSIS (returns JSON)
-- ============================================================================

CREATE OR REPLACE PROCEDURE DEMO.DEMO.ANALYZE_LOCATION_CONDITIONS(
    INPUT_LAT FLOAT,
    INPUT_LON FLOAT,
    RADIUS_MILES FLOAT
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
'
var weather_sql = `
    SELECT 
        station_id,
        location,
        weather,
        temp_f,
        wind_mph,
        visibility_mi,
        ROUND(ST_DISTANCE(geo_point, ST_MAKEPOINT(` + INPUT_LON + `, ` + INPUT_LAT + `)) / 1609.34, 2) AS distance_miles
    FROM DEMO.DEMO.WEATHER_STATIONS_GEO
    WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(` + INPUT_LON + `, ` + INPUT_LAT + `), ` + RADIUS_MILES + ` * 1609.34)
    ORDER BY distance_miles
    LIMIT 5
`;

var aq_sql = `
    SELECT 
        reportingarea,
        statecode,
        pollutant,
        aqi,
        category,
        ROUND(ST_DISTANCE(geo_point, ST_MAKEPOINT(` + INPUT_LON + `, ` + INPUT_LAT + `)) / 1609.34, 2) AS distance_miles
    FROM DEMO.DEMO.AIR_QUALITY_MONITORS_GEO
    WHERE ST_DWITHIN(geo_point, ST_MAKEPOINT(` + INPUT_LON + `, ` + INPUT_LAT + `), ` + RADIUS_MILES + ` * 1609.34)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pollutant ORDER BY ST_DISTANCE(geo_point, ST_MAKEPOINT(` + INPUT_LON + `, ` + INPUT_LAT + `))) <= 3
`;

var weather_results = [];
var aq_results = [];

var weather_stmt = snowflake.createStatement({sqlText: weather_sql});
var weather_rs = weather_stmt.execute();
while (weather_rs.next()) {
    weather_results.push({
        station_id: weather_rs.getColumnValue("STATION_ID"),
        location: weather_rs.getColumnValue("LOCATION"),
        conditions: weather_rs.getColumnValue("WEATHER"),
        temperature_f: weather_rs.getColumnValue("TEMP_F"),
        wind_mph: weather_rs.getColumnValue("WIND_MPH"),
        visibility_mi: weather_rs.getColumnValue("VISIBILITY_MI"),
        distance_miles: weather_rs.getColumnValue("DISTANCE_MILES")
    });
}

var aq_stmt = snowflake.createStatement({sqlText: aq_sql});
var aq_rs = aq_stmt.execute();
while (aq_rs.next()) {
    aq_results.push({
        reporting_area: aq_rs.getColumnValue("REPORTINGAREA"),
        state: aq_rs.getColumnValue("STATECODE"),
        pollutant: aq_rs.getColumnValue("POLLUTANT"),
        aqi: aq_rs.getColumnValue("AQI"),
        category: aq_rs.getColumnValue("CATEGORY"),
        distance_miles: aq_rs.getColumnValue("DISTANCE_MILES")
    });
}

return {
    location: {
        latitude: INPUT_LAT,
        longitude: INPUT_LON,
        search_radius_miles: RADIUS_MILES
    },
    weather: weather_results,
    air_quality: aq_results,
    analysis_timestamp: new Date().toISOString()
};
';

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

GRANT SELECT ON VIEW WEATHER_STATIONS_GEO TO ROLE PUBLIC;
GRANT SELECT ON VIEW AIR_QUALITY_MONITORS_GEO TO ROLE PUBLIC;
GRANT SELECT ON VIEW ENVIRONMENTAL_DATA_H3_JOINED TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION CALC_DISTANCE_MILES(FLOAT, FLOAT, FLOAT, FLOAT) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION GET_NEAREST_WEATHER_STATION(FLOAT, FLOAT) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION GET_WEATHER_STATIONS_WITHIN_RADIUS(FLOAT, FLOAT, FLOAT) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION GET_NEAREST_AIR_QUALITY(FLOAT, FLOAT) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION GET_ENVIRONMENTAL_CONDITIONS_AT_LOCATION(FLOAT, FLOAT, FLOAT) TO ROLE PUBLIC;
GRANT USAGE ON PROCEDURE ANALYZE_LOCATION_CONDITIONS(FLOAT, FLOAT, FLOAT) TO ROLE PUBLIC;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Find nearest weather station to Times Square (cast to FLOAT to avoid type errors)
-- SELECT * FROM TABLE(GET_NEAREST_WEATHER_STATION(40.7580::FLOAT, -73.9855::FLOAT));

-- Find all weather stations within 25 miles of JFK Airport  
-- SELECT * FROM TABLE(GET_WEATHER_STATIONS_WITHIN_RADIUS(40.6413::FLOAT, -73.7781::FLOAT, 25::FLOAT));

-- Get full environmental conditions at Central Park
-- SELECT * FROM TABLE(GET_ENVIRONMENTAL_CONDITIONS_AT_LOCATION(40.7829::FLOAT, -73.9654::FLOAT, 50::FLOAT));

-- Call the stored procedure for JSON report
-- CALL ANALYZE_LOCATION_CONDITIONS(40.7128, -74.0060, 50);

-- Query the H3 joined view
-- SELECT * FROM ENVIRONMENTAL_DATA_H3_JOINED WHERE avg_aqi IS NOT NULL ORDER BY avg_aqi DESC LIMIT 10;
