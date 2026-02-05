-- ============================================================================
-- NYC TRAFFIC DATA PLATFORM - AI SQL FUNCTIONS WITH CLAUDE 4.5
-- ============================================================================
-- Cortex AI SQL functions utilizing claude-sonnet-4-5 for semantic analysis,
-- classification, sentiment, extraction, and similarity search
-- ============================================================================

USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;

-- ============================================================================
-- 1. AI_COMPLETE: ADVANCED SEMANTIC ANALYSIS WITH CLAUDE 4.5
-- ============================================================================

-- View: AI-Powered Transit Alert Analysis
CREATE OR REPLACE VIEW DEMO.DEMO.TRANSIT_ALERTS_AI_ANALYSIS AS
SELECT 
    title,
    description,
    ai_complete(
        'claude-sonnet-4-5',
        PROMPT('Analyze this transit alert and provide a structured analysis:
1) Affected routes/lines
2) Impact severity (LOW/MEDIUM/HIGH/CRITICAL)
3) Estimated duration
4) Recommended actions for commuters
Keep response concise (max 200 words): {0}', description)
    ) AS ai_analysis,
    pubdate,
    created_at
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL;

-- View: AI-Powered Weather Impact Assessment
CREATE OR REPLACE VIEW DEMO.DEMO.WEATHER_AI_ANALYSIS AS
SELECT 
    station_id,
    location,
    weather,
    temp_f,
    wind_mph,
    visibility_mi,
    AI_COMPLETE(
        'claude-sonnet-4-5',
        PROMPT('Based on these weather conditions, assess the impact on NYC traffic:
Weather: {0}
Temperature: ' || temp_f || 'F
Wind: ' || wind_mph || ' mph
Visibility: ' || visibility_mi || ' miles

Provide: 1) Traffic impact level 2) Safety recommendations 3) Areas most affected. Keep concise.', weather)
    ) AS traffic_impact_analysis,
    observation_time
FROM DEMO.DEMO.NOAAWEATHER
WHERE weather IS NOT NULL;

-- View: AI-Powered Air Quality Advisory
CREATE OR REPLACE VIEW DEMO.DEMO.AIR_QUALITY_AI_ADVISORY AS
SELECT 
    reportingarea,
    parametername AS pollutant,
    aqi,
    categoryname AS category,
    AI_COMPLETE(
        'claude-sonnet-4-5',
        PROMPT('Generate a public health advisory for this air quality reading:
Area: {0}
Pollutant: ' || parametername || '
AQI Level: ' || aqi || '
Category: ' || categoryname || '

Include: 1) Health risks 2) Vulnerable populations affected 3) Recommended precautions', reportingarea)
    ) AS health_advisory,
    dateobserved
FROM DEMO.DEMO.AQ
WHERE aqi IS NOT NULL;

-- ============================================================================
-- 2. AI_FILTER: INTELLIGENT ALERT FILTERING
-- ============================================================================

-- View: Filter Urgent/Emergency Alerts
CREATE OR REPLACE VIEW DEMO.DEMO.URGENT_TRANSIT_ALERTS AS
SELECT 
    title,
    description,
    AI_FILTER(PROMPT('Is this transit alert about an emergency, immediate closure, safety hazard, or urgent service disruption? Alert: {0}', description)) AS is_urgent,
    pubdate
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL
AND AI_FILTER(PROMPT('Is this transit alert about an emergency, immediate closure, safety hazard, or urgent service disruption? Alert: {0}', description)) = TRUE;

-- View: Filter Weather-Related Disruptions  
CREATE OR REPLACE VIEW DEMO.DEMO.WEATHER_RELATED_ALERTS AS
SELECT 
    title,
    description,
    AI_FILTER(PROMPT('Is this alert related to weather conditions like rain, snow, ice, wind, flooding, or extreme temperatures? Alert: {0}', description)) AS is_weather_related,
    pubdate
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL
AND AI_FILTER(PROMPT('Is this alert related to weather conditions like rain, snow, ice, wind, flooding, or extreme temperatures? Alert: {0}', description)) = TRUE;

-- View: Filter Accessibility-Related Alerts
CREATE OR REPLACE VIEW DEMO.DEMO.ACCESSIBILITY_ALERTS AS
SELECT 
    title,
    description,
    AI_FILTER(PROMPT('Is this alert about elevator outages, escalator closures, wheelchair accessibility, or ADA compliance issues? Alert: {0}', description)) AS is_accessibility_related,
    pubdate
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL
AND AI_FILTER(PROMPT('Is this alert about elevator outages, escalator closures, wheelchair accessibility, or ADA compliance issues? Alert: {0}', description)) = TRUE;

-- ============================================================================
-- 3. AI_SENTIMENT: ALERT SEVERITY SCORING
-- ============================================================================

-- View: Transit Alerts with Sentiment Scoring
CREATE OR REPLACE VIEW DEMO.DEMO.TRANSIT_ALERTS_SENTIMENT AS
SELECT 
    title,
    LEFT(description, 200) AS description_preview,
    SNOWFLAKE.CORTEX.SENTIMENT(description) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(description) < -0.3 THEN 'Negative - Service Disruption'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(description) > 0.3 THEN 'Positive - Service Improvement'
        ELSE 'Neutral - Informational'
    END AS sentiment_category,
    pubdate
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL;

-- ============================================================================
-- 4. ENTITY EXTRACTION: STRUCTURED DATA FROM ALERTS
-- ============================================================================

-- View: Extract Structured Entities from Transit Alerts
CREATE OR REPLACE VIEW DEMO.DEMO.TRANSIT_ALERTS_ENTITIES AS
SELECT 
    title,
    description,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(description, 'What routes, trains, or bus lines are affected?')[0]:answer::VARCHAR AS affected_routes,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(description, 'What stations or stops are mentioned?')[0]:answer::VARCHAR AS affected_stations,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(description, 'When does this alert start and end?')[0]:answer::VARCHAR AS timing,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(description, 'What is the cause of this issue?')[0]:answer::VARCHAR AS cause,
    pubdate
FROM DEMO.DEMO.TRANSIT_ALERTS
WHERE description IS NOT NULL;

-- ============================================================================
-- 5. AI_AGG & AI_SUMMARIZE_AGG: AGGREGATED INSIGHTS
-- ============================================================================

-- Procedure: Generate Daily Transit Summary Report
CREATE OR REPLACE PROCEDURE DEMO.DEMO.GENERATE_DAILY_TRANSIT_SUMMARY()
RETURNS TABLE(report_date DATE, summary TEXT, top_issues TEXT, total_alerts INT)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        SELECT 
            CURRENT_DATE() AS report_date,
            AI_SUMMARIZE_AGG(description) AS summary,
            AI_AGG(description, 'Identify the top 5 most critical issues affecting transit services today. Rank by severity and passenger impact.') AS top_issues,
            COUNT(*) AS total_alerts
        FROM DEMO.DEMO.TRANSIT_ALERTS
        WHERE description IS NOT NULL
        AND updated_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    );
    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- 6. SUMMARIZATION: CONDENSED REPORTS
-- ============================================================================

-- View: Summarized Travel Advisories
CREATE OR REPLACE VIEW DEMO.DEMO.TRAVEL_ADVISORIES_SUMMARIZED AS
SELECT 
    title,
    SNOWFLAKE.CORTEX.SUMMARIZE(description) AS summary,
    LENGTH(description) AS original_length,
    LENGTH(SNOWFLAKE.CORTEX.SUMMARIZE(description)) AS summary_length,
    published
FROM DEMO.DEMO.TRAVELADVISORIES
WHERE description IS NOT NULL
AND LENGTH(description) > 200;

-- ============================================================================
-- 7. SIMILARITY SEARCH: SEMANTIC VECTOR SEARCH
-- ============================================================================

-- Function: Find Similar Transit Alerts Using Vector Search
CREATE OR REPLACE FUNCTION DEMO.DEMO.FIND_SIMILAR_ALERTS(search_query VARCHAR, result_limit INT DEFAULT 5)
RETURNS TABLE(
    title VARCHAR,
    description_preview VARCHAR,
    similarity_distance FLOAT
)
LANGUAGE SQL
AS
$$
    WITH query_embedding AS (
        SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', search_query) AS query_vec
    ),
    alert_embeddings AS (
        SELECT 
            title,
            LEFT(description, 150) AS description_preview,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', description) AS embedding
        FROM DEMO.DEMO.TRANSIT_ALERTS
        WHERE description IS NOT NULL
    )
    SELECT 
        a.title,
        a.description_preview,
        VECTOR_L2_DISTANCE(a.embedding, q.query_vec) AS similarity_distance
    FROM alert_embeddings a
    CROSS JOIN query_embedding q
    ORDER BY similarity_distance ASC
    LIMIT result_limit
$$;

-- Function: Find Similar Weather Conditions
CREATE OR REPLACE FUNCTION DEMO.DEMO.FIND_SIMILAR_WEATHER(search_condition VARCHAR, result_limit INT DEFAULT 5)
RETURNS TABLE(
    station_id VARCHAR,
    location VARCHAR,
    weather VARCHAR,
    temp_f FLOAT,
    similarity_distance FLOAT
)
LANGUAGE SQL
AS
$$
    WITH query_embedding AS (
        SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', search_condition) AS query_vec
    ),
    weather_embeddings AS (
        SELECT 
            station_id,
            location,
            weather,
            temp_f,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', weather || ' ' || COALESCE(location, '')) AS embedding
        FROM DEMO.DEMO.NOAAWEATHER
        WHERE weather IS NOT NULL
    )
    SELECT 
        w.station_id,
        w.location,
        w.weather,
        w.temp_f,
        VECTOR_L2_DISTANCE(w.embedding, q.query_vec) AS similarity_distance
    FROM weather_embeddings w
    CROSS JOIN query_embedding q
    ORDER BY similarity_distance ASC
    LIMIT result_limit
$$;

-- ============================================================================
-- 8. INTELLIGENT CLASSIFICATION
-- ============================================================================

-- Function: Classify Alert Severity Using Claude 4.5
CREATE OR REPLACE FUNCTION DEMO.DEMO.CLASSIFY_ALERT_SEVERITY(alert_text VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$
    SELECT AI_COMPLETE(
        'claude-sonnet-4-5',
        PROMPT('Classify this transit alert severity. Respond with ONLY one word: CRITICAL, HIGH, MEDIUM, or LOW.

Alert: {0}

Classification:', alert_text)
    )::VARCHAR
$;

-- Function: Extract Alert Category
CREATE OR REPLACE FUNCTION DEMO.DEMO.CATEGORIZE_ALERT(alert_text VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$
    SELECT AI_COMPLETE(
        'claude-sonnet-4-5',
        PROMPT('Categorize this transit alert into ONE category. Respond with ONLY the category name.

Categories: Service Disruption, Maintenance, Weather Impact, Safety Alert, Schedule Change, Special Event, Accessibility, Construction

Alert: {0}

Category:', alert_text)
    )::VARCHAR
$;

-- ============================================================================
-- 9. MULTIMODAL ANALYSIS PREPARATION (FOR CAMERA IMAGES)
-- ============================================================================

-- Note: For actual image analysis, images need to be staged in Snowflake.
-- This creates the framework for future camera image analysis.

-- View: Camera Locations Prepared for Image Analysis
CREATE OR REPLACE VIEW DEMO.DEMO.CAMERAS_FOR_IMAGE_ANALYSIS AS
SELECT 
    id AS camera_id,
    name AS camera_name,
    roadwayname AS roadway,
    directionoftravel AS direction,
    latitude,
    longitude,
    videourl AS image_url,
    ST_MAKEPOINT(longitude, latitude) AS geo_point,
    'Ready for AI image analysis when images are staged' AS analysis_status
FROM DEMO.DEMO.CAMERAS
WHERE latitude IS NOT NULL 
AND longitude IS NOT NULL;

-- ============================================================================
-- 10. COMPREHENSIVE AI ANALYSIS VIEW
-- ============================================================================

-- View: Complete AI-Enriched Transit Intelligence
CREATE OR REPLACE VIEW DEMO.DEMO.TRANSIT_INTELLIGENCE_AI AS
SELECT 
    ta.title,
    ta.description,
    -- Sentiment Analysis
    SNOWFLAKE.CORTEX.SENTIMENT(ta.description) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(ta.description) < -0.3 THEN 'Negative'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(ta.description) > 0.3 THEN 'Positive'
        ELSE 'Neutral'
    END AS sentiment_category,
    -- Entity Extraction
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(ta.description, 'What routes are affected?')[0]:answer::VARCHAR AS extracted_routes,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(ta.description, 'What stations are mentioned?')[0]:answer::VARCHAR AS extracted_stations,
    -- Urgency Filter
    AI_FILTER(PROMPT('Is this an urgent or emergency alert? Alert: {0}', ta.description)) AS is_urgent,
    -- Summarization
    SNOWFLAKE.CORTEX.SUMMARIZE(ta.description) AS alert_summary,
    ta.updated_at
FROM DEMO.DEMO.TRANSIT_ALERTS ta
WHERE ta.description IS NOT NULL;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

/*
-- 1. AI-Powered Alert Analysis with Claude 4.5
SELECT * FROM DEMO.DEMO.TRANSIT_ALERTS_AI_ANALYSIS LIMIT 5;

-- 2. Get Urgent Alerts Only
SELECT * FROM DEMO.DEMO.URGENT_TRANSIT_ALERTS LIMIT 10;

-- 3. View Alert Sentiment Distribution
SELECT sentiment_category, COUNT(*) AS count
FROM DEMO.DEMO.TRANSIT_ALERTS_SENTIMENT
GROUP BY sentiment_category;

-- 4. Extract Structured Entities
SELECT title, affected_routes, affected_stations, timing
FROM DEMO.DEMO.TRANSIT_ALERTS_ENTITIES
LIMIT 10;

-- 5. Find Similar Alerts (Semantic Search)
SELECT * FROM TABLE(DEMO.DEMO.FIND_SIMILAR_ALERTS('train delays and cancellations', 5));

-- 6. Classify Alert Severity
SELECT title, description, DEMO.DEMO.CLASSIFY_ALERT_SEVERITY(description) AS severity
FROM DEMO.DEMO.TRANSIT_ALERTS
LIMIT 5;

-- 7. Categorize Alerts
SELECT title, description, DEMO.DEMO.CATEGORIZE_ALERT(description) AS category
FROM DEMO.DEMO.TRANSIT_ALERTS
LIMIT 5;

-- 8. Generate Daily Summary Report
CALL DEMO.DEMO.GENERATE_DAILY_TRANSIT_SUMMARY();

-- 9. Full AI-Enriched Intelligence View
SELECT * FROM DEMO.DEMO.TRANSIT_INTELLIGENCE_AI LIMIT 5;

-- 10. Weather Impact Analysis
SELECT * FROM DEMO.DEMO.WEATHER_AI_ANALYSIS LIMIT 5;

-- 11. Air Quality Health Advisory
SELECT * FROM DEMO.DEMO.AIR_QUALITY_AI_ADVISORY LIMIT 5;

-- 12. Find Similar Weather Conditions
SELECT * FROM TABLE(DEMO.DEMO.FIND_SIMILAR_WEATHER('heavy rain and fog', 5));

-- 13. Accessibility Alerts
SELECT * FROM DEMO.DEMO.ACCESSIBILITY_ALERTS;

-- 14. Summarized Travel Advisories
SELECT * FROM DEMO.DEMO.TRAVEL_ADVISORIES_SUMMARIZED LIMIT 5;
*/

-- ============================================================================
-- PRINT DEPLOYMENT STATUS
-- ============================================================================
SELECT 'AI SQL Functions with Claude 4.5 deployed successfully!' AS status;
