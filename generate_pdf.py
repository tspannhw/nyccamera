"""
NYC Camera Pipeline Documentation PDF Generator
Generates a professional PDF document from the project documentation
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, black, white
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    PageBreak, Image, ListFlowable, ListItem
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.pdfgen import canvas
from datetime import datetime
import os

BLADE_RUNNER_BLUE = HexColor('#00d4ff')
BLADE_RUNNER_ORANGE = HexColor('#ff6b35')
BLADE_RUNNER_DARK = HexColor('#1a1a2e')
BLADE_RUNNER_GRAY = HexColor('#16213e')

def create_styles():
    styles = getSampleStyleSheet()
    
    styles.add(ParagraphStyle(
        name='CoverTitle',
        parent=styles['Title'],
        fontSize=28,
        textColor=BLADE_RUNNER_BLUE,
        alignment=TA_CENTER,
        spaceAfter=20,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CoverSubtitle',
        parent=styles['Normal'],
        fontSize=14,
        textColor=BLADE_RUNNER_ORANGE,
        alignment=TA_CENTER,
        spaceAfter=10,
        fontName='Helvetica-Oblique'
    ))
    
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=BLADE_RUNNER_BLUE,
        spaceBefore=20,
        spaceAfter=10,
        fontName='Helvetica-Bold',
        borderPadding=5,
    ))
    
    styles.add(ParagraphStyle(
        name='SubsectionHeader',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=BLADE_RUNNER_ORANGE,
        spaceBefore=15,
        spaceAfter=8,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='CustomBodyText',
        parent=styles['Normal'],
        fontSize=10,
        textColor=black,
        alignment=TA_JUSTIFY,
        spaceAfter=8,
        leading=14
    ))
    
    styles.add(ParagraphStyle(
        name='CodeBlock',
        parent=styles['Code'],
        fontSize=8,
        textColor=HexColor('#333333'),
        backColor=HexColor('#f5f5f5'),
        borderPadding=8,
        spaceAfter=10,
        fontName='Courier'
    ))
    
    styles.add(ParagraphStyle(
        name='Quote',
        parent=styles['Normal'],
        fontSize=10,
        textColor=HexColor('#666666'),
        fontName='Helvetica-Oblique',
        leftIndent=20,
        rightIndent=20,
        spaceBefore=10,
        spaceAfter=10
    ))
    
    styles.add(ParagraphStyle(
        name='TableHeader',
        parent=styles['Normal'],
        fontSize=9,
        textColor=white,
        fontName='Helvetica-Bold',
        alignment=TA_CENTER
    ))
    
    return styles

def add_header_footer(canvas, doc):
    canvas.saveState()
    canvas.setFillColor(BLADE_RUNNER_DARK)
    canvas.rect(0, letter[1] - 40, letter[0], 40, fill=1, stroke=0)
    canvas.setFillColor(BLADE_RUNNER_BLUE)
    canvas.setFont('Helvetica-Bold', 10)
    canvas.drawString(inch, letter[1] - 25, "NYC Camera Pipeline Documentation")
    canvas.setFillColor(BLADE_RUNNER_ORANGE)
    canvas.drawRightString(letter[0] - inch, letter[1] - 25, "Tyrell Corp Data Systems")
    canvas.setFillColor(HexColor('#333333'))
    canvas.setFont('Helvetica', 8)
    canvas.drawString(inch, 0.5 * inch, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    canvas.drawRightString(letter[0] - inch, 0.5 * inch, f"Page {doc.page}")
    canvas.restoreState()

def create_table(data, col_widths=None):
    if col_widths is None:
        col_widths = [1.5*inch] * len(data[0])
    
    table = Table(data, colWidths=col_widths)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), BLADE_RUNNER_DARK),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f9f9f9')),
        ('TEXTCOLOR', (0, 1), (-1, -1), black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#cccccc')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f5f5f5')]),
    ]))
    return table

def build_pdf(output_path):
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=1*inch,
        bottomMargin=0.75*inch
    )
    
    styles = create_styles()
    story = []
    
    story.append(Spacer(1, 1.5*inch))
    story.append(Paragraph("NYC Street Camera Data Pipeline", styles['CoverTitle']))
    story.append(Spacer(1, 0.25*inch))
    story.append(Paragraph("Blade Runner Surveillance System", styles['CoverSubtitle']))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph(
        '"I\'ve seen things you people wouldn\'t believe...<br/>Traffic cameras on fire off the shoulder of I-495..."',
        styles['Quote']
    ))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph(
        "Stream NYC traffic camera data to Snowflake via Snowpipe Streaming High Speed v2 REST API, "
        "with optional PostgreSQL storage and Slack notifications. A real-time data engineering pipeline for the cyberpunk age.",
        styles['CustomBodyText']
    ))
    story.append(Spacer(1, 1*inch))
    
    cover_table_data = [
        ['Document', 'NYC Camera Pipeline Technical Documentation'],
        ['Version', '2.0'],
        ['Date', datetime.now().strftime('%B %d, %Y')],
        ['Author', 'Tyrell Corporation Data Systems'],
        ['Classification', 'REPLICANT APPROVED']
    ]
    story.append(create_table(cover_table_data, [1.5*inch, 4*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("Table of Contents", styles['SectionHeader']))
    toc_items = [
        "1. Architecture Overview",
        "2. Features",
        "3. Data Sources",
        "4. System Components",
        "5. Prerequisites",
        "6. Quick Start Guide",
        "7. Project Structure",
        "8. Data Schema",
        "9. Snowflake Tables",
        "10. SQL Semantic Views",
        "11. Dashboards and Notebooks",
        "12. Example Queries",
        "13. Environmental Data Integration",
        "14. API Reference",
        "15. Troubleshooting"
    ]
    for item in toc_items:
        story.append(Paragraph(item, styles['CustomBodyText']))
    story.append(PageBreak())
    
    story.append(Paragraph("1. Architecture Overview", styles['SectionHeader']))
    story.append(Paragraph(
        "The NYC Camera Pipeline is a real-time data engineering system that streams traffic camera data, "
        "traffic events, speed data, and environmental information to Snowflake using the Snowpipe Streaming "
        "High Speed v2 REST API.",
        styles['CustomBodyText']
    ))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("System Components:", styles['SubsectionHeader']))
    components = [
        "Data Acquisition Layer - 511NY API, NOAA Weather, EPA Air Quality",
        "Processing Engine - Python Streaming Client with JWT/PAT Auth",
        "Storage Layer - Snowflake Standard Tables, Iceberg Tables",
        "Integration Layer - PostgreSQL Backup, Slack Notifications",
        "Visualization Layer - Streamlit Dashboards, Snowflake Notebooks",
        "AI Analysis - Cortex AI with Claude for image analysis"
    ]
    for comp in components:
        story.append(Paragraph(f"- {comp}", styles['CustomBodyText']))
    story.append(PageBreak())
    
    story.append(Paragraph("2. Features", styles['SectionHeader']))
    features_data = [
        ['Feature', 'Description', 'Status'],
        ['Real-time Streaming', 'Snowpipe Streaming v2 REST API (10GB/s)', 'Active'],
        ['Iceberg Tables', 'Open table format for data lakehouse', 'Active'],
        ['PostgreSQL Backup', 'Redundant metadata storage', 'Optional'],
        ['Slack Alerts', 'Real-time image notifications', 'Optional'],
        ['Cortex AI', 'Claude-powered image analysis', 'Ready'],
        ['Streamlit Dashboard', 'Interactive web visualization', 'Active'],
        ['Snowflake Notebooks', 'In-platform analytics (3 notebooks)', 'Active'],
        ['SQL Semantic Views', 'Natural language queries', 'Active'],
        ['Traffic Events', '511NY incidents and construction', 'Active'],
        ['Traffic Speeds', 'Real-time congestion data', 'Active'],
        ['Weather Integration', 'NOAA weather station data', 'Active'],
        ['Air Quality', 'EPA AQI monitoring data', 'Active']
    ]
    story.append(create_table(features_data, [1.5*inch, 3.5*inch, 0.8*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("3. Data Sources", styles['SectionHeader']))
    story.append(Paragraph("Primary Sources (511NY API)", styles['SubsectionHeader']))
    primary_data = [
        ['Source', 'Endpoint', 'Data Type', 'Refresh'],
        ['Cameras', '/cameras', 'Live camera feeds', '60 sec'],
        ['Traffic Events', '/events', 'Incidents, construction', '60 sec'],
        ['Traffic Speeds', '/speeds', 'Segment speeds', '60 sec']
    ]
    story.append(create_table(primary_data, [1.2*inch, 1.5*inch, 2*inch, 1*inch]))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("Environmental Sources", styles['SubsectionHeader']))
    env_data = [
        ['Source', 'Table', 'Description'],
        ['NOAA Weather', 'NOAAWEATHER', 'Weather observations from NOAA stations'],
        ['EPA AirNow', 'AQ', 'Air Quality Index measurements'],
        ['Weather Obs', 'WEATHER_OBSERVATIONS', 'Extended weather data']
    ]
    story.append(create_table(env_data, [1.5*inch, 2*inch, 2.5*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("4. System Components", styles['SectionHeader']))
    
    story.append(Paragraph("Data Acquisition (nyc_camera_sensor.py)", styles['SubsectionHeader']))
    story.append(Paragraph(
        "Connects to 511NY traffic camera API, fetches 2000+ camera feeds across NYC metropolitan area, "
        "filters disabled/blocked cameras, and extracts location, direction, roadway, and URLs.",
        styles['CustomBodyText']
    ))
    
    story.append(Paragraph("Traffic Events (nyc_traffic_events_sensor.py)", styles['SubsectionHeader']))
    story.append(Paragraph(
        "Handles real-time traffic incidents and construction data, speed data from traffic sensors, "
        "event severity classification, and geographic coverage mapping.",
        styles['CustomBodyText']
    ))
    
    story.append(Paragraph("Streaming Client (snowpipe_streaming_client.py)", styles['SubsectionHeader']))
    story.append(Paragraph(
        "Implements Snowpipe Streaming v2 REST API with JWT/PAT authentication via snowflake_jwt_auth.py, "
        "channel management (open, append, close), and offset token tracking for exactly-once delivery.",
        styles['CustomBodyText']
    ))
    
    story.append(Paragraph("Storage Layer", styles['SubsectionHeader']))
    storage_items = [
        "Snowflake Standard Table - Primary storage for real-time queries",
        "Snowflake Iceberg Table - Open format for interoperability",
        "PostgreSQL - Optional backup/redundancy"
    ]
    for item in storage_items:
        story.append(Paragraph(f"- {item}", styles['CustomBodyText']))
    
    story.append(Paragraph("AI Analysis (Cortex)", styles['SubsectionHeader']))
    ai_items = [
        "claude-3-5-sonnet for multimodal image analysis",
        "Traffic condition assessment",
        "Weather/visibility detection",
        "Vehicle counting"
    ]
    for item in ai_items:
        story.append(Paragraph(f"- {item}", styles['CustomBodyText']))
    story.append(PageBreak())
    
    story.append(Paragraph("5. Prerequisites", styles['SectionHeader']))
    prereq_data = [
        ['Requirement', 'Version', 'Notes'],
        ['Python', '3.9+', 'Required'],
        ['Snowflake Account', 'Enterprise+', 'Snowpipe Streaming enabled'],
        ['511NY API Key', '-', 'Free at https://511ny.org/'],
        ['PostgreSQL', '12+', 'Optional'],
        ['Slack Bot Token', '-', 'Optional, xoxb-* format']
    ]
    story.append(create_table(prereq_data, [1.8*inch, 1.2*inch, 2.5*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("6. Quick Start Guide", styles['SectionHeader']))
    
    story.append(Paragraph("Step 1: Clone and Install", styles['SubsectionHeader']))
    story.append(Paragraph("git clone https://github.com/tspannhw/nyccamera.git", styles['CodeBlock']))
    story.append(Paragraph("cd nyccamera", styles['CodeBlock']))
    story.append(Paragraph("pip install -r requirements.txt", styles['CodeBlock']))
    
    story.append(Paragraph("Step 2: Configure Snowflake", styles['SubsectionHeader']))
    story.append(Paragraph(
        "Create snowflake_config.json with user, account, pat, role, database, schema, table, "
        "iceberg_table, pipe, and channel_name settings.",
        styles['CustomBodyText']
    ))
    
    story.append(Paragraph("Step 3: Initialize Database", styles['SubsectionHeader']))
    story.append(Paragraph("python setup_tables.py --snowflake-only", styles['CodeBlock']))
    
    story.append(Paragraph("Step 4: Run the Pipeline", styles['SubsectionHeader']))
    story.append(Paragraph("export NYC_API_KEY='your_511ny_api_key'", styles['CodeBlock']))
    story.append(Paragraph("./run_nyc_camera.sh", styles['CodeBlock']))
    story.append(PageBreak())
    
    story.append(Paragraph("7. Project Structure", styles['SectionHeader']))
    structure_data = [
        ['Category', 'Files', 'Description'],
        ['Core App', 'nyc_camera_main.py', 'Main entry point'],
        ['Core App', 'nyc_camera_sensor.py', '511NY camera API client'],
        ['Core App', 'nyc_traffic_events_sensor.py', '511NY events/speeds client'],
        ['Core App', 'snowpipe_streaming_client.py', 'Snowpipe v2 REST client'],
        ['Core App', 'snowflake_jwt_auth.py', 'Authentication module'],
        ['Integrations', 'postgresql_client.py', 'PostgreSQL storage'],
        ['Integrations', 'slack_notifier.py', 'Slack notifications'],
        ['Visualization', 'streamlit_app.py', 'Camera dashboard'],
        ['Visualization', 'traffic_dashboard.py', 'Full traffic dashboard'],
        ['Notebooks', 'nyc_camera_analytics.ipynb', 'Camera analytics'],
        ['Notebooks', 'nyc_traffic_analytics.ipynb', 'Traffic analytics'],
        ['Notebooks', 'nyc_environmental_analysis.ipynb', 'Weather/AQ analysis'],
        ['Config', 'semantic_views.sql', 'SQL semantic views'],
        ['Setup', 'SETUP_SNOWFLAKE.sql', 'Snowflake DDL']
    ]
    story.append(create_table(structure_data, [1.3*inch, 2.5*inch, 2*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("8. Data Schema", styles['SectionHeader']))
    
    story.append(Paragraph("NYC_CAMERA_DATA Table", styles['SubsectionHeader']))
    camera_schema = [
        ['Column', 'Type', 'Description'],
        ['uuid', 'STRING', 'Unique record ID'],
        ['camera_id', 'STRING', '511NY camera ID'],
        ['name', 'STRING', 'Camera location'],
        ['latitude', 'FLOAT', 'GPS latitude'],
        ['longitude', 'FLOAT', 'GPS longitude'],
        ['direction_of_travel', 'STRING', 'Camera facing direction'],
        ['roadway_name', 'STRING', 'Highway name'],
        ['video_url', 'STRING', 'Live stream URL'],
        ['image_url', 'STRING', 'Snapshot URL'],
        ['disabled', 'BOOLEAN', 'Camera disabled flag'],
        ['blocked', 'BOOLEAN', 'View blocked flag'],
        ['image_timestamp', 'TIMESTAMP', 'Capture time'],
        ['ingest_timestamp', 'TIMESTAMP', 'Ingestion time']
    ]
    story.append(create_table(camera_schema, [1.8*inch, 1.2*inch, 2.8*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("NYC_TRAFFIC_EVENTS Table", styles['SubsectionHeader']))
    events_schema = [
        ['Column', 'Type', 'Description'],
        ['uuid', 'STRING', 'Unique record ID'],
        ['event_id', 'STRING', '511NY event ID'],
        ['event_type', 'STRING', 'incident, construction, etc.'],
        ['event_subtype', 'STRING', 'Event subcategory'],
        ['severity', 'STRING', 'Event severity level'],
        ['roadway_name', 'STRING', 'Affected roadway'],
        ['direction', 'STRING', 'Affected direction'],
        ['description', 'STRING', 'Event description'],
        ['latitude', 'FLOAT', 'GPS latitude'],
        ['longitude', 'FLOAT', 'GPS longitude'],
        ['event_timestamp', 'TIMESTAMP', 'Event time']
    ]
    story.append(create_table(events_schema, [1.8*inch, 1.2*inch, 2.8*inch]))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("NYC_TRAFFIC_SPEEDS Table", styles['SubsectionHeader']))
    speeds_schema = [
        ['Column', 'Type', 'Description'],
        ['uuid', 'STRING', 'Unique record ID'],
        ['segment_id', 'STRING', 'Road segment ID'],
        ['roadway_name', 'STRING', 'Roadway name'],
        ['direction', 'STRING', 'Travel direction'],
        ['from_location', 'STRING', 'Segment start'],
        ['to_location', 'STRING', 'Segment end'],
        ['current_speed', 'FLOAT', 'Current speed (mph)'],
        ['free_flow_speed', 'FLOAT', 'Free flow speed (mph)'],
        ['travel_time', 'FLOAT', 'Travel time (seconds)'],
        ['traffic_timestamp', 'TIMESTAMP', 'Measurement time']
    ]
    story.append(create_table(speeds_schema, [1.8*inch, 1.2*inch, 2.8*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("9. Snowflake Tables", styles['SectionHeader']))
    
    story.append(Paragraph("Traffic Tables", styles['SubsectionHeader']))
    traffic_tables = [
        ['Table', 'Type', 'Description'],
        ['NYC_CAMERA_DATA', 'Standard', 'Camera metadata and URLs'],
        ['NYC_CAMERA_ICEBERG', 'Iceberg', 'Open format camera data'],
        ['NYC_TRAFFIC_EVENTS', 'Standard', 'Incidents and construction'],
        ['NYC_TRAFFIC_SPEEDS', 'Standard', 'Speed sensor data']
    ]
    story.append(create_table(traffic_tables, [2*inch, 1.2*inch, 2.5*inch]))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("Environmental Tables", styles['SubsectionHeader']))
    env_tables = [
        ['Table', 'Type', 'Description'],
        ['NOAAWEATHER', 'Standard', 'NOAA weather observations'],
        ['AQ', 'Iceberg', 'EPA Air Quality Index'],
        ['WEATHER_OBSERVATIONS', 'Standard', 'Extended weather data']
    ]
    story.append(create_table(env_tables, [2*inch, 1.2*inch, 2.5*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("10. SQL Semantic Views", styles['SectionHeader']))
    story.append(Paragraph(
        "Three SQL-based semantic views enable natural language querying via Cortex Analyst:",
        styles['CustomBodyText']
    ))
    story.append(Spacer(1, 0.15*inch))
    
    story.append(Paragraph("NYC_CAMERA_SEMANTIC_VIEW", styles['SubsectionHeader']))
    camera_queries = [
        "How many cameras are there?",
        "What cameras are on I-495?",
        "Which roadways have the most cameras?"
    ]
    for q in camera_queries:
        story.append(Paragraph(f'- "{q}"', styles['CustomBodyText']))
    
    story.append(Paragraph("NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW", styles['SubsectionHeader']))
    event_queries = [
        "How many incidents today?",
        "What construction is happening on I-95?",
        "Show events by severity"
    ]
    for q in event_queries:
        story.append(Paragraph(f'- "{q}"', styles['CustomBodyText']))
    
    story.append(Paragraph("NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW", styles['SubsectionHeader']))
    speed_queries = [
        "What is the average speed?",
        "Where is traffic slowest?",
        "Show congestion by roadway"
    ]
    for q in speed_queries:
        story.append(Paragraph(f'- "{q}"', styles['CustomBodyText']))
    story.append(PageBreak())
    
    story.append(Paragraph("11. Dashboards and Notebooks", styles['SectionHeader']))
    
    story.append(Paragraph("Streamlit Dashboards", styles['SubsectionHeader']))
    dashboards = [
        ['Dashboard', 'File', 'Description'],
        ['Camera Dashboard', 'streamlit_app.py', 'Camera locations, map, statistics'],
        ['Traffic Dashboard', 'traffic_dashboard.py', 'Events, speeds, congestion analysis']
    ]
    story.append(create_table(dashboards, [1.5*inch, 2*inch, 2.3*inch]))
    story.append(Spacer(1, 0.15*inch))
    story.append(Paragraph(
        "Launch: SNOWFLAKE_CONNECTION_NAME=default streamlit run traffic_dashboard.py --server.port 8501",
        styles['CodeBlock']
    ))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("Snowflake Notebooks", styles['SubsectionHeader']))
    notebooks = [
        ['Notebook', 'Description'],
        ['nyc_camera_analytics.ipynb', 'Camera coverage and activity analysis'],
        ['nyc_traffic_analytics.ipynb', 'Traffic events and speed correlation'],
        ['nyc_environmental_analysis.ipynb', 'Weather x Air Quality x Traffic']
    ]
    story.append(create_table(notebooks, [2.5*inch, 3.3*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("12. Example Queries", styles['SectionHeader']))
    
    story.append(Paragraph("Recent Camera Activity", styles['SubsectionHeader']))
    story.append(Paragraph(
        "SELECT camera_id, name, roadway_name, image_timestamp FROM DEMO.DEMO.NYC_CAMERA_DATA "
        "WHERE image_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP()) "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY camera_id ORDER BY image_timestamp DESC) = 1 "
        "ORDER BY image_timestamp DESC LIMIT 20;",
        styles['CodeBlock']
    ))
    
    story.append(Paragraph("Traffic Congestion Hotspots", styles['SubsectionHeader']))
    story.append(Paragraph(
        "SELECT roadway_name, ROUND(AVG(current_speed), 1) as avg_speed, "
        "ROUND(AVG(current_speed / NULLIF(free_flow_speed, 0)) * 100, 1) as flow_pct "
        "FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS WHERE traffic_timestamp >= DATEADD('hour', -6, CURRENT_TIMESTAMP()) "
        "GROUP BY roadway_name ORDER BY flow_pct ASC LIMIT 20;",
        styles['CodeBlock']
    ))
    
    story.append(Paragraph("Air Quality by Region", styles['SubsectionHeader']))
    story.append(Paragraph(
        "SELECT reportingarea, parametername, ROUND(AVG(aqi), 1) as avg_aqi, "
        "MODE(categoryname) as typical_category FROM DEMO.DEMO.AQ "
        "GROUP BY reportingarea, parametername ORDER BY avg_aqi DESC;",
        styles['CodeBlock']
    ))
    story.append(PageBreak())
    
    story.append(Paragraph("13. Environmental Data Integration", styles['SectionHeader']))
    
    story.append(Paragraph("Weather Data (NOAA)", styles['SubsectionHeader']))
    weather_fields = [
        "Temperature (F/C)",
        "Relative humidity",
        "Wind speed and direction",
        "Visibility",
        "Barometric pressure",
        "Weather conditions (Clear, Rain, Snow, etc.)"
    ]
    for field in weather_fields:
        story.append(Paragraph(f"- {field}", styles['CustomBodyText']))
    
    story.append(Paragraph("Air Quality Data (EPA)", styles['SubsectionHeader']))
    aq_fields = [
        "Air Quality Index (AQI) values",
        "Pollutant types (PM2.5, PM10, Ozone)",
        "Category classifications (Good, Moderate, Unhealthy)",
        "Geographic reporting areas"
    ]
    for field in aq_fields:
        story.append(Paragraph(f"- {field}", styles['CustomBodyText']))
    
    story.append(Paragraph("Correlation Analysis", styles['SubsectionHeader']))
    story.append(Paragraph(
        "The nyc_environmental_analysis.ipynb notebook correlates weather impact on visibility and driving conditions, "
        "air quality health advisories, and traffic patterns during weather events.",
        styles['CustomBodyText']
    ))
    story.append(PageBreak())
    
    story.append(Paragraph("14. API Reference", styles['SectionHeader']))
    
    story.append(Paragraph("Snowpipe Streaming v2 REST API", styles['SubsectionHeader']))
    api_data = [
        ['Endpoint', 'Method', 'Description'],
        ['/v1/streaming/channels', 'POST', 'Open channel'],
        ['/v1/streaming/channels/{id}/append', 'POST', 'Append rows'],
        ['/v1/streaming/channels/{id}', 'DELETE', 'Close channel']
    ]
    story.append(create_table(api_data, [2.5*inch, 1*inch, 2.3*inch]))
    story.append(Spacer(1, 0.25*inch))
    
    story.append(Paragraph("511NY API", styles['SubsectionHeader']))
    ny_api = [
        ['Endpoint', 'Description'],
        ['/api/getevents', 'Traffic events'],
        ['/api/getcameras', 'Camera list'],
        ['/api/getspeeds', 'Speed data']
    ]
    story.append(create_table(ny_api, [2.5*inch, 3.3*inch]))
    story.append(PageBreak())
    
    story.append(Paragraph("15. Troubleshooting", styles['SectionHeader']))
    troubleshoot_data = [
        ['Issue', 'Solution'],
        ['401 Unauthorized', 'Check PAT token in config, regenerate if expired'],
        ['STALE_CONTINUATION_TOKEN', 'Channel needs reopening, restart application'],
        ['Connection refused PostgreSQL', 'Verify host/port, check pg_hba.conf'],
        ['Empty camera data', 'Check 511NY API key validity'],
        ['Slack upload fails', 'Verify bot has files:write scope'],
        ['Semantic view errors', 'Run semantic_views.sql to recreate']
    ]
    story.append(create_table(troubleshoot_data, [2.3*inch, 3.5*inch]))
    story.append(PageBreak())
    
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph(
        '"All those moments will be lost in time, like tears in rain... unless you stream them to Snowflake."',
        styles['Quote']
    ))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("Built with Snowflake + Python + Cortex AI", styles['CoverSubtitle']))
    story.append(Paragraph("Tyrell Corporation Data Systems - More Human Than Human", styles['CoverSubtitle']))
    
    doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)
    print(f"PDF generated: {output_path}")

if __name__ == "__main__":
    output_file = "/Users/tspann/Downloads/code/coco/nyccamera/NYC_Camera_Pipeline_Documentation.pdf"
    build_pdf(output_file)
