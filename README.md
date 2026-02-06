# 🌃 NYC Street Camera Data Pipeline

> *"I've seen things you people wouldn't believe... Traffic cameras on fire off the shoulder of I-495..."*

Stream NYC traffic camera data to Snowflake via **Snowpipe Streaming High Speed v2 REST API**, with optional PostgreSQL storage and Slack notifications. A real-time data engineering pipeline for the cyberpunk age.

![Architecture](architecture_bladerunner.png)

---

## 📖 Table of Contents

1. [Architecture](#-architecture)
2. [Features](#-features)
3. [Data Sources](#-data-sources)
4. [System Components](#-system-components)
5. [Prerequisites](#-prerequisites)
6. [Quick Start](#-quick-start)
7. [Project Structure](#-project-structure)
8. [Data Schema](#-data-schema)
9. [Snowflake Tables](#-snowflake-tables)
10. [SQL Semantic Views](#-sql-semantic-views)
11. [Dashboards & Notebooks](#-dashboards--notebooks)
12. [Example Queries](#-example-queries)
13. [Environmental Data Integration](#-environmental-data-integration)
14. [API Reference](#-api-reference)
15. [Troubleshooting](#-troubleshooting)
16. [References](#-references)

---

## 🏙️ Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                     NYC CAMERA SURVEILLANCE NETWORK v2.0                             ║
║                         [ TYRELL CORP DATA SYSTEMS ]                                 ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                      ║
║    ┌─────────────────────────────────────────────────────────────────────────────┐   ║
║    │                        ◈ DATA ACQUISITION LAYER ◈                           │   ║
║    │  ╭─────────────╮  ╭─────────────╮  ╭─────────────╮  ╭─────────────╮         │   ║
║    │  │ ◉ 511NY API │  │ 🌤️ NOAA     │  │ 🌬️ EPA      │  │ 📡 SENSORS  │         │   ║
║    │  │   CAMERAS   │  │  WEATHER    │  │  AIR QUAL   │  │   IoT DATA  │         │   ║
║    │  │  [LIVE FEED]│  │ [STATIONS]  │  │   [AQI]     │  │  [STREAMS]  │         │   ║
║    │  ╰─────────────╯  ╰─────────────╯  ╰─────────────╯  ╰─────────────╯         │   ║
║    └─────────────────────────────────────────────────────────────────────────────┘   ║
║                                        ║                                             ║
║                                        ▼                                             ║
║    ┌─────────────────────────────────────────────────────────────────────────────┐   ║
║    │                      ◈ PROCESSING ENGINE ◈                                  │   ║
║    │  ╭───────────────────────────────────────────────────────────────────────╮  │   ║
║    │  │                    PYTHON STREAMING CLIENT                            │  │   ║
║    │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │  │   ║
║    │  │  │ JWT/PAT AUTH │  │ REST CLIENT  │  │ IMAGE PROC   │                 │  │   ║
║    │  │  │   MODULE     │  │  SNOWPIPE v2 │  │   ENGINE     │                 │  │   ║
║    │  │  └──────────────┘  └──────────────┘  └──────────────┘                 │  │   ║
║    │  ╰───────────────────────────────────────────────────────────────────────╯  │   ║
║    └─────────────────────────────────────────────────────────────────────────────┘   ║
║                      ║                ║                 ║                            ║
║                      ▼                ▼                 ▼                            ║
║    ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐              ║
║    │    ❄ SNOWFLAKE    │ │   🐘 POSTGRESQL   │ │     💬 SLACK         │              ║
║    │  ╭──────────────╮  │ │  ╭──────────────╮  │ │  ╭──────────────╮  │              ║
║    │  │  STANDARD    │  │ │  │   METADATA   │  │ │  │    IMAGE     │  │              ║
║    │  │   TABLE      │  │ │  │    STORE     │  │ │  │  ALERTS      │  │              ║
║    │  ├──────────────┤  │ │  │   BACKUP     │  │ │  │  REAL-TIME   │  │              ║
║    │  │   ICEBERG    │  │ │  ╰──────────────╯  │ │  ╰──────────────╯  │              ║
║    │  │   TABLE      │  │ └────────────────────┘ └────────────────────┘              ║
║    │  ├──────────────┤  │                                                            ║
║    │  │  CORTEX AI   │  │         ┌────────────────────────────────┐                 ║
║    │  │  ANALYSIS    │  │         │      📊 VISUALIZATION          │                 ║
║    │  ├──────────────┤  │         │  ╭────────────╮ ╭────────────╮ │                 ║
║    │  │  SEMANTIC    │  │         │  │ STREAMLIT  │ │  NOTEBOOK  │ │                 ║
║    │  │   VIEWS      │  │         │  │ DASHBOARD  │ │  ANALYTICS │ │                 ║
║    │  ╰──────────────╯  │         │  ╰────────────╯ ╰────────────╯ │                 ║
║    └────────────────────┘         └────────────────────────────────┘                 ║
║                                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
                            ◈ REPLICANT APPROVED ◈
```

---

## ⚡ Features

| Feature | Description | Status |
|---------|-------------|--------|
| 🚀 **Real-time Streaming** | Snowpipe Streaming v2 REST API (10GB/s capable) | ✅ Active |
| 🧊 **Iceberg Tables** | Open table format for data lakehouse | ✅ Active |
| 🐘 **PostgreSQL Backup** | Redundant metadata storage | ✅ Optional |
| 💬 **Slack Alerts** | Real-time image notifications | ✅ Optional |
| 🤖 **Cortex AI** | Claude-powered image analysis | ✅ Ready |
| 📊 **Streamlit Dashboard** | Interactive web visualization | ✅ Active |
| 📓 **Snowflake Notebooks** | In-platform analytics (3 notebooks) | ✅ Active |
| 🗣️ **SQL Semantic Views** | Natural language queries with CREATE SEMANTIC VIEW | ✅ Active |
| 🚨 **Traffic Events** | 511NY incidents & construction | ✅ Active |
| 🏎️ **Traffic Speeds** | Real-time congestion data | ✅ Active |
| 🌤️ **Weather Integration** | NOAA weather station data | ✅ Active |
| 🌬️ **Air Quality** | EPA AQI monitoring data | ✅ Active |

---

## 📡 Data Sources

### Primary Sources (511NY API)

| Source | Endpoint | Data Type | Refresh Rate |
|--------|----------|-----------|--------------|
| **Cameras** | `/cameras` | Live camera feeds | 60 seconds |
| **Traffic Events** | `/events` | Incidents, construction | 60 seconds |
| **Traffic Speeds** | `/speeds` | Real-time segment speeds | 60 seconds |

### Environmental Sources

| Source | Table | Description | Coverage |
|--------|-------|-------------|----------|
| **NOAA Weather** | `NOAAWEATHER` | Weather observations | National |
| **EPA AirNow** | `AQ` | Air Quality Index | Regional |
| **Weather Observations** | `WEATHER_OBSERVATIONS` | Extended weather data | National |

---

## 🏗️ System Components

### Data Flow

```
[511NY API] ──HTTP GET──▶ [Python Client] ──REST POST──▶ [Snowpipe Streaming v2]
     │                          │                               │
     │                          ├──INSERT──▶ [PostgreSQL]       │
     │                          │                               │
     │                          └──UPLOAD──▶ [Slack]            │
     │                                                          │
     │                                                          ▼
     │                                              [Snowflake Tables]
     │                                                    │
     │                                    ┌───────────────┼───────────────┐
     │                                    │               │               │
     │                                    ▼               ▼               ▼
     │                              [Standard]      [Iceberg]     [Semantic]
     │                                Table           Table         Views
     │                                    │
     │                                    ▼
     │                              [Cortex AI]
     │                             Image Analysis
     │                                    │
     └────────────────────────────────────┴──▶ [Dashboards & Notebooks]
```

### Component Details

#### 1. Data Acquisition (`nyc_camera_sensor.py`)
- Connects to 511NY traffic camera API
- Fetches ~2000+ camera feeds across NYC metropolitan area
- Filters disabled/blocked cameras
- Extracts: location, direction, roadway, URLs

#### 2. Traffic Events (`nyc_traffic_events_sensor.py`)
- Real-time traffic incidents and construction
- Speed data from traffic sensors
- Event severity classification
- Geographic coverage mapping

#### 3. Streaming Client (`snowpipe_streaming_client.py`)
- Implements Snowpipe Streaming v2 REST API
- JWT/PAT authentication via `snowflake_jwt_auth.py`
- Channel management (open, append, close)
- Offset token tracking for exactly-once delivery

#### 4. Storage Layer
- **Snowflake Standard Table**: Primary storage for real-time queries
- **Snowflake Iceberg Table**: Open format for interoperability
- **PostgreSQL**: Optional backup/redundancy

#### 5. AI Analysis (Cortex)
- `claude-3-5-sonnet` for multimodal image analysis
- Traffic condition assessment
- Weather/visibility detection
- Vehicle counting

---

## 📋 Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.11+ | Required |
| uv | 0.4+ | Fast Python package manager |
| Snowflake Account | Enterprise+ | Snowpipe Streaming enabled |
| 511NY API Key | - | Free at https://511ny.org/ |
| PostgreSQL | 12+ | Optional |
| Slack Bot Token | - | Optional, `xoxb-*` format |

### Install uv (if not already installed)

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv
```

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/tspannhw/nyccamera.git
cd nyccamera

# Install with uv (recommended - fast!)
uv sync

# Or install with optional dependencies
uv sync --extra dev --extra notebook

# Alternative: pip install (slower)
pip install -e .
```

### 2. Configure Snowflake

**Option A: Environment Variables (recommended)**

```bash
# Copy the example env file
cp .env.example .env

# Edit with your credentials
vim .env
```

```env
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DEMO
SNOWFLAKE_SCHEMA=DEMO
SNOWFLAKE_WAREHOUSE=INGEST
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

**Option B: JSON Config File**

```bash
cp snowflake_config.example.json snowflake_config.json
vim snowflake_config.json
```

```json
{
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "database": "DEMO",
    "schema": "DEMO",
    "warehouse": "INGEST",
    "role": "ACCOUNTADMIN",
    "private_key_path": "/path/to/rsa_key.p8"
}
```

### 3. Initialize Database Objects

```bash
# Setup both PostgreSQL and Snowflake
python setup_tables.py \
    --pg-host localhost --pg-user postgres --pg-password secret \
    --external-volume TRANSCOM_TSPANNICEBERG_EXTVOL

# Or Snowflake only
python setup_tables.py --snowflake-only
```

### 4. Run the Pipeline

```bash
# Set required environment variables
export NYC_API_KEY="your_511ny_api_key"

# Run with uv (automatically uses virtual environment)
uv run python nyc_camera_main.py

# Or use the shell script
./run_nyc_camera.sh

# Full integration (PostgreSQL + Slack)
export SLACK_TOKEN="xoxb-your-token"
export PG_HOST="localhost"
export PG_USER="postgres" 
export PG_PASSWORD="password"
uv run python nyc_camera_main.py --send-images

# Fast mode (30 second intervals)
uv run python nyc_camera_main.py --fast
```

---

## 📁 Project Structure

```
nyccamera/
├── 📦 Project Configuration (uv)
│   ├── pyproject.toml                  # uv/Python project config
│   ├── .python-version                 # Python version pin
│   ├── .env.example                    # Environment template
│   └── snowflake_config.example.json   # Config template
│
├── 🐍 Core Application
│   ├── nyc_camera_main.py              # Main entry point
│   ├── nyc_camera_sensor.py            # 511NY camera API client
│   ├── nyc_traffic_events_sensor.py    # 511NY events & speeds client
│   ├── snowpipe_streaming_client.py    # Snowpipe v2 REST client
│   └── snowflake_jwt_auth.py           # Authentication module
│
├── 📚 Package Module (src/nyc_traffic_intelligence/)
│   ├── __init__.py                     # Package exports
│   ├── config.py                       # Configuration management
│   ├── sensors.py                      # Data collection sensors
│   └── streaming.py                    # Snowpipe streaming client
│
├── 🔌 Integrations
│   ├── postgresql_client.py            # PostgreSQL storage
│   └── slack_notifier.py               # Slack notifications
│
├── 📊 Visualization & Analytics
│   ├── streamlit_app.py                # Camera dashboard
│   ├── traffic_dashboard.py            # Full traffic dashboard
│   ├── nyc_camera_analytics.ipynb      # Camera analytics notebook
│   ├── nyc_traffic_analytics.ipynb     # Traffic analytics notebook
│   └── nyc_environmental_analysis.ipynb # Weather & Air Quality notebook
│
├── 🗄️ SQL Scripts
│   ├── SETUP_SNOWFLAKE.sql             # Snowflake DDL
│   ├── sql_semantic_views.sql          # Semantic view definitions
│   ├── geospatial_functions.sql        # Geospatial functions & views
│   └── ai_sql_functions.sql            # Cortex AI SQL functions
│
└── 📖 Documentation
    ├── README.md                       # This file
    └── architecture_bladerunner.png    # Architecture diagram
```

---

## 📊 Data Schema

### NYC_CAMERA_DATA Table

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `uuid` | STRING | Unique record ID | `a1b2c3d4-...` |
| `camera_id` | STRING | 511NY camera ID | `1234` |
| `name` | STRING | Camera location | `I-495 at Exit 32` |
| `latitude` | FLOAT | GPS latitude | `40.7589` |
| `longitude` | FLOAT | GPS longitude | `-73.9851` |
| `direction_of_travel` | STRING | Camera facing | `Eastbound` |
| `roadway_name` | STRING | Highway name | `I-495` |
| `video_url` | STRING | Live stream URL | `https://...` |
| `image_url` | STRING | Snapshot URL | `https://...` |
| `disabled` | BOOLEAN | Camera disabled | `false` |
| `blocked` | BOOLEAN | View blocked | `false` |
| `image_timestamp` | TIMESTAMP | Capture time | `2025-02-04 12:00:00` |
| `ingest_timestamp` | TIMESTAMP | Ingestion time | `2025-02-04 12:00:01` |
| `hostname` | STRING | Ingestion server | `edge-node-1` |
| `ip_address` | STRING | Server IP | `192.168.1.100` |

### NYC_TRAFFIC_EVENTS Table

| Column | Type | Description |
|--------|------|-------------|
| `uuid` | STRING | Unique record ID |
| `event_id` | STRING | 511NY event ID |
| `event_type` | STRING | incident, construction, etc. |
| `event_subtype` | STRING | Event subcategory |
| `severity` | STRING | Event severity level |
| `roadway_name` | STRING | Affected roadway |
| `direction` | STRING | Affected direction |
| `description` | STRING | Event description |
| `location` | STRING | Location description |
| `latitude` | FLOAT | GPS latitude |
| `longitude` | FLOAT | GPS longitude |
| `event_timestamp` | TIMESTAMP | Event time |

### NYC_TRAFFIC_SPEEDS Table

| Column | Type | Description |
|--------|------|-------------|
| `uuid` | STRING | Unique record ID |
| `segment_id` | STRING | Road segment ID |
| `link_id` | STRING | Link identifier |
| `roadway_name` | STRING | Roadway name |
| `direction` | STRING | Travel direction |
| `from_location` | STRING | Segment start |
| `to_location` | STRING | Segment end |
| `current_speed` | FLOAT | Current speed (mph) |
| `free_flow_speed` | FLOAT | Free flow speed (mph) |
| `travel_time` | FLOAT | Travel time (seconds) |
| `traffic_timestamp` | TIMESTAMP | Measurement time |

---

## ❄️ Snowflake Tables

### Traffic Tables

| Table | Type | Description | Records |
|-------|------|-------------|---------|
| `NYC_CAMERA_DATA` | Standard | Camera metadata & URLs | Real-time |
| `NYC_CAMERA_ICEBERG` | Iceberg | Open format camera data | Real-time |
| `NYC_TRAFFIC_EVENTS` | Standard | Incidents & construction | Real-time |
| `NYC_TRAFFIC_SPEEDS` | Standard | Speed sensor data | Real-time |

### Environmental Tables

| Table | Type | Description | Records |
|-------|------|-------------|---------|
| `NOAAWEATHER` | Standard | NOAA weather observations | 6,325+ |
| `AQ` | Iceberg | EPA Air Quality Index | 1,392+ |
| `WEATHER_OBSERVATIONS` | Standard | Extended weather data | 6,033+ |

---

## 🗣️ SQL Semantic Views

Three SQL-based semantic views for natural language querying via Cortex Analyst:

### NYC_CAMERA_SEMANTIC_VIEW

```sql
CREATE OR REPLACE SEMANTIC VIEW NYC_CAMERA_SEMANTIC_VIEW
  TABLES (cameras AS DEMO.DEMO.NYC_CAMERA_DATA PRIMARY KEY (uuid))
  DIMENSIONS (
    cameras.camera_id, cameras.camera_name, cameras.roadway,
    cameras.direction, cameras.latitude, cameras.longitude
  )
  METRICS (
    cameras.total_records AS COUNT(*),
    cameras.unique_cameras AS COUNT(DISTINCT camera_id)
  );
```

**Supported Questions:**
- "How many cameras are there?"
- "What cameras are on I-495?"
- "Which roadways have the most cameras?"

### NYC_TRAFFIC_EVENTS_SEMANTIC_VIEW

**Supported Questions:**
- "How many incidents today?"
- "What construction is happening on I-95?"
- "Show events by severity"

### NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW

**Supported Questions:**
- "What is the average speed?"
- "Where is traffic slowest?"
- "Show congestion by roadway"

**Query Example:**
```sql
SELECT * FROM SEMANTIC_VIEW(
  NYC_TRAFFIC_SPEEDS_SEMANTIC_VIEW
  DIMENSIONS speeds.roadway, speeds.direction
  METRICS speeds.avg_current_speed, speeds.avg_congestion
) ORDER BY avg_congestion ASC;
```

---

## 📊 Dashboards & Notebooks

### Streamlit Dashboards

| Dashboard | File | Description |
|-----------|------|-------------|
| **Camera Dashboard** | `streamlit_app.py` | Camera locations, map, statistics |
| **Traffic Dashboard** | `traffic_dashboard.py` | Events, speeds, congestion analysis |

**Launch:**
```bash
# Using uv
uv run streamlit run traffic_dashboard.py --server.port 8501

# Or with connection name
SNOWFLAKE_CONNECTION_NAME=default uv run streamlit run traffic_dashboard.py
```

### Snowflake Notebooks

| Notebook | Description |
|----------|-------------|
| `nyc_camera_analytics.ipynb` | Camera coverage & activity analysis |
| `nyc_traffic_analytics.ipynb` | Traffic events & speed correlation |
| `nyc_environmental_analysis.ipynb` | Weather × Air Quality × Traffic |

---

## 🔍 Example Queries

### Recent Camera Activity
```sql
SELECT camera_id, name, roadway_name, image_timestamp
FROM DEMO.DEMO.NYC_CAMERA_DATA
WHERE image_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
QUALIFY ROW_NUMBER() OVER (PARTITION BY camera_id ORDER BY image_timestamp DESC) = 1
ORDER BY image_timestamp DESC
LIMIT 20;
```

### Traffic Congestion Hotspots
```sql
SELECT 
    roadway_name,
    ROUND(AVG(current_speed), 1) as avg_speed,
    ROUND(AVG(current_speed / NULLIF(free_flow_speed, 0)) * 100, 1) as flow_pct
FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS
WHERE traffic_timestamp >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
GROUP BY roadway_name
ORDER BY flow_pct ASC
LIMIT 20;
```

### Weather Conditions
```sql
SELECT 
    location, weather, temp_f, wind_mph, visibility_mi
FROM DEMO.DEMO.NOAAWEATHER
QUALIFY ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY updated_at DESC) = 1
ORDER BY location;
```

### Air Quality by Region
```sql
SELECT 
    reportingarea, parametername,
    ROUND(AVG(aqi), 1) as avg_aqi,
    MODE(categoryname) as typical_category
FROM DEMO.DEMO.AQ
GROUP BY reportingarea, parametername
ORDER BY avg_aqi DESC;
```

---

## 🌍 Environmental Data Integration

### Weather Data (NOAA)

The `NOAAWEATHER` table provides:
- Temperature (°F/°C)
- Relative humidity
- Wind speed and direction
- Visibility
- Barometric pressure
- Weather conditions (Clear, Rain, Snow, etc.)

### Air Quality Data (EPA)

The `AQ` table provides:
- Air Quality Index (AQI) values
- Pollutant types (PM2.5, PM10, Ozone)
- Category classifications (Good, Moderate, Unhealthy)
- Geographic reporting areas

### Correlation Analysis

The `nyc_environmental_analysis.ipynb` notebook correlates:
- Weather impact on visibility/driving conditions
- Air quality health advisories
- Traffic patterns during weather events

---

## 🔌 API Reference

### Snowpipe Streaming v2 REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/streaming/channels` | POST | Open channel |
| `/v1/streaming/channels/{id}/append` | POST | Append rows |
| `/v1/streaming/channels/{id}` | DELETE | Close channel |

### 511NY API

| Endpoint | Description |
|----------|-------------|
| `/api/getevents` | Traffic events |
| `/api/getcameras` | Camera list |
| `/api/getspeeds` | Speed data |

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| `401 Unauthorized` | Check PAT token in config, regenerate if expired |
| `STALE_CONTINUATION_TOKEN` | Channel needs reopening, restart application |
| `Connection refused` PostgreSQL | Verify host/port, check pg_hba.conf |
| Empty camera data | Check 511NY API key validity |
| Slack upload fails | Verify bot has `files:write` scope |
| Semantic view errors | Run `semantic_views.sql` to recreate |

---

## 📚 References

### Snowflake Documentation
- [Snowpipe Streaming v2 REST API](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api)
- [Snowpipe Streaming High-Performance Overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [SQL Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/sql)
- [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Snowflake Postgresql](https://www.snowflake.com/en/product/features/postgres/)

### Data Sources
- [511NY Traffic API](https://511ny.org/)
- [NOAA Weather API](https://www.weather.gov/documentation/services-web-api)
- [EPA AirNow API](https://docs.airnowapi.org/)

### Related Projects
- [SNACKAI-CoCo-ADSB](https://github.com/tspannhw/SNACKAI-CoCo-ADSB) - Similar ADSB implementation
- [TrafficAI](https://github.com/tspannhw/TrafficAI) - NiFi-based traffic pipeline
- [AIM-NYCStreetCams](https://github.com/tspannhw/AIM-NYCStreetCams) - Milvus vector search

---

## 📄 License

Apache 2.0

---

## 🎬 Quotes

> *"All those moments will be lost in time, like tears in rain... unless you stream them to Snowflake."*

> *"I've seen things you people wouldn't believe... Attack ships on fire off the shoulder of Orion. I watched C-beams glitter in the dark near the Tannhäuser Gate. All those moments will be lost in time, like tears in rain... Time to stream."*

> *"The light that burns twice as bright burns half as long - and you have burned so very, very brightly, Roy. Now stream your data before it's gone."*

---

---

## 🛠️ Development

### Using uv for Development

```bash
# Install with dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Run type checker
uv run mypy .

# Add a new dependency
uv add requests

# Add a dev dependency
uv add --dev pytest-asyncio

# Update all dependencies
uv sync --upgrade
```

### Package Scripts

```bash
# Run camera sensor (defined in pyproject.toml)
uv run nyc-camera

# Run events sensor
uv run nyc-events

# Run dashboard
uv run nyc-dashboard
```

### Virtual Environment

```bash
# uv automatically manages .venv
# To activate manually (if needed):
source .venv/bin/activate

# Check Python version
uv run python --version
```

---

### Example Run

https://github.com/tspannhw/nyccamera/blob/main/run.md



**Built with ❄️ Snowflake + 🐍 Python + 🤖 Cortex AI + ⚡ uv + 🌃 Blade Runner Aesthetic**

*TimSpann Corporation Data Systems - More Human Than Human*
