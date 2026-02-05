# Setup both PostgreSQL and Snowflake
#python setup_tables.py \
#    --pg-host [localhost](https://localhost) --pg-user postgres --pg-password secret \
#    --external-volume TRANSCOM_TSPANNICEBERG_EXTVOL

# PostgreSQL only
#python setup_tables.py --pg-only \
#    --pg-host [localhost](https://localhost) --pg-user postgres --pg-password secret

# Snowflake only (with Iceberg)
#python setup_tables.py --snowflake-only --external-volume TRANSCOM_TSPANNICEBERG_EXTVOL

# Snowflake only (without Iceberg)
#python setup_tables.py --snowflake-only

#!/bin/bash
# ============================================================================
# NYC Street Camera Pipeline - Quick Start Script
# ============================================================================
# "I've seen things you people wouldn't believe..."
#
# This script runs the NYC camera streaming pipeline with all integrations.
# Make sure to set your credentials before running.
# ============================================================================

#source .env

#set -e

# Required: 511NY API Key (get from https://511ny.org/)
#export NYC_API_KEY="${NYC_API_KEY:-your_511ny_api_key}"

# Optional: Slack integration
#export SLACK_TOKEN="${SLACK_TOKEN:-}"
#export SLACK_CHANNEL="${SLACK_CHANNEL:-#traffic-cameras}"

# Optional: PostgreSQL integration
#export PG_HOST="${PG_HOST:-}"
#export PG_PORT="${PG_PORT:-5432}"
#export PG_DATABASE="${PG_DATABASE:-nyccamera}"
#export PG_USER="${PG_USER:-}"
#export PG_PASSWORD="${PG_PASSWORD:-}"

# Streaming interval (seconds)
#export INTERVAL="${INTERVAL:-60}"

# ============================================================================
# Run the pipeline
# ============================================================================

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║           NYC CAMERA SURVEILLANCE NETWORK v2.0                   ║"
echo "║               SNOWPIPE STREAMING HIGH SPEED                      ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Check for API key
if [ "$NYC_API_KEY" = "your_511ny_api_key" ]; then
    echo "ERROR: Please set NYC_API_KEY environment variable"
    echo "       Get your API key from https://511ny.org/"
    exit 1
fi

# Build command
CMD="python3 nyc_camera_main.py --api-key $NYC_API_KEY --interval $INTERVAL"

# Add Slack if configured
if [ -n "$SLACK_TOKEN" ]; then
    CMD="$CMD --slack-token $SLACK_TOKEN --slack-channel $SLACK_CHANNEL"
    echo "✓ Slack integration enabled"
fi

# Add PostgreSQL if configured
if [ -n "$PG_HOST" ] && [ -n "$PG_USER" ] && [ -n "$PG_PASSWORD" ]; then
    CMD="$CMD --pg-host $PG_HOST --pg-port $PG_PORT --pg-database $PG_DATABASE"
    CMD="$CMD --pg-user $PG_USER --pg-password $PG_PASSWORD"
    echo "✓ PostgreSQL integration enabled"
fi

# Add image sending if Slack is configured
if [ -n "$SLACK_TOKEN" ]; then
    CMD="$CMD --send-images"
fi

echo ""
echo "Starting pipeline..."
echo "Interval: ${INTERVAL}s"
echo ""

exec $CMD