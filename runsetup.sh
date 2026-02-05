# ============================================================================
# NYC Street Camera Pipeline - Quick Start Script
# ============================================================================
# "I've seen things you people wouldn't believe..."
#
# This script runs the NYC camera streaming pipeline with all integrations.
# Make sure to set your credentials before running.
# ============================================================================

# source .env

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

echo "HOST: $PG_HOST"

python setup_tables.py --pg-host $PG_HOST --pg-user $PG_USER --pg-password $PG_PASSWORD --pg-only --external-volume TRANSCOM_TSPANNICEBERG_EXTVOL --snowflake-connection tspann1

