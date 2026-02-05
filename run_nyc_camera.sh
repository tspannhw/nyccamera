#!/bin/bash
# NYC Camera Streaming Pipeline Runner
# =====================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables if .env exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

CONFIG_FILE="${CONFIG_FILE:-snowflake_config.json}"
API_KEY="${NYC_API_KEY:-}"
INTERVAL="${INTERVAL:-60}"

# Optional integrations
SLACK_TOKEN="${SLACK_BOT_TOKEN:-$SLACK_TOKEN}"
SLACK_CHANNEL="${SLACK_CHANNEL:-#traffic-cameras}"
PG_HOST="${PG_HOST:-}"
PG_PORT="${PG_PORT:-5432}"
PG_DATABASE="${PG_DATABASE:-nyccamera}"
PG_USER="${PG_USER:-}"
PG_PASSWORD="${PG_PASSWORD:-}"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Environment Variables:"
    echo "  NYC_API_KEY      - 511NY API key (required)"
    echo "  CONFIG_FILE      - Snowflake config file (default: snowflake_config.json)"
    echo "  INTERVAL         - Seconds between batches (default: 60)"
    echo "  SLACK_BOT_TOKEN  - Slack bot token (optional)"
    echo "  SLACK_CHANNEL    - Slack channel (default: #traffic-cameras)"
    echo "  PG_HOST          - PostgreSQL host (optional)"
    echo "  PG_PORT          - PostgreSQL port (default: 5432)"
    echo "  PG_DATABASE      - PostgreSQL database (default: nyccamera)"
    echo "  PG_USER          - PostgreSQL user (optional)"
    echo "  PG_PASSWORD      - PostgreSQL password (optional)"
    echo ""
    echo "Options:"
    echo "  --fast           Run with 30 second interval"
    echo "  --send-images    Download and send images to Slack"
    echo "  -h, --help       Show this help message"
}

EXTRA_ARGS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --fast)
            INTERVAL=30
            shift
            ;;
        --send-images)
            EXTRA_ARGS="$EXTRA_ARGS --send-images"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$API_KEY" ]; then
    echo "Error: NYC_API_KEY environment variable is required"
    echo "Get your API key from https://511ny.org/"
    exit 1
fi

# Check if uv is available, otherwise fall back to python3
if command -v uv &> /dev/null; then
    PYTHON_CMD="uv run python"
else
    PYTHON_CMD="python3"
    echo "Warning: uv not found, using system python3"
    echo "Install uv for better dependency management: curl -LsSf https://astral.sh/uv/install.sh | sh"
fi

CMD="$PYTHON_CMD nyc_camera_main.py --config $CONFIG_FILE --api-key $API_KEY --interval $INTERVAL"

if [ -n "$SLACK_TOKEN" ]; then
    CMD="$CMD --slack-token $SLACK_TOKEN --slack-channel $SLACK_CHANNEL"
fi

if [ -n "$PG_HOST" ] && [ -n "$PG_USER" ] && [ -n "$PG_PASSWORD" ]; then
    CMD="$CMD --pg-host $PG_HOST --pg-port $PG_PORT --pg-database $PG_DATABASE"
    CMD="$CMD --pg-user $PG_USER --pg-password $PG_PASSWORD"
fi

CMD="$CMD $EXTRA_ARGS"

echo "Starting NYC Camera Streaming Pipeline..."
echo "Command: $CMD"
echo ""

exec $CMD
