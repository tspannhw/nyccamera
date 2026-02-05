"""NYC Traffic Intelligence Platform.

A real-time traffic data pipeline using Snowflake Snowpipe Streaming,
Cortex AI for semantic analysis, and geospatial analytics.
"""

__version__ = "0.1.0"

from .config import SnowflakeConfig, load_config
from .streaming import SnowpipeStreamingClient
from .sensors import CameraSensor, TrafficEventsSensor
from .analytics import TrafficAnalytics

__all__ = [
    "SnowflakeConfig",
    "load_config",
    "SnowpipeStreamingClient",
    "CameraSensor",
    "TrafficEventsSensor",
    "TrafficAnalytics",
]
