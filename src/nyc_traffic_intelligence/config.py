"""Configuration management for NYC Traffic Intelligence Platform."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    
    account: str
    user: str
    database: str = "DEMO"
    schema: str = "DEMO"
    warehouse: str = "INGEST"
    role: str = "ACCOUNTADMIN"
    private_key_path: Optional[str] = None
    password: Optional[str] = None
    authenticator: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load configuration from environment variables."""
        load_dotenv()
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            database=os.getenv("SNOWFLAKE_DATABASE", "DEMO"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "DEMO"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "INGEST"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            private_key_path=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        )
    
    @classmethod
    def from_json(cls, path: str | Path) -> "SnowflakeConfig":
        """Load configuration from JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls(**data)
    
    @classmethod
    def from_connection_name(cls, connection_name: str) -> "SnowflakeConfig":
        """Load from Snowflake connection name (for use with snowflake-connector)."""
        return cls(
            account="",
            user="",
            authenticator=f"connection:{connection_name}",
        )


@dataclass
class APIConfig:
    """External API configuration."""
    
    nyc_camera_url: str = "https://webcams.nyctmc.org/api/cameras"
    nyc_events_url: str = "https://511ny.org/api/getevents"
    nyc_speeds_url: str = "https://511ny.org/api/getspeeds"
    noaa_weather_url: str = "https://api.weather.gov"
    slack_webhook_url: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "APIConfig":
        """Load API configuration from environment."""
        load_dotenv()
        return cls(
            nyc_camera_url=os.getenv("NYC_CAMERA_URL", cls.nyc_camera_url),
            nyc_events_url=os.getenv("NYC_EVENTS_URL", cls.nyc_events_url),
            nyc_speeds_url=os.getenv("NYC_SPEEDS_URL", cls.nyc_speeds_url),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        )


@dataclass
class PlatformConfig:
    """Complete platform configuration."""
    
    snowflake: SnowflakeConfig
    api: APIConfig = field(default_factory=APIConfig)
    poll_interval_seconds: int = 60
    batch_size: int = 100
    enable_slack_notifications: bool = False


def load_config(
    config_path: Optional[str | Path] = None,
    connection_name: Optional[str] = None,
) -> PlatformConfig:
    """Load platform configuration.
    
    Args:
        config_path: Path to JSON configuration file.
        connection_name: Snowflake connection name to use.
        
    Returns:
        Complete platform configuration.
    """
    if connection_name:
        snowflake_config = SnowflakeConfig.from_connection_name(connection_name)
    elif config_path:
        snowflake_config = SnowflakeConfig.from_json(config_path)
    else:
        snowflake_config = SnowflakeConfig.from_env()
    
    return PlatformConfig(
        snowflake=snowflake_config,
        api=APIConfig.from_env(),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        batch_size=int(os.getenv("BATCH_SIZE", "100")),
        enable_slack_notifications=os.getenv("ENABLE_SLACK", "false").lower() == "true",
    )
