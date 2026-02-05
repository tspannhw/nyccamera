"""Data sensors for collecting NYC traffic data."""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from uuid import uuid4

import requests

from .config import APIConfig

logger = logging.getLogger(__name__)


class BaseSensor(ABC):
    """Base class for data sensors."""
    
    def __init__(self, api_config: APIConfig | None = None):
        """Initialize sensor.
        
        Args:
            api_config: API configuration (uses defaults if not provided).
        """
        self.api_config = api_config or APIConfig()
        self.session = requests.Session()
        
    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        """Fetch data from the source.
        
        Returns:
            List of records.
        """
        pass
    
    def _make_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> dict | list | None:
        """Make HTTP request with error handling.
        
        Args:
            url: Request URL.
            params: Query parameters.
            timeout: Request timeout in seconds.
            
        Returns:
            Response JSON or None on error.
        """
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None


class CameraSensor(BaseSensor):
    """Sensor for NYC traffic camera data."""
    
    def fetch(self) -> list[dict[str, Any]]:
        """Fetch camera data from NYC TMC API.
        
        Returns:
            List of camera records with metadata.
        """
        data = self._make_request(self.api_config.nyc_camera_url)
        if not data:
            return []
        
        cameras = data if isinstance(data, list) else data.get("cameras", [])
        records = []
        
        ts = int(time.time() * 1000)
        for camera in cameras:
            record = {
                "id": camera.get("id", ""),
                "name": camera.get("name", ""),
                "url": camera.get("url", ""),
                "latitude": camera.get("latitude"),
                "longitude": camera.get("longitude"),
                "roadwayname": camera.get("roadwayName", ""),
                "directionoftravel": camera.get("directionOfTravel", ""),
                "videourl": camera.get("videoUrl", ""),
                "disabled": camera.get("disabled", False),
                "blocked": camera.get("blocked", False),
                "ts": ts,
                "uuid": str(uuid4()),
            }
            records.append(record)
            
        logger.info(f"Fetched {len(records)} cameras")
        return records


class TrafficEventsSensor(BaseSensor):
    """Sensor for NYC traffic events data."""
    
    def __init__(
        self,
        api_config: APIConfig | None = None,
        api_key: str | None = None,
    ):
        """Initialize sensor.
        
        Args:
            api_config: API configuration.
            api_key: 511NY API key.
        """
        super().__init__(api_config)
        self.api_key = api_key
        
    def fetch(self) -> list[dict[str, Any]]:
        """Fetch traffic events from 511NY API.
        
        Returns:
            List of traffic event records.
        """
        params = {}
        if self.api_key:
            params["key"] = self.api_key
            
        data = self._make_request(self.api_config.nyc_events_url, params=params)
        if not data:
            return []
        
        events = data if isinstance(data, list) else data.get("events", [])
        records = []
        
        ts = int(time.time() * 1000)
        for event in events:
            record = {
                "id": event.get("ID", ""),
                "eventtype": event.get("EventType", ""),
                "eventsubtype": event.get("EventSubType", ""),
                "severity": event.get("Severity", ""),
                "roadwayname": event.get("RoadwayName", ""),
                "direction": event.get("Direction", ""),
                "fromlocation": event.get("FromLocation", ""),
                "tolocation": event.get("ToLocation", ""),
                "latitude": event.get("Latitude"),
                "longitude": event.get("Longitude"),
                "description": event.get("Description", ""),
                "startdate": event.get("StartDate"),
                "plannedenddate": event.get("PlannedEndDate"),
                "lanesaffected": event.get("LanesAffected", ""),
                "lanestatus": event.get("LaneStatus", ""),
                "ts": ts,
                "uuid": str(uuid4()),
            }
            records.append(record)
            
        logger.info(f"Fetched {len(records)} traffic events")
        return records


class TrafficSpeedsSensor(BaseSensor):
    """Sensor for NYC traffic speeds data."""
    
    def __init__(
        self,
        api_config: APIConfig | None = None,
        api_key: str | None = None,
    ):
        """Initialize sensor.
        
        Args:
            api_config: API configuration.
            api_key: 511NY API key.
        """
        super().__init__(api_config)
        self.api_key = api_key
        
    def fetch(self) -> list[dict[str, Any]]:
        """Fetch traffic speeds from 511NY API.
        
        Returns:
            List of traffic speed records.
        """
        params = {}
        if self.api_key:
            params["key"] = self.api_key
            
        data = self._make_request(self.api_config.nyc_speeds_url, params=params)
        if not data:
            return []
        
        speeds = data if isinstance(data, list) else data.get("speeds", [])
        records = []
        
        ts = int(time.time() * 1000)
        for speed in speeds:
            record = {
                "id": speed.get("Id", ""),
                "linkid": speed.get("linkId", ""),
                "speed": speed.get("Speed"),
                "traveltime": speed.get("TravelTime"),
                "status": speed.get("Status", ""),
                "dataas_of": speed.get("DataAsOf"),
                "linkname": speed.get("LinkName", ""),
                "linkdirection": speed.get("LinkDirection", ""),
                "linklength": speed.get("LinkLength"),
                "ts": ts,
                "uuid": str(uuid4()),
            }
            records.append(record)
            
        logger.info(f"Fetched {len(records)} speed readings")
        return records
