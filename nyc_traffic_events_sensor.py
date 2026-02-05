import requests
import logging
import socket
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)


class NYCTrafficEventsSensor:
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.events_url = "https://511ny.org/api/getevents"
        self.traffic_url = "https://511ny.org/api/gettraffic"
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip()
        
        logger.info("NYC Traffic Events Sensor initialized")
    
    def _get_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
    
    def fetch_events(self) -> List[Dict]:
        try:
            url = f"{self.events_url}?key={self.api_key}&format=json"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            events = data if isinstance(data, list) else data.get('events', [])
            
            logger.info(f"[OK] Fetched {len(events)} traffic events")
            return events
            
        except requests.RequestException as e:
            logger.error(f"[ERROR] Failed to fetch events: {e}")
            return []
    
    def fetch_traffic_speeds(self) -> List[Dict]:
        try:
            url = f"{self.traffic_url}?key={self.api_key}&format=json"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            traffic = data if isinstance(data, list) else data.get('traffic', [])
            
            logger.info(f"[OK] Fetched {len(traffic)} traffic segments")
            return traffic
            
        except requests.RequestException as e:
            logger.error(f"[ERROR] Failed to fetch traffic: {e}")
            return []
    
    def process_events(self, events: List[Dict]) -> List[Dict]:
        import uuid as uuid_lib
        
        processed = []
        timestamp = datetime.utcnow().isoformat()
        
        for event in events:
            record = {
                'uuid': str(uuid_lib.uuid4()),
                'event_id': str(event.get('ID', '')),
                'event_type': event.get('EventType', ''),
                'event_subtype': event.get('EventSubType', ''),
                'severity': event.get('Severity', ''),
                'roadway_name': event.get('RoadwayName', ''),
                'direction': event.get('Direction', ''),
                'description': event.get('Description', ''),
                'location': event.get('Location', ''),
                'latitude': float(event.get('Latitude', 0)) if event.get('Latitude') else None,
                'longitude': float(event.get('Longitude', 0)) if event.get('Longitude') else None,
                'start_date': event.get('StartDate', ''),
                'planned_end_date': event.get('PlannedEndDate', ''),
                'last_updated': event.get('LastUpdated', ''),
                'event_timestamp': timestamp,
                'ingest_timestamp': timestamp,
                'hostname': self.hostname,
                'ip_address': self.ip_address
            }
            processed.append(record)
        
        logger.info(f"Processed {len(processed)} traffic events")
        return processed
    
    def process_traffic(self, traffic: List[Dict]) -> List[Dict]:
        import uuid as uuid_lib
        
        processed = []
        timestamp = datetime.utcnow().isoformat()
        
        for segment in traffic:
            record = {
                'uuid': str(uuid_lib.uuid4()),
                'segment_id': str(segment.get('ID', '')),
                'link_id': segment.get('LinkId', ''),
                'roadway_name': segment.get('RoadwayName', ''),
                'direction': segment.get('Direction', ''),
                'from_location': segment.get('From', ''),
                'to_location': segment.get('To', ''),
                'current_speed': float(segment.get('Speed', 0)) if segment.get('Speed') else None,
                'free_flow_speed': float(segment.get('FreeFlowSpeed', 0)) if segment.get('FreeFlowSpeed') else None,
                'travel_time': float(segment.get('TravelTime', 0)) if segment.get('TravelTime') else None,
                'data_as_of': segment.get('DataAsOf', ''),
                'traffic_timestamp': timestamp,
                'ingest_timestamp': timestamp,
                'hostname': self.hostname,
                'ip_address': self.ip_address
            }
            processed.append(record)
        
        logger.info(f"Processed {len(processed)} traffic segments")
        return processed
    
    def cleanup(self):
        logger.info("NYC Traffic Events sensor cleaned up")
