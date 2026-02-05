import requests
import logging
import socket
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class NYCCameraSensor:
    
    def __init__(self, api_key: str, base_url: str = "https://511ny.org/api/getcameras"):
        self.api_key = api_key
        self.base_url = base_url
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip()
        
        logger.info("NYC Camera Sensor initialized")
        logger.info(f"  API URL: {self.base_url}")
        logger.info(f"  Hostname: {self.hostname}")
        logger.info(f"  IP: {self.ip_address}")
    
    def _get_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"
    
    def fetch_cameras(self) -> List[Dict]:
        try:
            url = f"{self.base_url}?key={self.api_key}&format=json"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            cameras = data if isinstance(data, list) else data.get('cameras', [])
            
            logger.info(f"[OK] Fetched {len(cameras)} cameras from API")
            return cameras
            
        except requests.RequestException as e:
            logger.error(f"[ERROR] Failed to fetch cameras: {e}")
            return []
    
    def process_cameras(self, cameras: List[Dict]) -> List[Dict]:
        import uuid
        
        processed = []
        timestamp = datetime.utcnow().isoformat()
        
        for cam in cameras:
            if cam.get('Disabled') == 'True' or cam.get('Blocked') == 'True':
                continue
            
            record = {
                'uuid': str(uuid.uuid4()),
                'camera_id': str(cam.get('ID', '')),
                'name': cam.get('Name', ''),
                'latitude': float(cam.get('Latitude', 0)) if cam.get('Latitude') else None,
                'longitude': float(cam.get('Longitude', 0)) if cam.get('Longitude') else None,
                'direction_of_travel': cam.get('DirectionOfTravel', ''),
                'roadway_name': cam.get('RoadwayName', ''),
                'video_url': cam.get('VideoUrl', ''),
                'image_url': cam.get('Url', ''),
                'disabled': cam.get('Disabled', 'False') == 'True',
                'blocked': cam.get('Blocked', 'False') == 'True',
                'image_timestamp': timestamp,
                'ingest_timestamp': timestamp,
                'hostname': self.hostname,
                'ip_address': self.ip_address
            }
            processed.append(record)
        
        logger.info(f"Processed {len(processed)} active cameras")
        return processed
    
    def download_image(self, url: str, output_path: str) -> Optional[str]:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            content_length = len(response.content)
            if content_length < 5000:
                logger.warning(f"Image too small ({content_length} bytes), likely error")
                return None
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded image to {output_path} ({content_length} bytes)")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to download image: {e}")
            return None
    
    def cleanup(self):
        logger.info("NYC Camera sensor cleaned up")
