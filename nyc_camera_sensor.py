import requests
import logging
import socket
import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Known offline/placeholder image signatures
# These are file sizes and hashes of common "No live camera feed" images
OFFLINE_IMAGE_SIZES = {15136, 15000, 14000, 16000}  # Common placeholder sizes in bytes
OFFLINE_IMAGE_HASHES = set()  # Will be populated with known offline image hashes


class NYCCameraSensor:
    
    def __init__(self, api_key: str, base_url: str = "https://511ny.org/api/getcameras"):
        self.api_key = api_key
        self.base_url = base_url
        self.hostname = socket.gethostname()
        self.ip_address = self._get_ip()
        self.offline_image_hash = None  # Will store hash of first detected offline image
        
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
        """Download camera image and return path if successful and camera is online."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            content = response.content
            content_length = len(content)
            
            # Check if image is too small (likely error)
            if content_length < 5000:
                logger.warning(f"Image too small ({content_length} bytes), likely error")
                return None
            
            # Check if this is an offline/placeholder image
            is_offline, reason = self._is_offline_image(content, content_length)
            if is_offline:
                logger.info(f"Camera offline: {reason}")
                return None
            
            with open(output_path, 'wb') as f:
                f.write(content)
            
            logger.info(f"Downloaded image to {output_path} ({content_length} bytes)")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to download image: {e}")
            return None
    
    def _is_offline_image(self, content: bytes, content_length: int) -> Tuple[bool, str]:
        """
        Detect if image is an offline placeholder.
        Returns (is_offline, reason).
        """
        # Calculate hash of the image
        image_hash = hashlib.md5(content).hexdigest()
        
        # Check against known offline image hashes
        if image_hash in OFFLINE_IMAGE_HASHES:
            return True, f"Known offline image hash: {image_hash[:8]}"
        
        # Check if we've seen this as an offline image before
        if self.offline_image_hash and image_hash == self.offline_image_hash:
            return True, f"Previously detected offline image"
        
        # Check for exact known placeholder size (15136 bytes is common)
        if content_length == 15136:
            # This is the exact size of the "No live camera feed" placeholder
            # Store this hash for future detection
            if not self.offline_image_hash:
                self.offline_image_hash = image_hash
                OFFLINE_IMAGE_HASHES.add(image_hash)
                logger.info(f"Detected offline placeholder image (hash: {image_hash[:8]})")
            return True, f"Placeholder size match ({content_length} bytes)"
        
        # Check if file size is suspiciously uniform (many offline images same size)
        if content_length in OFFLINE_IMAGE_SIZES:
            # Additional check: try to detect "No live camera" text pattern
            # JPEG files won't have readable text, but we can check for uniform gray pixels
            # For now, just flag as potentially offline based on size
            pass
        
        return False, ""
    
    def check_camera_status(self, image_url: str) -> Dict:
        """
        Check if a camera is online by examining its image.
        Returns status dict with is_online and details.
        """
        try:
            response = requests.get(image_url, timeout=15)
            response.raise_for_status()
            
            content = response.content
            content_length = len(content)
            
            if content_length < 5000:
                return {
                    'is_online': False,
                    'reason': 'Image too small',
                    'size': content_length
                }
            
            is_offline, reason = self._is_offline_image(content, content_length)
            
            return {
                'is_online': not is_offline,
                'reason': reason if is_offline else 'Camera active',
                'size': content_length,
                'hash': hashlib.md5(content).hexdigest()[:8]
            }
            
        except Exception as e:
            return {
                'is_online': False,
                'reason': f'Error: {str(e)}',
                'size': 0
            }
    
    def cleanup(self):
        logger.info("NYC Camera sensor cleaned up")
