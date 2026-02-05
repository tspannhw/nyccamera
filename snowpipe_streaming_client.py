import json
import logging
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional
from snowflake_jwt_auth import SnowflakeJWTAuth

logger = logging.getLogger(__name__)


class SnowpipeStreamingClient:
    
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.account = self.config['account']
        self.user = self.config['user']
        self.database = self.config['database']
        self.schema = self.config['schema']
        self.table = self.config['table']
        self.pipe = self.config.get('pipe', f"{self.table}-STREAMING")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.channel_name = f"{self.config.get('channel_name', 'NYC_CAM')}_{timestamp}"
        
        # Initialize auth with full config (supports both PAT and JWT)
        self.auth = SnowflakeJWTAuth(self.config)
        
        self.ingest_host = None
        self.scoped_token = None
        self.continuation_token = None
        self.offset_token = 0
        
        self.stats = {
            'rows_sent': 0,
            'batches': 0,
            'bytes_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        logger.info("=" * 70)
        logger.info("SNOWPIPE STREAMING CLIENT - PRODUCTION MODE")
        logger.info("Using ONLY Snowpipe Streaming v2 REST API")
        logger.info("NO direct inserts - HIGH-PERFORMANCE STREAMING ONLY")
        logger.info("=" * 70)
        logger.info(f"Loaded configuration from {config_path}")
        logger.info("SnowpipeStreamingClient initialized")
        logger.info(f"Database: {self.database}")
        logger.info(f"Schema: {self.schema}")
        logger.info(f"Table: {self.table}")
        logger.info(f"Channel: {self.channel_name}")
    
    def _get_account_url(self) -> str:
        account_parts = self.account.lower().replace('_', '-').split('.')
        if len(account_parts) >= 2:
            return f"https://{account_parts[0]}.{account_parts[1]}.snowflakecomputing.com"
        return f"https://{account_parts[0]}.snowflakecomputing.com"
    
    def _get_scoped_token(self) -> str:
        """Get token for API calls - use PAT directly if available."""
        if self.scoped_token:
            return self.scoped_token
        
        # If PAT is available, use it directly as bearer token
        if self.config.get('pat'):
            logger.info("Using PAT as bearer token...")
            self.scoped_token = self.config['pat']
            return self.scoped_token
        
        # Otherwise, try OAuth token exchange
        logger.info("Obtaining new scoped token via OAuth...")
        url = f"{self._get_account_url()}/oauth/token"
        
        headers = {
            "Authorization": self.auth.get_authorization_header(),
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        payload = self.auth.get_scoped_token_payload(
            scope=f"session:scope:default database:{self.database} schema:{self.schema}"
        )
        
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        
        result = response.json()
        self.scoped_token = result.get('access_token') or result.get('token')
        logger.info("New scoped token obtained")
        return self.scoped_token
    
    def discover_ingest_host(self) -> str:
        logger.info("Discovering ingest host...")
        self._get_scoped_token()
        
        url = f"{self._get_account_url()}/v2/streaming/hostname"
        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Calling: GET {url}")
        response = requests.get(url, headers=headers)
        
        logger.info(f"Response status: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Failed to discover host: {response.status_code}")
            response.raise_for_status()
        
        # The response can be plain text or JSON depending on Snowflake version
        response_text = response.text.strip()
        
        if not response_text:
            # Construct hostname directly as fallback
            logger.warning("Empty response from hostname endpoint, constructing hostname...")
            account = self.account.lower().replace('_', '-')
            self.ingest_host = f"{account}.snowflakecomputing.com"
        elif response_text.startswith('{'):
            # JSON response
            result = response.json()
            self.ingest_host = result.get('hostname')
        else:
            # Plain text response (hostname directly)
            self.ingest_host = response_text
        
        logger.info(f"Ingest host discovered: {self.ingest_host}")
        return self.ingest_host
    
    def open_channel(self) -> dict:
        if not self.ingest_host:
            self.discover_ingest_host()
        
        logger.info(f"Opening channel: {self.channel_name}")
        
        url = (f"https://{self.ingest_host}/v2/streaming/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}")
        
        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.put(url, headers=headers, json={})
        response.raise_for_status()
        
        result = response.json()
        self.continuation_token = result.get('next_continuation_token')
        channel_status = result.get('channel_status', {})
        self.offset_token = int(channel_status.get('last_committed_offset_token', '0') or '0')
        
        logger.info("Channel opened successfully")
        logger.info(f"Continuation token: {self.continuation_token}")
        logger.info(f"Initial offset token: {self.offset_token}")
        
        return result
    
    def append_rows(self, rows: List[Dict]) -> dict:
        if not self.continuation_token:
            raise ValueError("Channel not open. Call open_channel() first.")
        
        logger.info(f"Appending {len(rows)} rows...")
        
        ndjson_payload = '\n'.join(json.dumps(row) for row in rows)
        payload_bytes = ndjson_payload.encode('utf-8')
        
        self.offset_token += 1
        
        url = (f"https://{self.ingest_host}/v2/streaming/data/"
               f"databases/{self.database}/schemas/{self.schema}/"
               f"pipes/{self.pipe}/channels/{self.channel_name}/rows"
               f"?continuationToken={self.continuation_token}"
               f"&offsetToken={self.offset_token}")
        
        headers = {
            "Authorization": f"Bearer {self.scoped_token}",
            "Content-Type": "application/x-ndjson"
        }
        
        response = requests.post(url, headers=headers, data=payload_bytes)
        response.raise_for_status()
        
        result = response.json()
        self.continuation_token = result.get('next_continuation_token')
        
        self.stats['rows_sent'] += len(rows)
        self.stats['batches'] += 1
        self.stats['bytes_sent'] += len(payload_bytes)
        
        logger.info(f"Successfully appended {len(rows)} rows")
        return result
    
    def close_channel(self):
        logger.info(f"Closing channel: {self.channel_name}")
        logger.info("Channel will auto-close after inactivity period")
    
    def print_stats(self):
        elapsed = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("INGESTION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total rows sent: {self.stats['rows_sent']}")
        logger.info(f"Total batches: {self.stats['batches']}")
        logger.info(f"Total bytes sent: {self.stats['bytes_sent']:,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Elapsed time: {elapsed:.2f} seconds")
        if elapsed > 0:
            logger.info(f"Average throughput: {self.stats['rows_sent']/elapsed:.2f} rows/sec")
        logger.info(f"Current offset token: {self.offset_token}")
        logger.info("=" * 60)
