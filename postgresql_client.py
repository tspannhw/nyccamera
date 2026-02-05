import logging
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class PostgreSQLClient:
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        logger.info(f"PostgreSQL client initialized for {host}:{port}/{database}")
    
    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Connected to PostgreSQL")
    
    def disconnect(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
    
    def create_table(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nyc_camera_data (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR(100) UNIQUE,
                    camera_id VARCHAR(50),
                    name VARCHAR(500),
                    latitude FLOAT,
                    longitude FLOAT,
                    direction_of_travel VARCHAR(100),
                    roadway_name VARCHAR(500),
                    video_url VARCHAR(1000),
                    image_url VARCHAR(1000),
                    disabled BOOLEAN,
                    blocked BOOLEAN,
                    image_timestamp TIMESTAMP,
                    ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    hostname VARCHAR(100),
                    ip_address VARCHAR(50)
                );
                
                CREATE INDEX IF NOT EXISTS idx_camera_id ON nyc_camera_data(camera_id);
                CREATE INDEX IF NOT EXISTS idx_image_timestamp ON nyc_camera_data(image_timestamp);
                CREATE INDEX IF NOT EXISTS idx_roadway ON nyc_camera_data(roadway_name);
            """)
            self.conn.commit()
            logger.info("PostgreSQL table created/verified")
    
    def insert_records(self, records: List[Dict]) -> int:
        if not records:
            return 0
        
        self.connect()
        
        insert_sql = """
            INSERT INTO nyc_camera_data (
                uuid, camera_id, name, latitude, longitude,
                direction_of_travel, roadway_name, video_url, image_url,
                disabled, blocked, image_timestamp, ingest_timestamp,
                hostname, ip_address
            ) VALUES (
                %(uuid)s, %(camera_id)s, %(name)s, %(latitude)s, %(longitude)s,
                %(direction_of_travel)s, %(roadway_name)s, %(video_url)s, %(image_url)s,
                %(disabled)s, %(blocked)s, %(image_timestamp)s, %(ingest_timestamp)s,
                %(hostname)s, %(ip_address)s
            )
            ON CONFLICT (uuid) DO NOTHING
        """
        
        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, insert_sql, records)
                self.conn.commit()
            
            logger.info(f"Inserted {len(records)} records to PostgreSQL")
            return len(records)
        except Exception as e:
            logger.error(f"Failed to insert records: {e}")
            self.conn.rollback()
            return 0
    
    def get_recent_cameras(self, limit: int = 100) -> List[Dict]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM nyc_camera_data
                ORDER BY image_timestamp DESC
                LIMIT %s
            """, (limit,))
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
    
    def get_cameras_by_roadway(self, roadway: str) -> List[Dict]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (camera_id) *
                FROM nyc_camera_data
                WHERE roadway_name ILIKE %s
                ORDER BY camera_id, image_timestamp DESC
            """, (f"%{roadway}%",))
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
