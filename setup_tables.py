#!/usr/bin/env python3
"""
Setup Tables for NYC Camera Pipeline
=====================================
Creates tables in both PostgreSQL and Snowflake for the NYC camera data pipeline.
"""

import os
import argparse
import logging
import psycopg2
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

POSTGRESQL_DDL = """
DROP TABLE IF EXISTS nyc_camera_data CASCADE;

CREATE TABLE nyc_camera_data (
    id SERIAL PRIMARY KEY,
    uuid VARCHAR(100) UNIQUE NOT NULL,
    camera_id VARCHAR(50),
    name VARCHAR(500),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    direction_of_travel VARCHAR(100),
    roadway_name VARCHAR(500),
    video_url VARCHAR(1000),
    image_url VARCHAR(1000),
    disabled BOOLEAN DEFAULT FALSE,
    blocked BOOLEAN DEFAULT FALSE,
    image_timestamp TIMESTAMP,
    ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hostname VARCHAR(100),
    ip_address VARCHAR(50)
);

CREATE INDEX idx_nyc_camera_id ON nyc_camera_data(camera_id);
CREATE INDEX idx_nyc_image_timestamp ON nyc_camera_data(image_timestamp);
CREATE INDEX idx_nyc_roadway ON nyc_camera_data(roadway_name);
CREATE INDEX idx_nyc_ingest_timestamp ON nyc_camera_data(ingest_timestamp);
"""

SNOWFLAKE_DDL = """
CREATE OR REPLACE TABLE DEMO.DEMO.NYC_CAMERA_DATA (
    uuid STRING NOT NULL,
    camera_id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    direction_of_travel STRING,
    roadway_name STRING,
    video_url STRING,
    image_url STRING,
    disabled BOOLEAN DEFAULT FALSE,
    blocked BOOLEAN DEFAULT FALSE,
    image_timestamp TIMESTAMP_NTZ,
    ingest_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    hostname STRING,
    ip_address STRING
);
"""

SNOWFLAKE_ICEBERG_DDL = """
CREATE OR REPLACE ICEBERG TABLE DEMO.DEMO.NYC_CAMERA_ICEBERG (
    uuid STRING NOT NULL,
    camera_id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    direction_of_travel STRING,
    roadway_name STRING,
    video_url STRING,
    image_url STRING,
    disabled BOOLEAN,
    blocked BOOLEAN,
    image_timestamp TIMESTAMP_NTZ,
    ingest_timestamp TIMESTAMP_NTZ,
    hostname STRING,
    ip_address STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{external_volume}'
BASE_LOCATION = 'nyc_camera/';
"""


def setup_postgresql(host: str, port: int, database: str, user: str, password: str):
    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{database}")
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = True
    
    with conn.cursor() as cur:
        logger.info("Creating PostgreSQL table...")
        cur.execute(POSTGRESQL_DDL)
    
    conn.close()
    logger.info("PostgreSQL table created successfully")


def setup_snowflake(connection_name: str = None, external_volume: str = None):
    logger.info("Connecting to Snowflake...")
    
    if connection_name:
        conn = snowflake.connector.connect(connection_name=connection_name)
    else:
        conn = snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
        )
    
    cur = conn.cursor()
    
    logger.info("Creating Snowflake standard table...")
    for stmt in SNOWFLAKE_DDL.strip().split(';'):
        if stmt.strip():
            cur.execute(stmt)
    logger.info("Snowflake standard table created")
    
    if external_volume:
        logger.info(f"Creating Snowflake Iceberg table with volume: {external_volume}")
        iceberg_ddl = SNOWFLAKE_ICEBERG_DDL.format(external_volume=external_volume)
        cur.execute(iceberg_ddl)
        logger.info("Snowflake Iceberg table created")
    else:
        logger.info("Skipping Iceberg table (no external volume specified)")
    
    cur.close()
    conn.close()
    logger.info("Snowflake setup complete")


def main():
    parser = argparse.ArgumentParser(description='Setup PostgreSQL and Snowflake tables')
    
    parser.add_argument('--pg-host', help='PostgreSQL host')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--pg-database', default='nyccamera', help='PostgreSQL database')
    parser.add_argument('--pg-user', help='PostgreSQL user')
    parser.add_argument('--pg-password', help='PostgreSQL password')
    
    parser.add_argument('--snowflake-connection', help='Snowflake connection name')
    parser.add_argument('--external-volume', help='External volume for Iceberg table')
    
    parser.add_argument('--pg-only', action='store_true', help='Only setup PostgreSQL')
    parser.add_argument('--snowflake-only', action='store_true', help='Only setup Snowflake')
    
    args = parser.parse_args()
    
    if not args.snowflake_only:
        if args.pg_host and args.pg_user and args.pg_password:
            setup_postgresql(
                host=args.pg_host,
                port=args.pg_port,
                database=args.pg_database,
                user=args.pg_user,
                password=args.pg_password
            )
        else:
            logger.warning("PostgreSQL credentials not provided, skipping")
    
    if not args.pg_only:
        setup_snowflake(
            connection_name=args.snowflake_connection,
            external_volume=args.external_volume
        )
    
    logger.info("Setup complete!")


if __name__ == '__main__':
    main()
