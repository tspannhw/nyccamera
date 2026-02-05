#!/usr/bin/env python3
"""
NYC Street Camera Data Pipeline
================================
Streams NYC traffic camera data to:
- Snowflake via Snowpipe Streaming High Speed v2 REST API
- PostgreSQL for metadata storage
- Slack for image notifications

Based on: https://github.com/tspannhw/SNACKAI-CoCo-ADSB
         https://github.com/tspannhw/TrafficAI
"""

import os
import sys
import json
import signal
import logging
import argparse
import time
from datetime import datetime
from typing import Optional

from nyc_camera_sensor import NYCCameraSensor
from snowpipe_streaming_client import SnowpipeStreamingClient
from slack_notifier import SlackNotifier
from postgresql_client import PostgreSQLClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    logger.info(f"\nReceived signal {signum}, shutting down gracefully...")
    shutdown_requested = True


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return json.load(f)


def main():
    global shutdown_requested
    
    parser = argparse.ArgumentParser(description='NYC Camera Streaming Pipeline')
    parser.add_argument('--config', default='snowflake_config.json',
                       help='Snowflake config file path')
    parser.add_argument('--api-key', required=True,
                       help='511NY API key')
    parser.add_argument('--interval', type=float, default=60.0,
                       help='Seconds between batches (default: 60)')
    parser.add_argument('--slack-token', help='Slack bot token')
    parser.add_argument('--slack-channel', default='#traffic-cameras',
                       help='Slack channel for notifications')
    parser.add_argument('--pg-host', help='PostgreSQL host')
    parser.add_argument('--pg-port', type=int, default=5432,
                       help='PostgreSQL port')
    parser.add_argument('--pg-database', default='nyccamera',
                       help='PostgreSQL database')
    parser.add_argument('--pg-user', help='PostgreSQL user')
    parser.add_argument('--pg-password', help='PostgreSQL password')
    parser.add_argument('--images-dir', default='./images',
                       help='Directory to save camera images')
    parser.add_argument('--send-images', action='store_true',
                       help='Download and send images to Slack')
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("PRODUCTION MODE: NYC Camera data + Snowpipe Streaming REST API")
    logger.info("=" * 70)
    logger.info("NYC Street Camera Streaming Application - PRODUCTION MODE")
    logger.info("511NY API -> Snowflake via Snowpipe Streaming v2")
    logger.info("=" * 70)
    logger.info("PRODUCTION CONFIGURATION:")
    logger.info("  - Real NYC camera data ONLY")
    logger.info("  - Snowpipe Streaming high-speed REST API ONLY")
    logger.info("  - Optional PostgreSQL metadata storage")
    logger.info("  - Optional Slack image notifications")
    logger.info("=" * 70)
    
    logger.info("Initializing NYC Camera sensor...")
    sensor = NYCCameraSensor(api_key=args.api_key)
    
    cameras = sensor.fetch_cameras()
    if cameras:
        logger.info(f"[OK] Connected to 511NY API")
        logger.info(f"[OK] Currently tracking {len(cameras)} cameras")
    else:
        logger.error("[ERROR] Failed to connect to 511NY API")
        sys.exit(1)
    
    logger.info("Initializing Snowpipe Streaming REST API client...")
    streaming_client = SnowpipeStreamingClient(args.config)
    
    slack_client: Optional[SlackNotifier] = None
    if args.slack_token:
        logger.info("Initializing Slack notifier...")
        slack_client = SlackNotifier(args.slack_token, args.slack_channel)
    
    pg_client: Optional[PostgreSQLClient] = None
    if args.pg_host and args.pg_user and args.pg_password:
        logger.info("Initializing PostgreSQL client...")
        pg_client = PostgreSQLClient(
            host=args.pg_host,
            port=args.pg_port,
            database=args.pg_database,
            user=args.pg_user,
            password=args.pg_password
        )
        pg_client.create_table()
    
    if args.send_images:
        os.makedirs(args.images_dir, exist_ok=True)
    
    logger.info("Initialization complete")
    logger.info(f"Batch interval: {args.interval} seconds")
    logger.info("Setting up Snowpipe Streaming connection...")
    
    logger.info("Discovering ingest host...")
    streaming_client.discover_ingest_host()
    logger.info(f"[OK] Ingest host: {streaming_client.ingest_host}")
    
    logger.info("Opening streaming channel...")
    streaming_client.open_channel()
    logger.info("[OK] Channel opened successfully")
    logger.info("Snowpipe Streaming connection ready!")
    
    logger.info("=" * 70)
    logger.info("Starting NYC camera data collection and streaming...")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 70)
    
    batch_num = 0
    total_records_sent = 0
    
    # Send startup notification to Slack
    if slack_client:
        startup_msg = (
            f"🚀 *NYC Camera Pipeline Started*\n"
            f"• Tracking `{len(cameras)}` cameras\n"
            f"• Batch interval: `{args.interval}s`\n"
            f"• Streaming to: `{streaming_client.database}.{streaming_client.schema}.{streaming_client.table}`"
        )
        slack_client.send_message(startup_msg)
        logger.info("[OK] Sent startup notification to Slack")
    
    try:
        while not shutdown_requested:
            batch_start = time.time()
            batch_num += 1
            logger.info(f"\n--- Batch {batch_num} ---")
            
            cameras = sensor.fetch_cameras()
            records = sensor.process_cameras(cameras)
            
            logger.info(f"Captured {len(records)} camera records")
            
            if records:
                streaming_client.append_rows(records)
                total_records_sent += len(records)
                logger.info(f"[OK] Successfully sent {len(records)} records to Snowpipe Streaming")
                
                if pg_client:
                    pg_client.insert_records(records)
                    logger.info(f"[OK] Inserted {len(records)} records to PostgreSQL")
                
                # Send Slack status every 5 batches or on first batch
                if slack_client and (batch_num == 1 or batch_num % 5 == 0):
                    status_msg = (
                        f"📊 *Batch {batch_num} Complete*\n"
                        f"• Records this batch: `{len(records)}`\n"
                        f"• Total records sent: `{total_records_sent}`\n"
                        f"• Cameras tracked: `{len(cameras)}`"
                    )
                    slack_client.send_message(status_msg)
                    logger.info(f"[OK] Sent batch {batch_num} status to Slack")
                
                # Send sample camera image on first batch if requested
                if slack_client and args.send_images and batch_num == 1:
                    sample_record = records[0]
                    if sample_record.get('image_url'):
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        image_path = os.path.join(
                            args.images_dir,
                            f"cam_{sample_record['camera_id']}_{timestamp}.jpg"
                        )
                        downloaded = sensor.download_image(
                            sample_record['image_url'],
                            image_path
                        )
                        if downloaded:
                            slack_client.send_camera_alert(sample_record, image_path)
                            logger.info("[OK] Sent sample camera image to Slack")
            
            elapsed = time.time() - batch_start
            wait_time = max(0, args.interval - elapsed)
            if wait_time > 0 and not shutdown_requested:
                logger.info(f"Waiting {wait_time:.1f}s until next batch...")
                time.sleep(wait_time)
    
    except Exception as e:
        logger.error(f"Error during streaming: {e}")
        raise
    
    finally:
        logger.info("\n")
        logger.info("=" * 70)
        logger.info("Shutting down...")
        logger.info("=" * 70)
        
        streaming_client.print_stats()
        
        # Send shutdown notification to Slack
        if slack_client:
            shutdown_msg = (
                f"🛑 *NYC Camera Pipeline Stopped*\n"
                f"• Total batches: `{batch_num}`\n"
                f"• Total records sent: `{total_records_sent}`\n"
                f"• Runtime stats logged"
            )
            try:
                slack_client.send_message(shutdown_msg)
                logger.info("[OK] Sent shutdown notification to Slack")
            except Exception as e:
                logger.warning(f"Could not send shutdown message: {e}")
        
        logger.info("Closing streaming channel...")
        streaming_client.close_channel()
        logger.info("[OK] Channel closed")
        
        logger.info("Cleaning up sensor...")
        sensor.cleanup()
        logger.info("[OK] Sensor cleaned up")
        
        if pg_client:
            pg_client.disconnect()
            logger.info("[OK] PostgreSQL disconnected")
        
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main()
