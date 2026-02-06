#### An Example Run

`````

~/Downloads/code/coco/nyccamera % ./run_nyc_camera.sh
Starting NYC Camera Streaming Pipeline...
Command: uv run python nyc_camera_main.py --config snowflake_config.json --api-key someAPIKey --interval 60 --slack-token aSlackToken --slack-channel #traffic-cameras -
     -pg-host someserver.app --pg-port 5432 --pg-database postgres --pg-user mypguser --pg-password somePassword 

2026-02-05 20:31:36,305 [INFO] __main__ - PRODUCTION MODE: NYC Camera data + Snowpipe Streaming REST API
2026-02-05 20:31:36,305 [INFO] __main__ - ======================================================================
2026-02-05 20:31:36,305 [INFO] __main__ - NYC Street Camera Streaming Application - PRODUCTION MODE
2026-02-05 20:31:36,305 [INFO] __main__ - 511NY API -> Snowflake via Snowpipe Streaming v2
2026-02-05 20:31:36,305 [INFO] __main__ - ======================================================================
2026-02-05 20:31:36,305 [INFO] __main__ - PRODUCTION CONFIGURATION:
2026-02-05 20:31:36,305 [INFO] __main__ -   - Real NYC camera data ONLY
2026-02-05 20:31:36,305 [INFO] __main__ -   - Snowpipe Streaming high-speed REST API ONLY
2026-02-05 20:31:36,305 [INFO] __main__ -   - Optional PostgreSQL metadata storage
2026-02-05 20:31:36,305 [INFO] __main__ -   - Optional Slack image notifications
2026-02-05 20:31:36,305 [INFO] __main__ - ======================================================================
2026-02-05 20:31:36,305 [INFO] __main__ - Initializing NYC Camera sensor...
2026-02-05 20:31:36,305 [INFO] nyc_camera_sensor - NYC Camera Sensor initialized
2026-02-05 20:31:36,305 [INFO] nyc_camera_sensor -   API URL: https://511ny.org/api/getcameras
2026-02-05 20:31:36,305 [INFO] nyc_camera_sensor -   Hostname: MYHOSTNAME
2026-02-05 20:31:36,305 [INFO] nyc_camera_sensor -   IP: 999.12.34.56
2026-02-05 20:31:37,447 [INFO] nyc_camera_sensor - [OK] Fetched 2920 cameras from API
2026-02-05 20:31:37,448 [INFO] __main__ - [OK] Connected to 511NY API
2026-02-05 20:31:37,448 [INFO] __main__ - [OK] Currently tracking 2920 cameras
2026-02-05 20:31:37,448 [INFO] __main__ - Initializing Snowpipe Streaming REST API client...
2026-02-05 20:31:37,449 [INFO] snowflake_jwt_auth - PAT authentication initialized for user: THERMAL_STREAMING_USER
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - ======================================================================
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - SNOWPIPE STREAMING CLIENT - PRODUCTION MODE
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Using ONLY Snowpipe Streaming v2 REST API
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - NO direct inserts - HIGH-PERFORMANCE STREAMING ONLY
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - ======================================================================
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Loaded configuration from snowflake_config.json
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - SnowpipeStreamingClient initialized
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Database: DEMO
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Schema: DEMO
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Table: NYC_CAMERA_DATA
2026-02-05 20:31:37,449 [INFO] snowpipe_streaming_client - Channel: NYC_CAM_CHNL_20260205_203137
2026-02-05 20:31:37,449 [INFO] __main__ - Initializing Slack notifier...
2026-02-05 20:31:37,449 [INFO] slack_notifier - Slack notifier initialized for channel: #traffic-cameras
2026-02-05 20:31:37,449 [INFO] __main__ - Initializing PostgreSQL client...
2026-02-05 20:31:37,449 [INFO] postgresql_client - PostgreSQL client initialized for server.app:5432/postgres
2026-02-05 20:31:38,590 [INFO] postgresql_client - Connected to PostgreSQL
2026-02-05 20:31:39,022 [INFO] postgresql_client - PostgreSQL table created/verified
2026-02-05 20:31:39,022 [INFO] __main__ - Images directory: ./images
2026-02-05 20:31:39,023 [INFO] __main__ - Initialization complete
2026-02-05 20:31:39,023 [INFO] __main__ - Batch interval: 60.0 seconds
2026-02-05 20:31:39,023 [INFO] __main__ - Setting up Snowpipe Streaming connection...
2026-02-05 20:31:39,023 [INFO] __main__ - Discovering ingest host...
2026-02-05 20:31:39,023 [INFO] snowpipe_streaming_client - Discovering ingest host...
2026-02-05 20:31:39,023 [INFO] snowpipe_streaming_client - Using PAT as bearer token...
2026-02-05 20:31:39,023 [INFO] snowpipe_streaming_client - Calling: GET https://snowflakeSERVER.com/v2/streaming/hostname
2026-02-05 20:31:39,782 [INFO] snowpipe_streaming_client - Response status: 200
2026-02-05 20:31:39,782 [INFO] snowpipe_streaming_client - Ingest host discovered: snowflakeSERVER.com
2026-02-05 20:31:39,785 [INFO] __main__ - [OK] Ingest host: snowflakeSERVER.com
2026-02-05 20:31:39,785 [INFO] __main__ - Opening streaming channel...
2026-02-05 20:31:39,785 [INFO] snowpipe_streaming_client - Opening channel: NYC_CAM_CHNL_20260205_203137
2026-02-05 20:31:41,280 [INFO] snowpipe_streaming_client - Channel opened successfully
2026-02-05 20:31:41,281 [INFO] snowpipe_streaming_client - Continuation token: 0_1
2026-02-05 20:31:41,281 [INFO] snowpipe_streaming_client - Initial offset token: 0
2026-02-05 20:31:41,284 [INFO] __main__ - [OK] Channel opened successfully
2026-02-05 20:31:41,284 [INFO] __main__ - Snowpipe Streaming connection ready!
2026-02-05 20:31:41,284 [INFO] __main__ - ======================================================================
2026-02-05 20:31:41,284 [INFO] __main__ - Starting NYC camera data collection and streaming...
2026-02-05 20:31:41,284 [INFO] __main__ - Press Ctrl+C to stop
2026-02-05 20:31:41,284 [INFO] __main__ - ======================================================================
2026-02-05 20:31:41,790 [INFO] slack_notifier - Message sent to #traffic-cameras
2026-02-05 20:31:41,790 [INFO] __main__ - [OK] Sent startup notification to Slack
2026-02-05 20:31:41,791 [INFO] __main__ - 
--- Batch 1 ---
2026-02-05 20:31:42,540 [INFO] nyc_camera_sensor - [OK] Fetched 2920 cameras from API
2026-02-05 20:31:42,558 [INFO] nyc_camera_sensor - Processed 2920 active cameras
2026-02-05 20:31:42,559 [INFO] __main__ - Captured 2920 camera records
2026-02-05 20:31:42,559 [INFO] snowpipe_streaming_client - Appending 2920 rows...
2026-02-05 20:31:44,364 [INFO] snowpipe_streaming_client - Successfully appended 2920 rows
2026-02-05 20:31:44,367 [INFO] __main__ - [OK] Successfully sent 2920 records to Snowpipe Streaming
2026-02-05 20:31:49,716 [INFO] postgresql_client - Inserted 2920 records to PostgreSQL
2026-02-05 20:31:49,716 [INFO] __main__ - [OK] Inserted 2920 records to PostgreSQL
2026-02-05 20:31:50,183 [INFO] slack_notifier - Message sent to #traffic-cameras
2026-02-05 20:31:50,183 [INFO] __main__ - [OK] Sent batch 1 status to Slack
2026-02-05 20:31:51,123 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_Skyline-1895_20260205_203150.jpg (270737 bytes)
2026-02-05 20:31:53,374 [INFO] slack_notifier - Image uploaded to #traffic-cameras: NSP at NY 110
2026-02-05 20:31:53,374 [INFO] __main__ - [OK] Sent ONLINE camera 1/3 to Slack: NSP at NY 110
2026-02-05 20:31:53,853 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_NYSDOT-4616613_20260205_203153.jpg (12157 bytes)
2026-02-05 20:31:55,601 [INFO] slack_notifier - Image uploaded to #traffic-cameras: Ocean Parkway @ Ditmas Avenue
2026-02-05 20:31:55,601 [INFO] __main__ - [OK] Sent ONLINE camera 2/3 to Slack: Ocean Parkway @ Ditmas Avenue
2026-02-05 20:31:56,376 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_Skyline-14291_20260205_203155.jpg (112093 bytes)
2026-02-05 20:31:58,267 [INFO] slack_notifier - Image uploaded to #traffic-cameras: Monroe Ave/Chestnut at Howell
2026-02-05 20:31:58,268 [INFO] __main__ - [OK] Sent ONLINE camera 3/3 to Slack: Monroe Ave/Chestnut at Howell
2026-02-05 20:31:58,268 [INFO] __main__ - Waiting 43.5s until next batch...
2026-02-05 20:32:41,796 [INFO] __main__ - 
--- Batch 2 ---
2026-02-05 20:32:42,501 [INFO] nyc_camera_sensor - [OK] Fetched 2920 cameras from API
2026-02-05 20:32:42,520 [INFO] nyc_camera_sensor - Processed 2920 active cameras
2026-02-05 20:32:42,520 [INFO] __main__ - Captured 2920 camera records
2026-02-05 20:32:42,520 [INFO] snowpipe_streaming_client - Appending 2920 rows...
2026-02-05 20:32:44,032 [INFO] snowpipe_streaming_client - Successfully appended 2920 rows
2026-02-05 20:32:44,035 [INFO] __main__ - [OK] Successfully sent 2920 records to Snowpipe Streaming
2026-02-05 20:32:48,981 [INFO] postgresql_client - Inserted 2920 records to PostgreSQL
2026-02-05 20:32:48,981 [INFO] __main__ - [OK] Inserted 2920 records to PostgreSQL
2026-02-05 20:32:49,753 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_Skyline-1828_20260205_203248.jpg (108980 bytes)
2026-02-05 20:32:52,096 [INFO] slack_notifier - Image uploaded to #traffic-cameras: I-290 at I-190 Interchange
2026-02-05 20:32:52,096 [INFO] __main__ - [OK] Sent ONLINE camera 1/3 to Slack: I-290 at I-190 Interchange
2026-02-05 20:32:52,704 [INFO] nyc_camera_sensor - Detected offline placeholder image (hash: 9c2d059e)
2026-02-05 20:32:52,704 [INFO] nyc_camera_sensor - Camera offline: Placeholder size match (15136 bytes)
2026-02-05 20:32:52,706 [INFO] __main__ - [SKIP] Camera offline: LIE Welcome Center LIE East Bound
2026-02-05 20:32:53,247 [INFO] nyc_camera_sensor - Camera offline: Known offline image hash: 9c2d059e
2026-02-05 20:32:53,250 [INFO] __main__ - [SKIP] Camera offline: NY 27 at Meadowbrook State Parkway
2026-02-05 20:32:53,817 [INFO] nyc_camera_sensor - Camera offline: Known offline image hash: 9c2d059e
2026-02-05 20:32:53,820 [INFO] __main__ - [SKIP] Camera offline: SBP at Heatherdell Rd MM 6.28
2026-02-05 20:32:54,402 [INFO] nyc_camera_sensor - Camera offline: Known offline image hash: 9c2d059e
2026-02-05 20:32:54,405 [INFO] __main__ - [SKIP] Camera offline: WBB-21 @ South Outer Roadway Manhattan Approach
2026-02-05 20:32:55,081 [INFO] nyc_camera_sensor - Camera offline: Known offline image hash: 9c2d059e
2026-02-05 20:32:55,083 [INFO] __main__ - [SKIP] Camera offline: I-84 at Stormville Rest Area - Cam B
2026-02-05 20:32:56,302 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_NYSDOT-dodso210rti_20260205_203255.jpg (8829 bytes)
2026-02-05 20:32:57,910 [INFO] slack_notifier - Image uploaded to #traffic-cameras: Traffic closest to the camera is traveling east
2026-02-05 20:32:57,910 [INFO] __main__ - [OK] Sent ONLINE camera 2/3 to Slack: Traffic closest to the camera is traveling east
2026-02-05 20:32:58,801 [INFO] nyc_camera_sensor - Downloaded image to ./images/cam_Skyline-14301_20260205_203257.jpg (111990 bytes)
2026-02-05 20:33:00,635 [INFO] slack_notifier - Image uploaded to #traffic-cameras: NY440 at Bloomingdale Road
2026-02-05 20:33:00,635 [INFO] __main__ - [OK] Sent ONLINE camera 3/3 to Slack: NY440 at Bloomingdale Road
2026-02-05 20:33:00,635 [INFO] __main__ - Waiting 41.2s until next batch...
^C2026-02-05 20:33:13,706 [INFO] __main__ - 
Received signal 2, shutting down gracefully...
2026-02-05 20:33:41,802 [INFO] __main__ - 

2026-02-05 20:33:41,803 [INFO] __main__ - ======================================================================
2026-02-05 20:33:41,803 [INFO] __main__ - Shutting down...
2026-02-05 20:33:41,803 [INFO] __main__ - ======================================================================
2026-02-05 20:33:41,803 [INFO] snowpipe_streaming_client - ============================================================
2026-02-05 20:33:41,803 [INFO] snowpipe_streaming_client - INGESTION STATISTICS
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - ============================================================
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Total rows sent: 5840
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Total batches: 2
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Total bytes sent: 3,109,224
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Errors: 0
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Elapsed time: 124.35 seconds
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Average throughput: 46.96 rows/sec
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - Current offset token: 2
2026-02-05 20:33:41,804 [INFO] snowpipe_streaming_client - ============================================================
2026-02-05 20:33:42,321 [INFO] slack_notifier - Message sent to #traffic-cameras
2026-02-05 20:33:42,322 [INFO] __main__ - [OK] Sent shutdown notification to Slack
2026-02-05 20:33:42,322 [INFO] __main__ - Closing streaming channel...
2026-02-05 20:33:42,322 [INFO] snowpipe_streaming_client - Closing channel: NYC_CAM_CHNL_20260205_203137
2026-02-05 20:33:42,322 [INFO] snowpipe_streaming_client - Channel will auto-close after inactivity period
2026-02-05 20:33:42,322 [INFO] __main__ - [OK] Channel closed
2026-02-05 20:33:42,322 [INFO] __main__ - Cleaning up sensor...
2026-02-05 20:33:42,322 [INFO] nyc_camera_sensor - NYC Camera sensor cleaned up
2026-02-05 20:33:42,322 [INFO] __main__ - [OK] Sensor cleaned up
2026-02-05 20:33:42,325 [INFO] postgresql_client - Disconnected from PostgreSQL
2026-02-05 20:33:42,326 [INFO] __main__ - [OK] PostgreSQL disconnected
2026-02-05 20:33:42,326 [INFO] __main__ - Shutdown complete

````



```
