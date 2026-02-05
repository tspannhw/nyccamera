"""
NYC Street Camera Dashboard
===========================
Streamlit dashboard for visualizing NYC traffic camera data
streamed via Snowpipe Streaming High Speed v2.
"""

import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

import snowflake.connector


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
    )


@st.cache_data(ttl=60)
def get_overview_stats():
    conn = get_connection()
    query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT camera_id) as unique_cameras,
        COUNT(DISTINCT roadway_name) as unique_roadways,
        MIN(image_timestamp) as first_capture,
        MAX(image_timestamp) as last_capture
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_recent_cameras(limit: int = 100):
    conn = get_connection()
    query = f"""
    SELECT 
        camera_id, name, roadway_name, direction_of_travel,
        latitude, longitude, video_url, image_url, image_timestamp
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    WHERE image_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY camera_id ORDER BY image_timestamp DESC) = 1
    ORDER BY image_timestamp DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_roadway_stats():
    conn = get_connection()
    query = """
    SELECT 
        roadway_name,
        COUNT(DISTINCT camera_id) as camera_count,
        COUNT(*) as total_captures,
        ROUND(AVG(latitude), 4) as avg_latitude,
        ROUND(AVG(longitude), 4) as avg_longitude
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    WHERE roadway_name IS NOT NULL AND roadway_name != ''
    GROUP BY roadway_name
    ORDER BY camera_count DESC
    LIMIT 30
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_hourly_captures():
    conn = get_connection()
    query = """
    SELECT 
        DATE_TRUNC('hour', image_timestamp) as capture_hour,
        COUNT(*) as capture_count,
        COUNT(DISTINCT camera_id) as active_cameras
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    WHERE image_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY capture_hour
    ORDER BY capture_hour
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_direction_stats():
    conn = get_connection()
    query = """
    SELECT 
        COALESCE(direction_of_travel, 'Unknown') as direction,
        COUNT(DISTINCT camera_id) as camera_count,
        COUNT(*) as total_captures
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    GROUP BY direction_of_travel
    ORDER BY camera_count DESC
    """
    return pd.read_sql(query, conn)


def main():
    st.set_page_config(
        page_title="NYC Camera Dashboard",
        page_icon="📷",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📷 NYC Street Camera Dashboard")
    st.markdown("Real-time visualization of NYC traffic camera data streamed via Snowpipe Streaming v2")
    
    with st.sidebar:
        st.header("🔧 Controls")
        auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("### Data Pipeline")
        st.markdown("""
        - **Source**: 511NY API
        - **Streaming**: Snowpipe v2 REST
        - **Storage**: Snowflake + Iceberg
        """)
    
    try:
        overview = get_overview_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Records", f"{overview['TOTAL_RECORDS'].iloc[0]:,}")
        with col2:
            st.metric("📸 Unique Cameras", f"{overview['UNIQUE_CAMERAS'].iloc[0]:,}")
        with col3:
            st.metric("🛣️ Roadways", f"{overview['UNIQUE_ROADWAYS'].iloc[0]:,}")
        with col4:
            last_capture = overview['LAST_CAPTURE'].iloc[0]
            if pd.notna(last_capture):
                st.metric("⏱️ Last Capture", last_capture.strftime('%H:%M:%S'))
            else:
                st.metric("⏱️ Last Capture", "N/A")
        
        st.markdown("---")
        
        tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Map", "📈 Analytics", "📋 Camera List", "📊 Roadways"])
        
        with tab1:
            st.subheader("Camera Locations Map")
            
            cameras = get_recent_cameras()
            
            if not cameras.empty and 'LATITUDE' in cameras.columns:
                map_data = cameras[
                    cameras['LATITUDE'].notna() & 
                    cameras['LONGITUDE'].notna() &
                    (cameras['LATITUDE'].between(40, 45)) &
                    (cameras['LONGITUDE'].between(-75, -72))
                ].copy()
                
                if not map_data.empty:
                    view_state = pdk.ViewState(
                        latitude=map_data['LATITUDE'].mean(),
                        longitude=map_data['LONGITUDE'].mean(),
                        zoom=9,
                        pitch=0
                    )
                    
                    layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=map_data,
                        get_position='[LONGITUDE, LATITUDE]',
                        get_color='[255, 100, 100, 200]',
                        get_radius=500,
                        pickable=True
                    )
                    
                    deck = pdk.Deck(
                        layers=[layer],
                        initial_view_state=view_state,
                        tooltip={"text": "{NAME}\n{ROADWAY_NAME}"}
                    )
                    
                    st.pydeck_chart(deck)
                else:
                    st.warning("No valid camera locations to display")
            else:
                st.warning("No camera data available")
        
        with tab2:
            st.subheader("Data Analytics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                hourly = get_hourly_captures()
                if not hourly.empty:
                    fig = px.line(
                        hourly, 
                        x='CAPTURE_HOUR', 
                        y='CAPTURE_COUNT',
                        title='Captures Over Time',
                        labels={'CAPTURE_HOUR': 'Time', 'CAPTURE_COUNT': 'Captures'}
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                directions = get_direction_stats()
                if not directions.empty:
                    fig = px.pie(
                        directions,
                        values='CAMERA_COUNT',
                        names='DIRECTION',
                        title='Cameras by Direction'
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.subheader("Recent Camera Data")
            
            cameras = get_recent_cameras()
            if not cameras.empty:
                display_cols = ['CAMERA_ID', 'NAME', 'ROADWAY_NAME', 
                               'DIRECTION_OF_TRAVEL', 'IMAGE_TIMESTAMP']
                available_cols = [c for c in display_cols if c in cameras.columns]
                st.dataframe(cameras[available_cols], use_container_width=True)
            else:
                st.info("No recent camera data available")
        
        with tab4:
            st.subheader("Roadway Statistics")
            
            roadways = get_roadway_stats()
            if not roadways.empty:
                fig = px.bar(
                    roadways.head(20),
                    x='CAMERA_COUNT',
                    y='ROADWAY_NAME',
                    orientation='h',
                    title='Top 20 Roadways by Camera Count',
                    labels={'CAMERA_COUNT': 'Cameras', 'ROADWAY_NAME': 'Roadway'}
                )
                fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
        
        if auto_refresh:
            import time
            time.sleep(60)
            st.cache_data.clear()
            st.rerun()
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure Snowflake connection is configured properly.")


if __name__ == "__main__":
    main()
