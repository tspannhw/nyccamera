"""
NYC Traffic Analytics Dashboard
================================
Comprehensive Streamlit dashboard for traffic events, speeds, and camera data
streamed via Snowpipe Streaming High Speed v2.

Blade Runner Surveillance System - Analytics Interface
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
    conn = st.connection("snowflake")
    return conn;


@st.cache_data(ttl=60)
def get_camera_stats():
    conn = get_connection()
    query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT camera_id) as unique_cameras,
        COUNT(DISTINCT roadway_name) as unique_roadways,
        MAX(image_timestamp) as last_capture
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_event_stats():
    conn = get_connection()
    query = """
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT event_id) as unique_events,
        COUNT(DISTINCT event_type) as event_types,
        MAX(event_timestamp) as last_event
    FROM DEMO.DEMO.NYC_TRAFFIC_EVENTS
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_speed_stats():
    conn = get_connection()
    query = """
    SELECT 
        COUNT(*) as total_readings,
        COUNT(DISTINCT segment_id) as unique_segments,
        ROUND(AVG(current_speed), 1) as avg_speed,
        ROUND(AVG(CASE WHEN free_flow_speed > 0 
              THEN current_speed / free_flow_speed ELSE NULL END) * 100, 1) as avg_flow_pct,
        MAX(traffic_timestamp) as last_reading
    FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_recent_events(limit: int = 200):
    conn = get_connection()
    query = f"""
    SELECT 
        event_id, event_type, event_subtype, severity,
        roadway_name, direction, description, location,
        latitude, longitude, event_timestamp
    FROM DEMO.DEMO.NYC_TRAFFIC_EVENTS
    WHERE event_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    ORDER BY event_timestamp DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_event_type_distribution():
    conn = get_connection()
    query = """
    SELECT 
        event_type,
        COUNT(*) as event_count
    FROM DEMO.DEMO.NYC_TRAFFIC_EVENTS
    GROUP BY event_type
    ORDER BY event_count DESC
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_events_by_roadway():
    conn = get_connection()
    query = """
    SELECT 
        roadway_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT event_type) as event_types
    FROM DEMO.DEMO.NYC_TRAFFIC_EVENTS
    WHERE roadway_name IS NOT NULL AND roadway_name != ''
    GROUP BY roadway_name
    ORDER BY event_count DESC
    LIMIT 20
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_events_by_severity():
    conn = get_connection()
    query = """
    SELECT 
        COALESCE(severity, 'Unknown') as severity,
        COUNT(*) as event_count
    FROM DEMO.DEMO.NYC_TRAFFIC_EVENTS
    GROUP BY severity
    ORDER BY event_count DESC
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_speed_data(limit: int = 500):
    conn = get_connection()
    query = f"""
    SELECT 
        segment_id, roadway_name, direction,
        from_location, to_location,
        current_speed, free_flow_speed, travel_time,
        CASE WHEN free_flow_speed > 0 
             THEN ROUND((current_speed / free_flow_speed) * 100, 1) 
             ELSE NULL END as flow_percentage,
        traffic_timestamp
    FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY traffic_timestamp DESC) = 1
    ORDER BY traffic_timestamp DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_slowest_segments(limit: int = 20):
    conn = get_connection()
    query = f"""
    SELECT 
        segment_id, roadway_name, direction,
        from_location, to_location,
        ROUND(AVG(current_speed), 1) as avg_speed,
        ROUND(AVG(free_flow_speed), 1) as avg_free_flow,
        ROUND(AVG(CASE WHEN free_flow_speed > 0 
                  THEN current_speed / free_flow_speed ELSE NULL END) * 100, 1) as flow_pct,
        COUNT(*) as reading_count
    FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS
    WHERE traffic_timestamp >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
      AND current_speed > 0
    GROUP BY segment_id, roadway_name, direction, from_location, to_location
    HAVING COUNT(*) >= 2
    ORDER BY flow_pct ASC
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_speed_by_roadway():
    conn = get_connection()
    query = """
    SELECT 
        roadway_name,
        ROUND(AVG(current_speed), 1) as avg_speed,
        ROUND(AVG(free_flow_speed), 1) as avg_free_flow,
        ROUND(AVG(CASE WHEN free_flow_speed > 0 
                  THEN current_speed / free_flow_speed ELSE NULL END) * 100, 1) as flow_pct,
        COUNT(DISTINCT segment_id) as segments
    FROM DEMO.DEMO.NYC_TRAFFIC_SPEEDS
    WHERE roadway_name IS NOT NULL AND roadway_name != ''
    GROUP BY roadway_name
    ORDER BY segments DESC
    LIMIT 25
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=60)
def get_cameras():
    conn = get_connection()
    query = """
    SELECT 
        camera_id, name, roadway_name, direction_of_travel,
        latitude, longitude, image_url, image_timestamp
    FROM DEMO.DEMO.NYC_CAMERA_DATA
    WHERE image_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY camera_id ORDER BY image_timestamp DESC) = 1
    ORDER BY image_timestamp DESC
    LIMIT 100
    """
    return pd.read_sql(query, conn)


def render_header():
    st.set_page_config(
        page_title="NYC Traffic Analytics",
        page_icon="🚦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #ff6b35;
    }
    .stMetric {
        background-color: rgba(26, 26, 46, 0.5);
        padding: 15px;
        border-radius: 8px;
        border-left: 3px solid #ff6b35;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("🚦 NYC Traffic Analytics Dashboard")
    st.markdown("*Blade Runner Surveillance System - Real-time Traffic Intelligence*")


def render_sidebar():
    with st.sidebar:
        st.image("https://img.icons8.com/external-flat-juicy-fish/64/external-surveillance-smart-city-flat-flat-juicy-fish.png", width=64)
        st.header("⚙️ Control Panel")
        
        selected_view = st.radio(
            "Select View",
            ["📊 Overview", "🚨 Traffic Events", "🏎️ Traffic Speeds", "📷 Cameras"],
            index=0
        )
        
        st.markdown("---")
        
        if st.button("🔄 Refresh All Data"):
            st.cache_data.clear()
            st.rerun()
        
        auto_refresh = st.checkbox("Auto-refresh (60s)")
        
        st.markdown("---")
        st.markdown("### 📡 Data Pipeline")
        st.markdown("""
        - **Source**: 511NY API
        - **Ingest**: Snowpipe v2 REST
        - **Storage**: Snowflake + Iceberg
        - **Analytics**: Semantic Views
        """)
        
        return selected_view, auto_refresh


def render_overview():
    st.header("📊 Traffic Overview")
    
    try:
        camera_stats = get_camera_stats()
        event_stats = get_event_stats()
        speed_stats = get_speed_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📷 Cameras", f"{camera_stats['UNIQUE_CAMERAS'].iloc[0]:,}" if camera_stats['UNIQUE_CAMERAS'].iloc[0] else "0")
        with col2:
            st.metric("🚨 Events", f"{event_stats['UNIQUE_EVENTS'].iloc[0]:,}" if event_stats['UNIQUE_EVENTS'].iloc[0] else "0")
        with col3:
            st.metric("🛣️ Segments", f"{speed_stats['UNIQUE_SEGMENTS'].iloc[0]:,}" if speed_stats['UNIQUE_SEGMENTS'].iloc[0] else "0")
        with col4:
            flow = speed_stats['AVG_FLOW_PCT'].iloc[0]
            st.metric("🏎️ Avg Flow", f"{flow}%" if flow else "N/A")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚨 Events by Type")
            events_dist = get_event_type_distribution()
            if not events_dist.empty:
                fig = px.pie(
                    events_dist,
                    values='EVENT_COUNT',
                    names='EVENT_TYPE',
                    color_discrete_sequence=px.colors.sequential.Plasma
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No event data available")
        
        with col2:
            st.subheader("🏎️ Speed by Roadway")
            speed_roadway = get_speed_by_roadway()
            if not speed_roadway.empty:
                fig = px.bar(
                    speed_roadway.head(15),
                    x='AVG_SPEED',
                    y='ROADWAY_NAME',
                    orientation='h',
                    color='FLOW_PCT',
                    color_continuous_scale='RdYlGn',
                    labels={'AVG_SPEED': 'Avg Speed (mph)', 'ROADWAY_NAME': 'Roadway', 'FLOW_PCT': 'Flow %'}
                )
                fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No speed data available")
        
    except Exception as e:
        st.error(f"Error loading overview: {e}")


def render_events():
    st.header("🚨 Traffic Events")
    
    try:
        col1, col2, col3, col4 = st.columns(4)
        
        event_stats = get_event_stats()
        with col1:
            st.metric("Total Events", f"{event_stats['TOTAL_EVENTS'].iloc[0]:,}" if event_stats['TOTAL_EVENTS'].iloc[0] else "0")
        with col2:
            st.metric("Unique Events", f"{event_stats['UNIQUE_EVENTS'].iloc[0]:,}" if event_stats['UNIQUE_EVENTS'].iloc[0] else "0")
        with col3:
            st.metric("Event Types", f"{event_stats['EVENT_TYPES'].iloc[0]}" if event_stats['EVENT_TYPES'].iloc[0] else "0")
        with col4:
            last = event_stats['LAST_EVENT'].iloc[0]
            st.metric("Last Event", last.strftime('%H:%M') if pd.notna(last) else "N/A")
        
        st.markdown("---")
        
        tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Event Map", "📊 Analytics", "📋 Event List", "🏆 Top Roadways"])
        
        with tab1:
            events = get_recent_events()
            if not events.empty:
                map_data = events[
                    events['LATITUDE'].notna() & 
                    events['LONGITUDE'].notna() &
                    (events['LATITUDE'].between(40, 45)) &
                    (events['LONGITUDE'].between(-76, -72))
                ].copy()
                
                if not map_data.empty:
                    color_map = {
                        'incident': [255, 0, 0, 200],
                        'construction': [255, 165, 0, 200],
                        'special_event': [0, 100, 255, 200],
                        'road_work': [255, 200, 0, 200]
                    }
                    map_data['color'] = map_data['EVENT_TYPE'].apply(
                        lambda x: color_map.get(str(x).lower(), [128, 128, 128, 200])
                    )
                    
                    view_state = pdk.ViewState(
                        latitude=map_data['LATITUDE'].mean(),
                        longitude=map_data['LONGITUDE'].mean(),
                        zoom=8,
                        pitch=0
                    )
                    
                    layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=map_data,
                        get_position='[LONGITUDE, LATITUDE]',
                        get_color='color',
                        get_radius=800,
                        pickable=True
                    )
                    
                    deck = pdk.Deck(
                        layers=[layer],
                        initial_view_state=view_state,
                        tooltip={"text": "{EVENT_TYPE}: {DESCRIPTION}\n{ROADWAY_NAME}"}
                    )
                    
                    st.pydeck_chart(deck)
                else:
                    st.warning("No events with valid locations")
            else:
                st.info("No recent events")
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                events_dist = get_event_type_distribution()
                if not events_dist.empty:
                    fig = px.bar(
                        events_dist,
                        x='EVENT_TYPE',
                        y='EVENT_COUNT',
                        color='EVENT_COUNT',
                        color_continuous_scale='Reds',
                        title='Events by Type'
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                severity = get_events_by_severity()
                if not severity.empty:
                    fig = px.pie(
                        severity,
                        values='EVENT_COUNT',
                        names='SEVERITY',
                        title='Events by Severity',
                        color_discrete_sequence=px.colors.sequential.Reds
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            events = get_recent_events(100)
            if not events.empty:
                display_cols = ['EVENT_TYPE', 'SEVERITY', 'ROADWAY_NAME', 'DESCRIPTION', 'EVENT_TIMESTAMP']
                available = [c for c in display_cols if c in events.columns]
                st.dataframe(events[available], use_container_width=True, height=500)
            else:
                st.info("No events to display")
        
        with tab4:
            roadways = get_events_by_roadway()
            if not roadways.empty:
                fig = px.bar(
                    roadways,
                    x='EVENT_COUNT',
                    y='ROADWAY_NAME',
                    orientation='h',
                    color='EVENT_TYPES',
                    color_continuous_scale='Viridis',
                    title='Top 20 Roadways by Event Count',
                    labels={'EVENT_COUNT': 'Events', 'ROADWAY_NAME': 'Roadway', 'EVENT_TYPES': 'Types'}
                )
                fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"Error loading events: {e}")


def render_speeds():
    st.header("🏎️ Traffic Speeds")
    
    try:
        speed_stats = get_speed_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Readings", f"{speed_stats['TOTAL_READINGS'].iloc[0]:,}" if speed_stats['TOTAL_READINGS'].iloc[0] else "0")
        with col2:
            st.metric("Segments", f"{speed_stats['UNIQUE_SEGMENTS'].iloc[0]:,}" if speed_stats['UNIQUE_SEGMENTS'].iloc[0] else "0")
        with col3:
            st.metric("Avg Speed", f"{speed_stats['AVG_SPEED'].iloc[0]} mph" if speed_stats['AVG_SPEED'].iloc[0] else "N/A")
        with col4:
            flow = speed_stats['AVG_FLOW_PCT'].iloc[0]
            color = "normal" if flow and flow > 75 else "inverse"
            st.metric("Avg Flow", f"{flow}%" if flow else "N/A")
        
        st.markdown("---")
        
        tab1, tab2, tab3 = st.tabs(["🐢 Congestion Hotspots", "📊 Speed Analysis", "📋 Segment Data"])
        
        with tab1:
            st.subheader("🐢 Slowest Segments (Last 6 Hours)")
            slowest = get_slowest_segments()
            if not slowest.empty:
                fig = px.bar(
                    slowest,
                    x='FLOW_PCT',
                    y='ROADWAY_NAME',
                    orientation='h',
                    color='FLOW_PCT',
                    color_continuous_scale='RdYlGn',
                    title='Congested Segments (Lower % = More Congestion)',
                    labels={'FLOW_PCT': 'Flow %', 'ROADWAY_NAME': 'Roadway'}
                )
                fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(
                    slowest[['ROADWAY_NAME', 'DIRECTION', 'FROM_LOCATION', 'TO_LOCATION', 
                            'AVG_SPEED', 'AVG_FREE_FLOW', 'FLOW_PCT']],
                    use_container_width=True
                )
            else:
                st.info("No recent speed data")
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                speed_roadway = get_speed_by_roadway()
                if not speed_roadway.empty:
                    fig = px.scatter(
                        speed_roadway,
                        x='AVG_FREE_FLOW',
                        y='AVG_SPEED',
                        size='SEGMENTS',
                        color='FLOW_PCT',
                        hover_name='ROADWAY_NAME',
                        color_continuous_scale='RdYlGn',
                        title='Speed vs Free Flow by Roadway',
                        labels={'AVG_SPEED': 'Current Avg (mph)', 'AVG_FREE_FLOW': 'Free Flow (mph)'}
                    )
                    fig.add_trace(go.Scatter(
                        x=[0, 80], y=[0, 80],
                        mode='lines',
                        name='Optimal',
                        line=dict(dash='dash', color='gray')
                    ))
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if not speed_roadway.empty:
                    fig = px.histogram(
                        speed_roadway,
                        x='FLOW_PCT',
                        nbins=20,
                        title='Flow Percentage Distribution',
                        labels={'FLOW_PCT': 'Flow %', 'count': 'Roadways'},
                        color_discrete_sequence=['#ff6b35']
                    )
                    fig.add_vline(x=75, line_dash="dash", line_color="green", annotation_text="Good Flow")
                    fig.add_vline(x=50, line_dash="dash", line_color="red", annotation_text="Congested")
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            speeds = get_speed_data(200)
            if not speeds.empty:
                st.dataframe(speeds, use_container_width=True, height=500)
    
    except Exception as e:
        st.error(f"Error loading speeds: {e}")


def render_cameras():
    st.header("📷 Traffic Cameras")
    
    try:
        camera_stats = get_camera_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", f"{camera_stats['TOTAL_RECORDS'].iloc[0]:,}" if camera_stats['TOTAL_RECORDS'].iloc[0] else "0")
        with col2:
            st.metric("Unique Cameras", f"{camera_stats['UNIQUE_CAMERAS'].iloc[0]:,}" if camera_stats['UNIQUE_CAMERAS'].iloc[0] else "0")
        with col3:
            st.metric("Roadways", f"{camera_stats['UNIQUE_ROADWAYS'].iloc[0]:,}" if camera_stats['UNIQUE_ROADWAYS'].iloc[0] else "0")
        with col4:
            last = camera_stats['LAST_CAPTURE'].iloc[0]
            st.metric("Last Capture", last.strftime('%H:%M:%S') if pd.notna(last) else "N/A")
        
        st.markdown("---")
        
        cameras = get_cameras()
        
        if not cameras.empty:
            tab1, tab2 = st.tabs(["🗺️ Camera Map", "📋 Camera List"])
            
            with tab1:
                map_data = cameras[
                    cameras['LATITUDE'].notna() & 
                    cameras['LONGITUDE'].notna() &
                    (cameras['LATITUDE'].between(40, 45)) &
                    (cameras['LONGITUDE'].between(-76, -72))
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
                        get_color='[100, 200, 255, 200]',
                        get_radius=500,
                        pickable=True
                    )
                    
                    deck = pdk.Deck(
                        layers=[layer],
                        initial_view_state=view_state,
                        tooltip={"text": "{NAME}\n{ROADWAY_NAME}"}
                    )
                    
                    st.pydeck_chart(deck)
            
            with tab2:
                display_cols = ['CAMERA_ID', 'NAME', 'ROADWAY_NAME', 'DIRECTION_OF_TRAVEL', 'IMAGE_TIMESTAMP']
                available = [c for c in display_cols if c in cameras.columns]
                st.dataframe(cameras[available], use_container_width=True, height=500)
        else:
            st.info("No camera data available")
    
    except Exception as e:
        st.error(f"Error loading cameras: {e}")


def main():
    render_header()
    selected_view, auto_refresh = render_sidebar()
    
    if selected_view == "📊 Overview":
        render_overview()
    elif selected_view == "🚨 Traffic Events":
        render_events()
    elif selected_view == "🏎️ Traffic Speeds":
        render_speeds()
    elif selected_view == "📷 Cameras":
        render_cameras()
    
    if auto_refresh:
        import time
        time.sleep(60)
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
