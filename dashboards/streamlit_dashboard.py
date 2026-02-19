"""
Fleet Analytics Dashboard
Real-time visualization of fleet and delivery KPIs using Streamlit.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import random
import numpy as np

# Page config
st.set_page_config(
    page_title="Fleet Analytics Dashboard",
    page_icon="🚚",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    .kpi-value {
        font-size: 36px;
        font-weight: bold;
        color: #00ff88;
    }
    .kpi-label {
        font-size: 14px;
        color: #888;
    }
</style>
""", unsafe_allow_html=True)


def generate_sample_data():
    """Generate sample data for demonstration."""
    np.random.seed(42)
    
    # Generate hourly data for the past 24 hours
    hours = pd.date_range(end=datetime.now(), periods=24, freq='H')
    
    hourly_data = pd.DataFrame({
        'timestamp': hours,
        'active_vehicles': np.random.randint(80, 120, 24),
        'deliveries_completed': np.random.randint(150, 300, 24),
        'on_time_pct': np.random.uniform(85, 98, 24),
        'avg_delay_minutes': np.random.uniform(0, 15, 24),
        'fleet_utilization': np.random.uniform(70, 95, 24),
        'fuel_efficiency': np.random.uniform(18, 25, 24)
    })
    
    # Generate daily data for the past 30 days
    days = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    daily_data = pd.DataFrame({
        'date': days,
        'total_deliveries': np.random.randint(2000, 4000, 30),
        'successful_deliveries': np.random.randint(1900, 3900, 30),
        'on_time_deliveries': np.random.randint(1800, 3800, 30),
        'total_miles': np.random.randint(15000, 25000, 30),
        'fuel_cost': np.random.uniform(8000, 15000, 30),
        'cost_per_delivery': np.random.uniform(3, 8, 30)
    })
    
    # Generate vehicle data
    vehicles = pd.DataFrame({
        'vehicle_id': [f'V{str(i).zfill(4)}' for i in range(1, 101)],
        'status': np.random.choice(['Active', 'Idle', 'Maintenance', 'Off'], 100, p=[0.6, 0.2, 0.1, 0.1]),
        'latitude': np.random.uniform(39, 42, 100),
        'longitude': np.random.uniform(-75, -72, 100),
        'speed': np.random.uniform(0, 65, 100),
        'fuel_level': np.random.uniform(0.1, 1.0, 100),
        'deliveries_today': np.random.randint(0, 25, 100)
    })
    
    # Generate driver leaderboard
    drivers = pd.DataFrame({
        'driver_id': [f'D{str(i).zfill(4)}' for i in range(1, 21)],
        'name': [f'Driver {i}' for i in range(1, 21)],
        'deliveries': np.random.randint(50, 150, 20),
        'on_time_pct': np.random.uniform(85, 99, 20),
        'avg_rating': np.random.uniform(4.0, 5.0, 20),
        'miles_driven': np.random.randint(500, 1500, 20)
    }).sort_values('on_time_pct', ascending=False)
    
    return hourly_data, daily_data, vehicles, drivers


def render_kpi_cards(hourly_data, daily_data):
    """Render KPI cards at the top of the dashboard."""
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        current_deliveries = daily_data['total_deliveries'].iloc[-1]
        prev_deliveries = daily_data['total_deliveries'].iloc[-2]
        delta = ((current_deliveries - prev_deliveries) / prev_deliveries) * 100
        st.metric(
            label="Today's Deliveries",
            value=f"{current_deliveries:,}",
            delta=f"{delta:.1f}%"
        )
        
    with col2:
        on_time = hourly_data['on_time_pct'].iloc[-1]
        st.metric(
            label="On-Time Delivery %",
            value=f"{on_time:.1f}%",
            delta=f"{on_time - hourly_data['on_time_pct'].iloc[-2]:.1f}%"
        )
        
    with col3:
        active = hourly_data['active_vehicles'].iloc[-1]
        st.metric(
            label="Active Vehicles",
            value=f"{active}",
            delta=f"{active - hourly_data['active_vehicles'].iloc[-2]}"
        )
        
    with col4:
        avg_delay = hourly_data['avg_delay_minutes'].iloc[-1]
        st.metric(
            label="Avg Delay (min)",
            value=f"{avg_delay:.1f}",
            delta=f"{hourly_data['avg_delay_minutes'].iloc[-2] - avg_delay:.1f}",
            delta_color="inverse"
        )
        
    with col5:
        cost = daily_data['cost_per_delivery'].iloc[-1]
        st.metric(
            label="Cost per Delivery",
            value=f"${cost:.2f}",
            delta=f"${daily_data['cost_per_delivery'].iloc[-2] - cost:.2f}",
            delta_color="inverse"
        )


def render_delivery_trends(hourly_data, daily_data):
    """Render delivery trend charts."""
    st.subheader("📈 Delivery Trends")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Hourly deliveries
        fig = px.line(
            hourly_data,
            x='timestamp',
            y='deliveries_completed',
            title='Deliveries per Hour (Last 24h)',
            labels={'deliveries_completed': 'Deliveries', 'timestamp': 'Time'}
        )
        fig.update_layout(
            template='plotly_dark',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # On-time percentage trend
        fig = px.area(
            hourly_data,
            x='timestamp',
            y='on_time_pct',
            title='On-Time Delivery % (Last 24h)',
            labels={'on_time_pct': 'On-Time %', 'timestamp': 'Time'}
        )
        fig.update_layout(
            template='plotly_dark',
            height=300,
            yaxis_range=[80, 100]
        )
        fig.update_traces(fill='tozeroy', line_color='#00ff88')
        st.plotly_chart(fig, use_container_width=True)


def render_fleet_status(vehicles):
    """Render fleet status visualization."""
    st.subheader("🚚 Fleet Status")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Vehicle map
        fig = px.scatter_mapbox(
            vehicles,
            lat='latitude',
            lon='longitude',
            color='status',
            size='deliveries_today',
            hover_name='vehicle_id',
            hover_data=['speed', 'fuel_level'],
            title='Vehicle Locations',
            color_discrete_map={
                'Active': '#00ff88',
                'Idle': '#ffaa00',
                'Maintenance': '#ff4444',
                'Off': '#888888'
            },
            zoom=6
        )
        fig.update_layout(
            mapbox_style='carto-darkmatter',
            height=400,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Status breakdown
        status_counts = vehicles['status'].value_counts()
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title='Fleet Status Breakdown',
            color=status_counts.index,
            color_discrete_map={
                'Active': '#00ff88',
                'Idle': '#ffaa00',
                'Maintenance': '#ff4444',
                'Off': '#888888'
            }
        )
        fig.update_layout(
            template='plotly_dark',
            height=200
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Fuel status
        low_fuel = (vehicles['fuel_level'] < 0.25).sum()
        st.warning(f"⛽ {low_fuel} vehicles with low fuel (<25%)")


def render_performance_metrics(daily_data, drivers):
    """Render performance metrics and driver leaderboard."""
    st.subheader("📊 Performance Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Daily performance trend
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(
                x=daily_data['date'],
                y=daily_data['total_deliveries'],
                name='Total Deliveries',
                marker_color='#4488ff'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_data['date'],
                y=(daily_data['on_time_deliveries'] / daily_data['total_deliveries'] * 100),
                name='On-Time %',
                line=dict(color='#00ff88', width=2)
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title='Daily Delivery Performance (30 Days)',
            template='plotly_dark',
            height=350,
            legend=dict(orientation='h', yanchor='bottom', y=1.02)
        )
        fig.update_yaxes(title_text="Deliveries", secondary_y=False)
        fig.update_yaxes(title_text="On-Time %", secondary_y=True, range=[80, 100])
        
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Driver leaderboard
        st.markdown("**🏆 Top Drivers (This Week)**")
        
        for i, row in drivers.head(5).iterrows():
            col_a, col_b, col_c = st.columns([2, 1, 1])
            with col_a:
                st.write(f"**{row['name']}**")
            with col_b:
                st.write(f"{row['deliveries']} deliveries")
            with col_c:
                st.write(f"{row['on_time_pct']:.1f}% on-time")


def render_cost_analysis(daily_data):
    """Render cost analysis charts."""
    st.subheader("💰 Cost Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Cost per delivery trend
        fig = px.line(
            daily_data,
            x='date',
            y='cost_per_delivery',
            title='Cost per Delivery Trend',
            labels={'cost_per_delivery': 'Cost ($)', 'date': 'Date'}
        )
        fig.update_layout(
            template='plotly_dark',
            height=300
        )
        fig.add_hline(y=daily_data['cost_per_delivery'].mean(), 
                      line_dash="dash", line_color="red",
                      annotation_text="Average")
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Cost breakdown
        cost_data = pd.DataFrame({
            'Category': ['Fuel', 'Labor', 'Maintenance', 'Insurance', 'Other'],
            'Amount': [45000, 85000, 15000, 8000, 7000]
        })
        
        fig = px.pie(
            cost_data,
            values='Amount',
            names='Category',
            title='Monthly Cost Breakdown',
            hole=0.4
        )
        fig.update_layout(
            template='plotly_dark',
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard application."""
    st.title("🚚 Real-Time Fleet Analytics Dashboard")
    st.markdown("---")
    
    # Generate sample data
    hourly_data, daily_data, vehicles, drivers = generate_sample_data()
    
    # Auto-refresh toggle
    col1, col2 = st.columns([4, 1])
    with col2:
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
        
    if auto_refresh:
        st.markdown("*Dashboard refreshes every 30 seconds*")
        
    # Render dashboard sections
    render_kpi_cards(hourly_data, daily_data)
    st.markdown("---")
    
    render_delivery_trends(hourly_data, daily_data)
    st.markdown("---")
    
    render_fleet_status(vehicles)
    st.markdown("---")
    
    render_performance_metrics(daily_data, drivers)
    st.markdown("---")
    
    render_cost_analysis(daily_data)
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data source: AWS Kinesis → S3 → Redshift*"
    )
    
    # Auto-refresh
    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
