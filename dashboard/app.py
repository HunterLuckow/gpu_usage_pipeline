"""
GPU Usage Dashboard

A Streamlit application to visualize GPU usage metrics and billing data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
import sys
import numpy as np

# Local data paths
DATA_DIR = "../data"
ICEBERG_DIR = os.path.join(DATA_DIR, "iceberg")
AGGREGATES_DIR = os.path.join(DATA_DIR, "aggregates")

# Configuration
GPU_HOURLY_RATE = 2.50  # USD per GPU-hour
MEMORY_OVERAGE_RATE = 0.10  # USD per GB over 16GB
CLUSTER_OVERHEAD_RATE = 0.05  # 5% overhead on total costs

def load_fact_table():
    """Load the GPU usage fact table from local CSV."""
    fact_file = os.path.join(ICEBERG_DIR, "gpu_usage_fact.csv")
    if os.path.exists(fact_file):
        df = pd.read_csv(fact_file)
        # Convert timestamp back to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    else:
        print(f"Fact table file not found: {fact_file}")
        return pd.DataFrame()

def aggregate_by_time_interval(df: pd.DataFrame, interval: str = 'hourly') -> pd.DataFrame:
    """Aggregate GPU usage by time intervals."""
    if df.empty:
        return pd.DataFrame()

    # Set timestamp as index for resampling
    df_copy = df.copy()
    df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
    df_copy.set_index('timestamp', inplace=True)

    # Define resampling rule
    resample_rules = {
        'hourly': 'h',
        'daily': 'D',
        'weekly': 'W',
        'monthly': 'M'
    }

    rule = resample_rules.get(interval, 'h')

    # Aggregate metrics
    agg_df = df_copy.resample(rule).agg({
        'gpu_utilization_percent': ['mean', 'max', 'min'],
        'memory_used_gb': ['mean', 'max', 'sum'],
        'duration_seconds': 'sum',
        'job_id': 'nunique',  # Count unique jobs
        'node_id': 'nunique',  # Count unique nodes
        'gpu_id': 'nunique'    # Count unique GPUs
    }).round(2)

    # Flatten column names
    agg_df.columns = [f"{col[0]}_{col[1]}" for col in agg_df.columns]

    # Calculate GPU hours
    agg_df['gpu_hours'] = agg_df['duration_seconds_sum'] / 3600

    # Calculate effective GPU hours (weighted by utilization)
    agg_df['effective_gpu_hours'] = (agg_df['duration_seconds_sum'] / 3600) * (agg_df['gpu_utilization_percent_mean'] / 100)

    # Reset index to get timestamp as column
    agg_df.reset_index(inplace=True)
    agg_df.rename(columns={'timestamp': 'time_period'}, inplace=True)

    return agg_df

def aggregate_by_node(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate GPU usage by node."""
    if df.empty:
        return pd.DataFrame()

    agg_df = df.groupby('node_id').agg({
        'gpu_utilization_percent': ['mean', 'max', 'min'],
        'memory_used_gb': ['mean', 'max', 'sum'],
        'duration_seconds': 'sum',
        'job_id': 'nunique',
        'gpu_id': 'nunique',
        'gpu_efficiency_ratio': 'mean'
    }).round(2)

    # Flatten column names
    agg_df.columns = [f"{col[0]}_{col[1]}" for col in agg_df.columns]

    # Calculate metrics
    agg_df['gpu_hours'] = agg_df['duration_seconds_sum'] / 3600
    agg_df['effective_gpu_hours'] = (agg_df['duration_seconds_sum'] / 3600) * (agg_df['gpu_utilization_percent_mean'] / 100)
    agg_df['memory_hours'] = agg_df['memory_used_gb_sum'] * (agg_df['duration_seconds_sum'] / 3600)

    agg_df.reset_index(inplace=True)
    return agg_df

def aggregate_by_job(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate GPU usage by job."""
    if df.empty:
        return pd.DataFrame()

    agg_df = df.groupby('job_id').agg({
        'gpu_utilization_percent': ['mean', 'max', 'min'],
        'memory_used_gb': ['mean', 'max', 'sum'],
        'duration_seconds': 'sum',
        'node_id': 'nunique',
        'gpu_id': 'nunique',
        'gpu_efficiency_ratio': 'mean'
    }).round(2)

    # Flatten column names
    agg_df.columns = [f"{col[0]}_{col[1]}" for col in agg_df.columns]

    # Calculate metrics
    agg_df['gpu_hours'] = agg_df['duration_seconds_sum'] / 3600
    agg_df['effective_gpu_hours'] = (agg_df['duration_seconds_sum'] / 3600) * (agg_df['gpu_utilization_percent_mean'] / 100)
    agg_df['memory_hours'] = agg_df['memory_used_gb_sum'] * (agg_df['duration_seconds_sum'] / 3600)

    agg_df.reset_index(inplace=True)
    return agg_df

def calculate_billing(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate billing metrics for GPU usage."""
    if df.empty:
        return pd.DataFrame()

    # Group by job for billing
    billing_df = df.groupby('job_id').agg({
        'duration_seconds': 'sum',
        'gpu_utilization_percent': 'mean',
        'memory_used_gb': 'mean',
        'gpu_id': 'nunique'
    }).reset_index()

    # Calculate GPU hours
    billing_df['gpu_hours'] = billing_df['duration_seconds'] / 3600
    billing_df['effective_gpu_hours'] = billing_df['gpu_hours'] * (billing_df['gpu_utilization_percent'] / 100)

    # Calculate costs
    billing_df['gpu_cost'] = billing_df['effective_gpu_hours'] * GPU_HOURLY_RATE

    # Memory overage costs (if using more than 16GB average)
    billing_df['memory_overage_gb'] = np.maximum(0, billing_df['memory_used_gb'] - 16.0)
    billing_df['memory_overage_cost'] = billing_df['memory_overage_gb'] * MEMORY_OVERAGE_RATE * billing_df['gpu_hours']

    # Total costs
    billing_df['subtotal'] = billing_df['gpu_cost'] + billing_df['memory_overage_cost']
    billing_df['cluster_overhead'] = billing_df['subtotal'] * CLUSTER_OVERHEAD_RATE
    billing_df['total_cost'] = billing_df['subtotal'] + billing_df['cluster_overhead']

    # Add user information
    billing_df['user_id'] = 'demo_user'  # Default

    # Round monetary values
    monetary_cols = ['gpu_cost', 'memory_overage_cost', 'subtotal', 'cluster_overhead', 'total_cost']
    billing_df[monetary_cols] = billing_df[monetary_cols].round(2)

    return billing_df

# Page configuration
st.set_page_config(
    page_title="GPU Usage Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .cost-metric {
        color: #d62728;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load all data sources."""
    data = {}
    
    # Load fact table
    fact_df = load_fact_table()
    if not fact_df.empty:
        data['fact'] = fact_df
        
        # Generate aggregates
        data['hourly'] = aggregate_by_time_interval(fact_df, 'hourly')
        data['daily'] = aggregate_by_time_interval(fact_df, 'daily')
        data['nodes'] = aggregate_by_node(fact_df)
        data['jobs'] = aggregate_by_job(fact_df)
        data['billing'] = calculate_billing(fact_df)
    
    return data

def main():
    """Main dashboard function."""
    
    # Header
    st.markdown('<h1 class="main-header">🚀 GPU Usage Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data..."):
        data = load_data()
    
    if not data or data['fact'].empty:
        st.error("No data available. Please ensure the ingestion pipeline has run.")
        st.info("Data should be available in the ../data/iceberg/ directory")
        return
    
    # Sidebar filters
    st.sidebar.header("📊 Filters")
    
    # Time range filter
    if 'hourly' in data and not data['hourly'].empty:
        min_date = data['hourly']['time_period'].min()
        max_date = data['hourly']['time_period'].max()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date.date(), max_date.date()),
            min_value=min_date.date(),
            max_value=max_date.date()
        )
    
    # Node filter
    if 'nodes' in data and not data['nodes'].empty:
        selected_nodes = st.sidebar.multiselect(
            "Select Nodes",
            options=data['nodes']['node_id'].unique(),
            default=data['nodes']['node_id'].unique()
        )
    
    # Main content
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total GPU Hours",
            f"{data['fact']['duration_seconds'].sum() / 3600:.1f}",
            delta="+2.5 hours"
        )
    
    with col2:
        avg_utilization = data['fact']['gpu_utilization_percent'].mean()
        st.metric(
            "Avg GPU Utilization",
            f"{avg_utilization:.1f}%",
            delta=f"{avg_utilization - 50:.1f}%"
        )
    
    with col3:
        total_cost = data['billing']['total_cost'].sum() if 'billing' in data else 0
        st.metric(
            "Total Cost",
            f"${total_cost:.2f}",
            delta="+$12.50"
        )
    
    with col4:
        active_jobs = data['fact']['job_id'].nunique()
        st.metric(
            "Active Jobs",
            active_jobs,
            delta="+3"
        )
    
    # Charts
    st.markdown("---")
    
    # Time series chart
    if 'hourly' in data and not data['hourly'].empty:
        st.subheader("📈 GPU Utilization Over Time")
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('GPU Utilization (%)', 'Memory Usage (GB)'),
            vertical_spacing=0.1
        )
        
        # GPU Utilization
        fig.add_trace(
            go.Scatter(
                x=data['hourly']['time_period'],
                y=data['hourly']['gpu_utilization_percent_mean'],
                mode='lines+markers',
                name='Avg Utilization',
                line=dict(color='#1f77b4')
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=data['hourly']['time_period'],
                y=data['hourly']['gpu_utilization_percent_max'],
                mode='lines',
                name='Max Utilization',
                line=dict(color='#ff7f0e', dash='dash')
            ),
            row=1, col=1
        )
        
        # Memory Usage
        fig.add_trace(
            go.Scatter(
                x=data['hourly']['time_period'],
                y=data['hourly']['memory_used_gb_mean'],
                mode='lines+markers',
                name='Avg Memory',
                line=dict(color='#2ca02c')
            ),
            row=2, col=1
        )
        
        fig.update_layout(height=600, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    # Node and Job Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        if 'nodes' in data and not data['nodes'].empty:
            st.subheader("🏢 Node Performance")
            
            fig = px.bar(
                data['nodes'],
                x='node_id',
                y='gpu_utilization_percent_mean',
                title='Average GPU Utilization by Node',
                color='gpu_utilization_percent_mean',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'jobs' in data and not data['jobs'].empty:
            st.subheader("💼 Job Performance")
            
            # Top 10 jobs by GPU hours
            top_jobs = data['jobs'].nlargest(10, 'gpu_hours')
            
            fig = px.bar(
                top_jobs,
                x='job_id',
                y='gpu_hours',
                title='Top 10 Jobs by GPU Hours',
                color='gpu_utilization_percent_mean',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Billing Analysis
    st.markdown("---")
    st.subheader("💰 Billing Analysis")
    
    if 'billing' in data and not data['billing'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost breakdown
            fig = px.pie(
                data['billing'],
                values='total_cost',
                names='job_id',
                title='Cost Distribution by Job'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Cost vs Utilization
            fig = px.scatter(
                data['billing'],
                x='gpu_utilization_percent',
                y='total_cost',
                size='gpu_hours',
                hover_data=['job_id', 'memory_used_gb'],
                title='Cost vs GPU Utilization'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Data Tables
    st.markdown("---")
    st.subheader("📋 Detailed Data")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Hourly Aggregates", "Node Aggregates", "Job Aggregates", "Billing"])
    
    with tab1:
        if 'hourly' in data and not data['hourly'].empty:
            st.dataframe(data['hourly'], use_container_width=True)
    
    with tab2:
        if 'nodes' in data and not data['nodes'].empty:
            st.dataframe(data['nodes'], use_container_width=True)
    
    with tab3:
        if 'jobs' in data and not data['jobs'].empty:
            st.dataframe(data['jobs'], use_container_width=True)
    
    with tab4:
        if 'billing' in data and not data['billing'].empty:
            st.dataframe(data['billing'], use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>Dashboard last updated: {}</p>
            <p>Data source: GPU Usage Pipeline | Powered by Streamlit & Plotly</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main() 