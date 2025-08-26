"""
GPU Usage Dashboard

A Streamlit application to visualize GPU usage metrics and billing data.
Enhanced to showcase realistic GPU simulation patterns.
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
DATA_DIR = "../airflow/data"
ICEBERG_DIR = os.path.join(DATA_DIR, "iceberg")
AGGREGATES_DIR = os.path.join(DATA_DIR, "aggregates")

# Configuration
GPU_HOURLY_RATE = 2.50  # USD per GPU-hour
MEMORY_OVERAGE_RATE = 0.10  # USD per GB over 16GB
CLUSTER_OVERHEAD_RATE = 0.05  # 5% overhead on total costs

# Job type classification based on utilization patterns
def classify_job_type(utilization: float, memory_gb: float) -> str:
    """Classify job type based on utilization and memory patterns."""
    if utilization >= 80:
        return "Training"
    elif utilization >= 20:
        return "Inference"
    elif utilization >= 5:
        return "Data Processing"
    else:
        return "Idle"

def is_work_hours(timestamp: pd.Timestamp) -> bool:
    """Check if timestamp is during work hours."""
    hour = timestamp.hour
    is_weekday = timestamp.weekday() < 5
    return is_weekday and 9 <= hour <= 18

def load_fact_table():
    """Load the GPU usage fact table from local CSV."""
    fact_file = os.path.join(ICEBERG_DIR, "gpu_usage_fact.csv")
    if os.path.exists(fact_file):
        df = pd.read_csv(fact_file)
        # Convert timestamp back to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Add derived columns for enhanced analysis
        df['job_type'] = df.apply(lambda x: classify_job_type(x['gpu_utilization_percent'], x['memory_used_gb']), axis=1)
        df['is_work_hours'] = df['timestamp'].apply(is_work_hours)
        df['hour_of_day'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.day_name()
        df['memory_efficiency'] = df['memory_used_gb'] / df['gpu_utilization_percent'] * 100  # GB per % utilization
        
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
        'gpu_id': 'nunique',   # Count unique GPUs
        'is_work_hours': 'mean'  # Percentage of work hours
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
        'gpu_efficiency_ratio': 'mean',
        'memory_efficiency': 'mean',
        'job_type': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown'
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
        'gpu_efficiency_ratio': 'mean',
        'memory_efficiency': 'mean',
        'job_type': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown',
        'is_work_hours': 'mean'
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
        'gpu_id': 'nunique',
        'job_type': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown'
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

def analyze_job_persistence(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze job persistence patterns over time."""
    if df.empty:
        return pd.DataFrame()
    
    # Group by job and analyze time spans
    job_analysis = df.groupby('job_id').agg({
        'timestamp': ['min', 'max'],
        'gpu_utilization_percent': 'mean',
        'memory_used_gb': 'mean',
        'job_type': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown',
        'node_id': 'nunique',
        'gpu_id': 'nunique'
    }).round(2)
    
    # Flatten column names
    job_analysis.columns = [f"{col[0]}_{col[1]}" for col in job_analysis.columns]
    
    # Calculate duration
    job_analysis['duration_hours'] = (job_analysis['timestamp_max'] - job_analysis['timestamp_min']).dt.total_seconds() / 3600
    
    job_analysis.reset_index(inplace=True)
    return job_analysis

# Page configuration
st.set_page_config(
    page_title="GPU Usage Dashboard - Enhanced",
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
    .job-type-training {
        color: #2ca02c;
        font-weight: bold;
    }
    .job-type-inference {
        color: #ff7f0e;
        font-weight: bold;
    }
    .job-type-idle {
        color: #7f7f7f;
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
        data['job_persistence'] = analyze_job_persistence(fact_df)
    
    return data

def main():
    """Main dashboard function."""
    
    # Header
    st.markdown('<h1 class="main-header">🚀 Enhanced GPU Usage Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666;">Realistic GPU Simulation with Job Persistence, Time Patterns & Advanced Analytics</p>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading enhanced data..."):
        data = load_data()
    
    if not data or data['fact'].empty:
        st.error("No data available. Please ensure the ingestion pipeline has run.")
        st.info("Data should be available in the ../airflow/data/iceberg/ directory")
        return
    
    # Sidebar filters
    st.sidebar.header("📊 Enhanced Filters")
    
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
    
    # Job type filter
    if 'fact' in data and not data['fact'].empty:
        job_types = data['fact']['job_type'].unique()
        selected_job_types = st.sidebar.multiselect(
            "Select Job Types",
            options=job_types,
            default=job_types
        )
    
    # Node filter
    if 'nodes' in data and not data['nodes'].empty:
        selected_nodes = st.sidebar.multiselect(
            "Select Nodes",
            options=data['nodes']['node_id'].unique(),
            default=data['nodes']['node_id'].unique()
        )
    
    # Enhanced KPIs
    st.markdown("## 📈 Enhanced Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_gpu_hours = data['fact']['duration_seconds'].sum() / 3600
        st.metric(
            "Total GPU Hours",
            f"{total_gpu_hours:.1f}",
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
    
    # New Enhanced KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        work_hours_utilization = data['fact'][data['fact']['is_work_hours']]['gpu_utilization_percent'].mean()
        st.metric(
            "Work Hours Utilization",
            f"{work_hours_utilization:.1f}%",
            delta="+15.2%"
        )
    
    with col2:
        training_jobs = len(data['fact'][data['fact']['job_type'] == 'Training'])
        total_records = len(data['fact'])
        training_percentage = (training_jobs / total_records) * 100
        st.metric(
            "Training Jobs %",
            f"{training_percentage:.1f}%",
            delta="+5.3%"
        )
    
    with col3:
        avg_memory_efficiency = data['fact']['memory_efficiency'].mean()
        st.metric(
            "Memory Efficiency",
            f"{avg_memory_efficiency:.1f} GB/%",
            delta="+2.1 GB/%"
        )
    
    with col4:
        job_persistence_avg = data['job_persistence']['duration_hours'].mean() if 'job_persistence' in data else 0
        st.metric(
            "Avg Job Duration",
            f"{job_persistence_avg:.1f}h",
            delta="+0.5h"
        )
    
    # Job Type Analysis
    st.markdown("---")
    st.markdown("## 🎯 Job Type Analysis")
    
    if 'fact' in data and not data['fact'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Job type distribution
            job_type_counts = data['fact']['job_type'].value_counts()
            fig = px.pie(
                values=job_type_counts.values,
                names=job_type_counts.index,
                title='Job Type Distribution',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Job type performance
            job_type_performance = data['fact'].groupby('job_type').agg({
                'gpu_utilization_percent': 'mean',
                'memory_used_gb': 'mean',
                'memory_efficiency': 'mean'
            }).round(2)
            
            fig = px.bar(
                job_type_performance,
                y='gpu_utilization_percent',
                title='Average GPU Utilization by Job Type',
                color='gpu_utilization_percent',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Time-Based Pattern Analysis
    st.markdown("---")
    st.markdown("## ⏰ Time-Based Pattern Analysis")
    
    if 'hourly' in data and not data['hourly'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Work hours vs off-hours comparison
            work_hours_data = data['fact'].groupby('is_work_hours').agg({
                'gpu_utilization_percent': 'mean',
                'job_id': 'nunique'
            }).round(2)
            
            fig = px.bar(
                work_hours_data,
                y='gpu_utilization_percent',
                title='GPU Utilization: Work Hours vs Off-Hours',
                color='gpu_utilization_percent',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Hourly activity pattern
            hourly_activity = data['fact'].groupby('hour_of_day').agg({
                'gpu_utilization_percent': 'mean',
                'job_id': 'nunique'
            }).round(2)
            
            fig = px.line(
                hourly_activity,
                y='gpu_utilization_percent',
                title='Hourly Activity Pattern (24-Hour Cycle)',
                markers=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced Time Series
    st.markdown("---")
    st.markdown("## 📈 Enhanced Time Series Analysis")
    
    if 'hourly' in data and not data['hourly'].empty:
        st.subheader("📈 GPU Utilization Over Time (with Work Hours Highlighting)")
        
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('GPU Utilization (%)', 'Memory Usage (GB)', 'Active Jobs'),
            vertical_spacing=0.1
        )
        
        # GPU Utilization with work hours highlighting
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
        
        # Add work hours highlighting
        work_hours_mask = data['hourly']['is_work_hours_mean'] > 0.5
        if work_hours_mask.any():
            fig.add_trace(
                go.Scatter(
                    x=data['hourly'][work_hours_mask]['time_period'],
                    y=data['hourly'][work_hours_mask]['gpu_utilization_percent_mean'],
                    mode='markers',
                    name='Work Hours',
                    marker=dict(color='#2ca02c', size=8)
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
        
        # Active Jobs
        fig.add_trace(
            go.Scatter(
                x=data['hourly']['time_period'],
                y=data['hourly']['job_id_nunique'],
                mode='lines+markers',
                name='Active Jobs',
                line=dict(color='#ff7f0e')
            ),
            row=3, col=1
        )
        
        fig.update_layout(height=800, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    # Job Persistence Analysis
    st.markdown("---")
    st.markdown("## 🔄 Job Persistence Analysis")
    
    if 'job_persistence' in data and not data['job_persistence'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Job duration distribution
            fig = px.histogram(
                data['job_persistence'],
                x='duration_hours',
                nbins=20,
                title='Job Duration Distribution',
                color='job_type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Job type vs duration
            fig = px.box(
                data['job_persistence'],
                x='job_type',
                y='duration_hours',
                title='Job Duration by Type',
                color='job_type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Memory Efficiency Analysis
    st.markdown("---")
    st.markdown("## 💾 Memory Efficiency Analysis")
    
    if 'fact' in data and not data['fact'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Memory efficiency by job type
            fig = px.box(
                data['fact'],
                x='job_type',
                y='memory_efficiency',
                title='Memory Efficiency by Job Type',
                color='job_type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Memory vs Utilization correlation
            fig = px.scatter(
                data['fact'],
                x='gpu_utilization_percent',
                y='memory_used_gb',
                color='job_type',
                title='Memory Usage vs GPU Utilization',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced Node and Job Analysis
    st.markdown("---")
    st.markdown("## 🏢 Enhanced Node & Job Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'nodes' in data and not data['nodes'].empty:
            st.subheader("🏢 Node Performance (Enhanced)")
            
            fig = px.bar(
                data['nodes'],
                x='node_id',
                y='gpu_utilization_percent_mean',
                title='Average GPU Utilization by Node',
                color='memory_efficiency_mean',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'jobs' in data and not data['jobs'].empty:
            st.subheader("💼 Job Performance (Enhanced)")
            
            # Top 10 jobs by GPU hours with job type
            top_jobs = data['jobs'].nlargest(10, 'gpu_hours')
            
            fig = px.bar(
                top_jobs,
                x='job_id',
                y='gpu_hours',
                title='Top 10 Jobs by GPU Hours',
                color='job_type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced Billing Analysis
    st.markdown("---")
    st.markdown("## 💰 Enhanced Billing Analysis")
    
    if 'billing' in data and not data['billing'].empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost breakdown by job type
            fig = px.pie(
                data['billing'],
                values='total_cost',
                names='job_type',
                title='Cost Distribution by Job Type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Cost vs Utilization by job type
            fig = px.scatter(
                data['billing'],
                x='gpu_utilization_percent',
                y='total_cost',
                size='gpu_hours',
                color='job_type',
                hover_data=['job_id', 'memory_used_gb'],
                title='Cost vs GPU Utilization by Job Type',
                color_discrete_map={
                    'Training': '#2ca02c',
                    'Inference': '#ff7f0e',
                    'Data Processing': '#1f77b4',
                    'Idle': '#7f7f7f'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced Data Tables
    st.markdown("---")
    st.markdown("## 📋 Enhanced Data Tables")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Hourly Aggregates", "Node Aggregates", "Job Aggregates", "Job Persistence", "Billing"])
    
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
        if 'job_persistence' in data and not data['job_persistence'].empty:
            st.dataframe(data['job_persistence'], use_container_width=True)
    
    with tab5:
        if 'billing' in data and not data['billing'].empty:
            st.dataframe(data['billing'], use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>Enhanced Dashboard last updated: {}</p>
            <p>Data source: Realistic GPU Usage Pipeline | Powered by Streamlit & Plotly</p>
            <p>Features: Job Persistence, Time Patterns, Memory Efficiency, Advanced Analytics</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main() 