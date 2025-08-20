"""
GPU Usage Aggregation and Analytics Module

This module provides functions to:
1. Aggregate GPU usage data by different dimensions (node, GPU, job, time)
2. Compute billing metrics based on usage
3. Generate summary tables for monitoring and reporting
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from typing import Dict, List, Optional

# Add plugins directory to path
sys.path.append('/opt/airflow/plugins')

# Import catalog conditionally
try:
    from mock_iceberg_catalog import catalog
    print("Using mock Iceberg catalog for aggregation")
except ImportError:
    print("Warning: Could not import mock catalog")
    catalog = None

# Configuration
GPU_HOURLY_RATE = 2.50  # USD per GPU-hour
MEMORY_OVERAGE_RATE = 0.10  # USD per GB over 16GB
CLUSTER_OVERHEAD_RATE = 0.05  # 5% overhead on total costs

# Output directories
AGGREGATE_DATA_DIR = "/opt/airflow/data/aggregates"
os.makedirs(AGGREGATE_DATA_DIR, exist_ok=True)


def load_fact_table() -> pd.DataFrame:
    """Load the GPU usage fact table."""
    if catalog is None:
        print("⚠️ Catalog not available. Cannot load fact table.")
        return pd.DataFrame()
    
    try:
        table = catalog.load_table("gpu_usage_fact")
        if os.path.exists(table.data_file):
            df = pd.read_csv(table.data_file)
            # Convert timestamp back to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        else:
            print("⚠️ Fact table file not found.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error loading fact table: {e}")
        return pd.DataFrame()


def aggregate_by_time_interval(df: pd.DataFrame, interval: str = 'hourly') -> pd.DataFrame:
    """
    Aggregate GPU usage by time intervals.
    
    Args:
        df: Fact table DataFrame
        interval: 'hourly', 'daily', 'weekly', 'monthly'
    
    Returns:
        Aggregated DataFrame with metrics
    """
    if df.empty:
        return pd.DataFrame()
    
    # Set timestamp as index for resampling
    df_copy = df.copy()
    df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
    df_copy.set_index('timestamp', inplace=True)
    
    # Define resampling rule
    resample_rules = {
        'hourly': 'H',
        'daily': 'D',
        'weekly': 'W',
        'monthly': 'M'
    }
    
    rule = resample_rules.get(interval, 'H')
    
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


def calculate_billing(df: pd.DataFrame, user_mapping: Optional[Dict] = None) -> pd.DataFrame:
    """
    Calculate billing metrics for GPU usage.
    
    Args:
        df: Fact table DataFrame
        user_mapping: Optional mapping of job_id to user_id
    
    Returns:
        DataFrame with billing calculations
    """
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
    
    # Add user information if mapping provided
    if user_mapping:
        billing_df['user_id'] = billing_df['job_id'].map(user_mapping).fillna('unknown')
    else:
        billing_df['user_id'] = 'demo_user'  # Default
    
    # Round monetary values
    monetary_cols = ['gpu_cost', 'memory_overage_cost', 'subtotal', 'cluster_overhead', 'total_cost']
    billing_df[monetary_cols] = billing_df[monetary_cols].round(2)
    
    return billing_df


def save_aggregates(aggregates: Dict[str, pd.DataFrame]):
    """Save aggregated data to CSV files."""
    for name, df in aggregates.items():
        if not df.empty:
            file_path = os.path.join(AGGREGATE_DATA_DIR, f"{name}.csv")
            df.to_csv(file_path, index=False)
            print(f"[✓] Saved {name} with {len(df)} records to {file_path}")


def append_to_iceberg(table_name: str, df: pd.DataFrame):
    """Append data to Iceberg table."""
    if catalog is None:
        print(f"⚠️ Catalog not available. Skipping append to {table_name}")
        return
    
    try:
        table = catalog.load_table(table_name)
        records = df.to_dict(orient="records")
        
        with table.transaction() as tx:
            tx.append(records)
            tx.commit()
        
        print(f"[✓] Appended {len(records)} records to {table_name}")
    except Exception as e:
        print(f"Error appending to {table_name}: {e}")


def run_aggregation_pipeline():
    """Main aggregation pipeline function."""
    print("🔄 Starting GPU usage aggregation pipeline...")
    
    # Load fact table
    fact_df = load_fact_table()
    if fact_df.empty:
        print("⚠️ No fact table data available for aggregation.")
        return
    
    print(f"📊 Loaded {len(fact_df)} records from fact table")
    
    # Generate aggregates
    aggregates = {}
    
    # Time-based aggregates
    print("⏰ Computing time-based aggregates...")
    aggregates['hourly_usage'] = aggregate_by_time_interval(fact_df, 'hourly')
    aggregates['daily_usage'] = aggregate_by_time_interval(fact_df, 'daily')
    
    # Dimension-based aggregates
    print("🏢 Computing node-based aggregates...")
    aggregates['node_usage'] = aggregate_by_node(fact_df)
    
    print("💼 Computing job-based aggregates...")
    aggregates['job_usage'] = aggregate_by_job(fact_df)
    
    # Billing calculations
    print("💰 Computing billing metrics...")
    aggregates['billing'] = calculate_billing(fact_df)
    
    # Save to CSV files
    print("💾 Saving aggregates to CSV files...")
    save_aggregates(aggregates)
    
    # Append to Iceberg tables (if catalog available)
    if catalog is not None:
        print("🗄️ Appending aggregates to Iceberg tables...")
        for name, df in aggregates.items():
            if not df.empty:
                append_to_iceberg(f"{name}_agg", df)
    
    print("✅ Aggregation pipeline completed successfully!")
    
    # Print summary statistics
    print("\n📈 Summary Statistics:")
    for name, df in aggregates.items():
        if not df.empty:
            print(f"  {name}: {len(df)} records")
            if 'total_cost' in df.columns:
                total_cost = df['total_cost'].sum()
                print(f"    Total cost: ${total_cost:.2f}")


if __name__ == "__main__":
    run_aggregation_pipeline() 