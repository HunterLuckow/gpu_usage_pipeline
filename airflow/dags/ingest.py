import os
import glob
import pandas as pd
from datetime import datetime, timedelta

import sys
import os
sys.path.append('/opt/airflow/plugins')

# Import mock catalog for testing
try:
    from mock_iceberg_catalog import catalog
    print("Using mock Iceberg catalog for testing")
except ImportError:
    print("Warning: Could not import mock catalog")
    catalog = None

RAW_DATA_DIR = "/opt/airflow/data/raw"
LOG_FILE = "/opt/airflow/data/ingestion_log.csv"

# Hardcoded assumptions
MEMORY_TOTAL_GB = 24.0  # max memory per GPU
INTERVAL_MINUTES = 1    # assume each log represents 1-minute interval

# Helper: append dataframe to Iceberg
def append_to_iceberg(table_name: str, df: pd.DataFrame):
    table = catalog.load_table(table_name)

    # Convert pandas dataframe → dict records
    records = df.to_dict(orient="records")

    with table.transaction() as tx:
        tx.append(records)
        tx.commit()

    print(f"[✓] Appended {len(df)} rows to {table_name}")

# Helper: upsert dimension records (avoid duplicates)
def upsert_dimension_records(table_name: str, df: pd.DataFrame, key_columns: list):
    """Upsert dimension records to avoid duplicates."""
    table = catalog.load_table(table_name)
    
    # For now, we'll use append but filter out duplicates in the dataframe
    # In a production system, you'd want proper upsert logic
    df_deduped = df.drop_duplicates(subset=key_columns)
    
    if len(df_deduped) > 0:
        records = df_deduped.to_dict(orient="records")
        with table.transaction() as tx:
            tx.append(records)
            tx.commit()
        print(f"[✓] Upserted {len(df_deduped)} unique records to {table_name}")
    else:
        print(f"[ℹ] No new unique records for {table_name}")


def categorize_usage(utilization_percent: float) -> str:
    """Categorize GPU usage as low, medium, or high."""
    if utilization_percent < 20:
        return "low"
    elif utilization_percent < 80:
        return "medium"
    else:
        return "high"

def compute_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Compute derived columns and map raw CSV to fact schema."""
    # Rename raw columns to match fact schema
    df = df.rename(columns={
        "usage_percent": "gpu_utilization_percent",
        "memory_mb": "memory_used_gb",
    })

    # Convert memory to GB
    df["memory_used_gb"] = df["memory_used_gb"] / 1024.0
    df["memory_total_gb"] = MEMORY_TOTAL_GB

    # Duration in seconds
    df["duration_seconds"] = INTERVAL_MINUTES * 60

    # Derived metrics
    df["usage_category"] = df["gpu_utilization_percent"].apply(
        lambda x: "low" if x < 20 else "medium" if x < 80 else "high"
    )
    df["memory_efficiency"] = df["memory_used_gb"] / df["memory_total_gb"]
    
    # Compute GPU efficiency ratio (new column for schema evolution demo)
    df["gpu_efficiency_ratio"] = df["gpu_utilization_percent"] / 100.0

    # Convert timestamp from epoch millis to datetime
    # Handle both epoch milliseconds and datetime strings
    try:
        # First try epoch milliseconds
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    except (ValueError, TypeError):
        try:
            # If epoch conversion fails, try parsing as datetime string
            df["timestamp"] = pd.to_datetime(df["timestamp"], format='mixed')
        except (ValueError, TypeError):
            # If all else fails, try to convert to numeric first
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors='coerce')
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    # Ensure string IDs
    df["job_id"] = df["job_id"].astype(str)
    df["node_id"] = df["node_id"].astype(str)
    df["gpu_id"] = df["gpu_id"].astype(str)

    return df


def log_ingestion(file_path: str, record_count: int):
    """Log ingestion metadata."""
    log_entry = {
        "file_name": os.path.basename(file_path),
        "timestamp": datetime.now().isoformat(),
        "records_ingested": record_count,
    }
    log_df = pd.DataFrame([log_entry])

    if os.path.exists(LOG_FILE):
        log_df.to_csv(LOG_FILE, mode="a", header=False, index=False)
    else:
        log_df.to_csv(LOG_FILE, index=False)


def get_unprocessed_files(raw_dir, log_file):
    """Get list of files that haven't been processed yet."""
    all_files = glob.glob(os.path.join(raw_dir, "*.csv"))
    if not all_files:
        return []
    
    # Read processed files from log
    processed_files = set()
    if os.path.exists(log_file):
        try:
            log_df = pd.read_csv(log_file)
            processed_files = set(log_df['file_name'].tolist())
        except Exception as e:
            print(f"Warning: Could not read log file: {e}")
    
    # Filter out already processed files
    unprocessed = [f for f in all_files if os.path.basename(f) not in processed_files]
    return unprocessed

def ingest_raw_data(raw_dir=RAW_DATA_DIR):
    """Main ingestion function."""
    unprocessed_files = get_unprocessed_files(raw_dir, LOG_FILE)
    if not unprocessed_files:
        print("⚠️ No new CSV files found to process in data/raw/")
        return

    # Check if catalog is available
    if catalog is None:
        print("⚠️ Iceberg catalog not available. Skipping ingestion.")
        print("This is expected if the catalog service is not running.")
        return

    # Load Iceberg tables
    tables = {
        "gpu_usage_fact": catalog.load_table("gpu_usage_fact"),
        "nodes_dim": catalog.load_table("nodes_dim"),
        "jobs_dim": catalog.load_table("jobs_dim"),
    }

    for file_path in unprocessed_files:
        print(f"→ Processing {file_path}")
        df = pd.read_csv(file_path)

        # Fact table transformation
        fact_df = compute_derived_columns(df)
        append_to_iceberg("gpu_usage_fact", fact_df)

        # Nodes dimension
        nodes_df = df[['node_id']].copy()
        nodes_df['node_name'] = "Node-" + nodes_df['node_id'].astype(str)
        nodes_df['cluster'] = "Cluster-1"
        upsert_dimension_records("nodes_dim", nodes_df, ['node_id'])

        # Jobs dimension
        jobs_df = df[['job_id']].copy()
        jobs_df['user'] = "demo_user"
        jobs_df['job_type'] = "training"
        jobs_df['submission_time'] = datetime.now()
        upsert_dimension_records("jobs_dim", jobs_df, ['job_id'])

        # Log ingestion
        log_ingestion(file_path, len(fact_df))
        print(f"[INFO] Ingested {len(fact_df)} records from {os.path.basename(file_path)} at {datetime.now()}")


if __name__ == "__main__":
    ingest_raw_data()

