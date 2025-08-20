import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, TimestampType

warehouse_path = "/opt/airflow/data/iceberg"
os.makedirs(warehouse_path, exist_ok=True)

# For now, let's create a simple catalog configuration
# We'll use a basic setup that should work with the available catalog types
try:
    catalog = load_catalog(
        "gpu_catalog",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": f"file://{warehouse_path}",
        },
    )
except Exception as e:
    print(f"Warning: Could not load catalog: {e}")
    print("Creating a mock catalog for testing...")
    catalog = None

# Define fact table schema
from pyiceberg.types import NestedField

fact_schema = Schema(
    NestedField(1, "job_id", StringType(), required=True),
    NestedField(2, "node_id", StringType(), required=True),
    NestedField(3, "gpu_id", StringType(), required=True),
    NestedField(4, "timestamp", TimestampType(), required=True),
    NestedField(5, "gpu_utilization_percent", DoubleType(), required=True),
    NestedField(6, "memory_used_gb", DoubleType(), required=True),
    NestedField(7, "memory_total_gb", DoubleType(), required=True),
    NestedField(8, "duration_seconds", DoubleType(), required=True),
    NestedField(9, "usage_category", StringType(), required=True),
    NestedField(10, "memory_efficiency", DoubleType(), required=True),
    NestedField(11, "gpu_efficiency_ratio", DoubleType(), required=False)  # New column for schema evolution demo
)

# Only create tables if catalog is available
if catalog is not None:
    try:
        gpu_usage_fact = catalog.create_table(
            identifier="gpu_usage_fact",
            schema=fact_schema,
            location=f"{warehouse_path}/gpu_usage_fact"
        )

        # Nodes dimension table
        nodes_schema = Schema(
            NestedField(1, "node_id", StringType(), required=True),
            NestedField(2, "node_name", StringType(), required=True),
            NestedField(3, "cluster", StringType(), required=True)
        )

        nodes_dim = catalog.create_table(
            identifier="nodes_dim",
            schema=nodes_schema,
            location=f"{warehouse_path}/nodes_dim"
        )

        # Jobs dimension table
        jobs_schema = Schema(
            NestedField(1, "job_id", StringType(), required=True),
            NestedField(2, "user", StringType(), required=True),
            NestedField(3, "job_type", StringType(), required=True),
            NestedField(4, "submission_time", TimestampType(), required=True)
        )

        jobs_dim = catalog.create_table(
            identifier="jobs_dim",
            schema=jobs_schema,
            location=f"{warehouse_path}/jobs_dim"
        )
    except Exception as e:
        print(f"Warning: Could not create tables: {e}")
        gpu_usage_fact = None
        nodes_dim = None
        jobs_dim = None
else:
    gpu_usage_fact = None
    nodes_dim = None
    jobs_dim = None

