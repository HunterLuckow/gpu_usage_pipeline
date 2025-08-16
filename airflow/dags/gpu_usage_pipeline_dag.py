# airflow/dags/gpu_usage_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Import your GPU log generator and PySpark ingestion scripts
from generate_gpu_logs import generate_logs
# The Spark ingestion function will be implemented in phase 3
# from spark_jobs.ingest_to_iceberg import ingest_to_iceberg

def run_generate_logs():
    """Python callable to generate GPU logs."""
    generate_logs(hours=2)

def run_ingest_to_iceberg():
    """Python callable to ingest CSV logs into Iceberg via PySpark."""
    # Placeholder until Phase 3 is implemented
    print("Ingesting CSV logs into Iceberg...")
    # Example: ingest_to_iceberg()  

# Define the DAG
with DAG(
    dag_id="gpu_usage_pipeline",
    start_date=datetime(2025, 8, 15),
    schedule="@hourly",
    catchup=False,
    tags=["gpu", "billing"]
) as dag:

    generate_logs_task = PythonOperator(
        task_id="generate_logs",
        python_callable=run_generate_logs
    )

    ingest_to_iceberg_task = PythonOperator(
        task_id="ingest_to_iceberg",
        python_callable=run_ingest_to_iceberg
    )

    # Define task dependencies
    generate_logs_task >> ingest_to_iceberg_task
