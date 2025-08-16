import pandas as pd
import random
from datetime import datetime, timedelta
import os

OUTPUT_DIR = "/opt/airflow/data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_logs(hours=1, interval_minutes=1):
    """Generate fake GPU usage logs for N hours."""
    start_time = datetime.now() - timedelta(hours=hours)
    end_time = datetime.now()
    records = []

    current_time = start_time
    while current_time <= end_time:
        record = {
            "timestamp": current_time.isoformat(),
            "node_id": random.randint(1, 5),  # Simulate 5 GPU nodes
            "gpu_id": random.randint(0, 3),   # Each node has 4 GPUs
            "usage_percent": round(random.uniform(0, 100), 2),
            "memory_mb": random.randint(1000, 24000),
            "job_id": random.randint(1000, 2000)
        }
        records.append(record)
        current_time += timedelta(minutes=interval_minutes)

    df = pd.DataFrame(records)
    filename = os.path.join(OUTPUT_DIR, f"gpu_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(filename, index=False)
    print(f"Generated: {filename}")

if __name__ == "__main__":
    generate_logs(hours=2)  # Generate last 2 hours
