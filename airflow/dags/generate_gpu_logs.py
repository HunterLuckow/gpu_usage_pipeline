import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple

OUTPUT_DIR = "/opt/airflow/data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Realistic GPU cluster configuration
CLUSTER_CONFIG = {
    "nodes": 5,
    "gpus_per_node": 4,
    "total_gpus": 20,
    "memory_per_gpu_gb": 24
}

# Job types with realistic characteristics
JOB_TYPES = {
    "training": {
        "utilization_range": (80, 95),
        "memory_usage_factor": 0.8,  # Uses 80% of available memory
        "duration_hours": (2, 12),
        "frequency": 0.4  # 40% of jobs are training
    },
    "inference": {
        "utilization_range": (20, 60),
        "memory_usage_factor": 0.4,  # Uses 40% of available memory
        "duration_hours": (0.5, 4),
        "frequency": 0.35  # 35% of jobs are inference
    },
    "data_processing": {
        "utilization_range": (30, 70),
        "memory_usage_factor": 0.6,  # Uses 60% of available memory
        "duration_hours": (1, 6),
        "frequency": 0.15  # 15% of jobs are data processing
    },
    "idle": {
        "utilization_range": (5, 15),
        "memory_usage_factor": 0.1,  # Uses 10% of available memory
        "duration_hours": (0.5, 2),
        "frequency": 0.1  # 10% of jobs are idle
    }
}

class RealisticGPUJob:
    """Represents a realistic GPU job with persistence and patterns."""
    
    def __init__(self, job_id: int, job_type: str, start_time: datetime):
        self.job_id = job_id
        self.job_type = job_type
        self.start_time = start_time
        
        # Set realistic job characteristics
        config = JOB_TYPES[job_type]
        self.duration_hours = random.uniform(*config["duration_hours"])
        self.end_time = start_time + timedelta(hours=self.duration_hours)
        self.memory_factor = config["memory_usage_factor"]
        self.utilization_range = config["utilization_range"]
        
        # Assign to specific GPU
        self.node_id = random.randint(1, CLUSTER_CONFIG["nodes"])
        self.gpu_id = random.randint(0, CLUSTER_CONFIG["gpus_per_node"] - 1)
        
        # Job-specific patterns
        self.base_utilization = random.uniform(*self.utilization_range)
        self.utilization_variance = random.uniform(5, 15)  # How much utilization varies
        
    def get_utilization_at_time(self, current_time: datetime) -> float:
        """Get realistic utilization for this job at a specific time."""
        if current_time < self.start_time or current_time > self.end_time:
            return 0.0
            
        # Add realistic variation (training jobs have more variance)
        if self.job_type == "training":
            variation = random.uniform(-self.utilization_variance, self.utilization_variance)
        else:
            variation = random.uniform(-self.utilization_variance/2, self.utilization_variance/2)
            
        utilization = self.base_utilization + variation
        return max(0, min(100, utilization))
    
    def get_memory_usage(self, utilization: float) -> int:
        """Get realistic memory usage based on utilization and job type."""
        # Base memory usage for job type
        base_memory_gb = CLUSTER_CONFIG["memory_per_gpu_gb"] * self.memory_factor
        
        # Add correlation with utilization (higher usage = more memory)
        utilization_factor = 0.5 + (utilization / 100) * 0.5
        
        # Add some randomness
        memory_gb = base_memory_gb * utilization_factor * random.uniform(0.9, 1.1)
        
        return int(memory_gb * 1024)  # Convert to MB

def is_work_hours(current_time: datetime) -> bool:
    """Check if current time is during work hours."""
    hour = current_time.hour
    is_weekday = current_time.weekday() < 5  # Monday = 0, Sunday = 6
    
    # Work hours: 9 AM - 6 PM on weekdays
    return is_weekday and 9 <= hour <= 18

def get_time_multiplier(current_time: datetime) -> float:
    """Get activity multiplier based on time of day."""
    hour = current_time.hour
    is_weekday = current_time.weekday() < 5
    
    if not is_weekday:
        # Weekend: much lower activity
        return 0.3
    elif 9 <= hour <= 17:
        # Core work hours: high activity
        return 1.0
    elif 7 <= hour <= 9 or 17 <= hour <= 19:
        # Transition hours: medium activity
        return 0.7
    else:
        # Night hours: low activity
        return 0.4

def generate_realistic_logs(hours: int = 1, interval_minutes: int = 1) -> None:
    """Generate realistic GPU usage logs with job persistence and patterns."""
    
    start_time = datetime.now() - timedelta(hours=hours)
    end_time = datetime.now()
    current_time = start_time
    
    # Track active jobs
    active_jobs: Dict[int, RealisticGPUJob] = {}
    next_job_id = 1000
    records = []
    
    while current_time <= end_time:
        time_multiplier = get_time_multiplier(current_time)
        
        # Create new jobs based on time and current load
        if len(active_jobs) < CLUSTER_CONFIG["total_gpus"] * 0.8:  # Don't overload
            # Probability of new job depends on time multiplier
            if random.random() < 0.1 * time_multiplier:  # Base 10% chance, modified by time
                # Select job type based on frequencies
                job_type = random.choices(
                    list(JOB_TYPES.keys()),
                    weights=[JOB_TYPES[jt]["frequency"] for jt in JOB_TYPES.keys()]
                )[0]
                
                # Create new job
                new_job = RealisticGPUJob(next_job_id, job_type, current_time)
                active_jobs[next_job_id] = new_job
                next_job_id += 1
        
        # Generate records for each GPU
        for node_id in range(1, CLUSTER_CONFIG["nodes"] + 1):
            for gpu_id in range(CLUSTER_CONFIG["gpus_per_node"]):
                
                # Find if this GPU has an active job
                active_job = None
                for job in active_jobs.values():
                    if job.node_id == node_id and job.gpu_id == gpu_id:
                        active_job = job
                        break
                
                if active_job:
                    # GPU has active job
                    utilization = active_job.get_utilization_at_time(current_time)
                    memory_mb = active_job.get_memory_usage(utilization)
                    job_id = active_job.job_id
                    
                    # Remove job if it's finished
                    if current_time > active_job.end_time:
                        del active_jobs[active_job.job_id]
                        
                else:
                    # GPU is idle
                    utilization = random.uniform(1, 5)  # Very low idle usage
                    memory_mb = random.randint(100, 500)  # Minimal memory usage
                    job_id = 0  # 0 indicates no job
                
                # Add realistic noise and ensure bounds
                utilization = max(0, min(100, utilization + random.uniform(-2, 2)))
                memory_mb = max(100, min(CLUSTER_CONFIG["memory_per_gpu_gb"] * 1024, memory_mb))
                
                record = {
                    "timestamp": int(current_time.timestamp() * 1000),
                    "node_id": node_id,
                    "gpu_id": gpu_id,
                    "usage_percent": round(utilization, 2),
                    "memory_mb": int(memory_mb),
                    "job_id": job_id
                }
                records.append(record)
        
        current_time += timedelta(minutes=interval_minutes)
    
    # Create DataFrame and save
    df = pd.DataFrame(records)
    filename = os.path.join(OUTPUT_DIR, f"gpu_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(filename, index=False)
    
    # Print summary statistics
    total_records = len(df)
    avg_utilization = df['usage_percent'].mean()
    active_jobs_count = df[df['job_id'] > 0]['job_id'].nunique()
    
    print(f"Generated: {filename}")
    print(f"Records: {total_records}")
    print(f"Average GPU utilization: {avg_utilization:.1f}%")
    print(f"Active jobs: {active_jobs_count}")
    print(f"Time span: {start_time} to {end_time}")

def generate_logs(hours: int = 1, interval_minutes: int = 1) -> None:
    """Main function to generate realistic GPU logs."""
    generate_realistic_logs(hours, interval_minutes)

if __name__ == "__main__":
    generate_logs(hours=2)  # Generate last 2 hours
