# 🚀 GPU Usage Pipeline

A comprehensive data pipeline for monitoring, analyzing, and billing GPU usage in compute clusters. This project demonstrates a complete data engineering solution using Apache Airflow, PyIceberg, and modern analytics tools.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [Configuration](#configuration)
- [Testing](#testing)
- [Dashboard](#dashboard)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🎯 Overview

This project implements a production-ready GPU usage monitoring and billing pipeline that:

- **Generates** realistic GPU usage logs for testing
- **Ingests** raw data into a structured data warehouse
- **Transforms** data into analytical fact and dimension tables
- **Aggregates** usage metrics for monitoring and billing
- **Visualizes** results through an interactive dashboard
- **Orchestrates** everything with Apache Airflow

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│   Airflow DAG   │───▶│  Iceberg Tables │
│  (GPU Logs)     │    │   (Orchestrator)│    │   (Data Lake)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Aggregation   │───▶│   Dashboard     │
                       │   & Analytics   │    │  (Streamlit)    │
                       └─────────────────┘    └─────────────────┘
```

### Key Components:

- **Data Generation**: `generate_gpu_logs.py` - Creates realistic GPU usage data
- **Data Ingestion**: `ingest.py` - Transforms and loads data into Iceberg tables
- **Data Aggregation**: `aggregate.py` - Computes metrics and billing calculations
- **Orchestration**: Airflow DAG with three tasks: generate → ingest → aggregate
- **Storage**: Mock Iceberg catalog (easily replaceable with real Iceberg)
- **Visualization**: Streamlit dashboard with interactive charts

## ✨ Features

### 🔄 **Complete Data Pipeline**
- End-to-end data flow from generation to visualization
- Robust error handling and logging
- Idempotent operations (safe to re-run)

### 📊 **Rich Analytics**
- Time-based aggregations (hourly, daily, weekly, monthly)
- Multi-dimensional analysis (by node, GPU, job, user)
- Derived metrics (efficiency ratios, usage categories)

### 💰 **Billing System**
- GPU-hour based pricing
- Memory overage charges
- Cluster overhead calculations
- User/job cost allocation

### 🎨 **Interactive Dashboard**
- Real-time metrics and KPIs
- Interactive charts and filters
- Cost analysis and trends
- Performance monitoring

### 🧪 **Testing & Quality**
- Comprehensive unit tests
- Mock catalog for development
- Schema evolution support
- Data validation

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- 4GB+ RAM available

### 1. Clone and Setup

```bash
git clone <repository-url>
cd gpu_usage_pipeline
```

### 2. Start Airflow

```bash
cd airflow
docker-compose up -d
```

Wait for all services to start (2-3 minutes). Airflow will be available at:
- **Web UI**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

### 3. Run the Pipeline

1. **Trigger the DAG manually**:
   - Go to Airflow UI → DAGs → `gpu_usage_pipeline`
   - Click "Play" button to trigger a run

2. **Or wait for scheduled runs**:
   - The DAG runs automatically every hour

### 4. View Results

- **Data**: Check `/opt/airflow/data/` for generated files
- **Logs**: View in Airflow UI or `/opt/airflow/logs/`
- **Dashboard**: Run `streamlit run dashboard/app.py` (see Dashboard section)

## 📁 Project Structure

```
gpu_usage_pipeline/
├── airflow/                          # Airflow configuration
│   ├── dags/                        # DAG definitions
│   │   ├── gpu_usage_pipeline_dag.py # Main pipeline DAG
│   │   ├── generate_gpu_logs.py     # Data generation
│   │   ├── ingest.py                # Data ingestion
│   │   ├── aggregate.py             # Analytics & billing
│   │   ├── test_ingest.py           # Unit tests
│   │   └── test_aggregate.py        # Unit tests
│   ├── plugins/                     # Custom plugins
│   │   ├── iceberg_catalog.py       # Iceberg schema definitions
│   │   └── mock_iceberg_catalog.py  # Mock catalog for testing
│   ├── docker-compose.yaml          # Airflow services
│   ├── Dockerfile                   # Custom Airflow image
│   └── requirements.txt             # Python dependencies
├── dashboard/                       # Streamlit dashboard
│   ├── app.py                      # Main dashboard
│   └── requirements.txt            # Dashboard dependencies
├── data/                           # Data storage
│   ├── raw/                        # Raw GPU logs
│   ├── iceberg/                    # Iceberg tables (CSV format)
│   └── aggregates/                 # Aggregated data
└── README.md                       # This file
```

## 🔧 Pipeline Components

### 1. Data Generation (`generate_gpu_logs.py`)

Generates realistic GPU usage logs with:
- **Timestamps**: Epoch milliseconds or ISO format
- **Metrics**: GPU utilization, memory usage, job IDs
- **Variability**: Realistic usage patterns and distributions

```python
# Generate 2 hours of data
generate_logs(hours=2)
```

### 2. Data Ingestion (`ingest.py`)

Transforms raw logs into analytical tables:

**Fact Table (`gpu_usage_fact`)**:
- Primary usage metrics
- Derived columns (efficiency ratios, categories)
- Time-series data for analysis

**Dimension Tables**:
- `nodes_dim`: Node information
- `jobs_dim`: Job metadata

**Features**:
- Automatic timestamp parsing (epoch/ISO)
- Deduplication and upsert logic
- Ingestion state tracking
- Schema evolution support

### 3. Data Aggregation (`aggregate.py`)

Computes analytical metrics:

**Time Aggregations**:
- Hourly, daily, weekly, monthly summaries
- GPU hours, effective utilization
- Memory usage patterns

**Dimensional Aggregations**:
- Per-node performance metrics
- Per-job resource consumption
- User cost allocation

**Billing Calculations**:
- GPU-hour based pricing ($2.50/hour)
- Memory overage charges ($0.10/GB over 16GB)
- Cluster overhead (5%)

### 4. Airflow DAG (`gpu_usage_pipeline_dag.py`)

Three-task pipeline:
1. **`generate_logs`**: Creates new GPU usage data
2. **`ingest_to_iceberg`**: Loads data into analytical tables
3. **`aggregate_usage`**: Computes metrics and billing

**Schedule**: Runs every hour
**Dependencies**: generate → ingest → aggregate

## ⚙️ Configuration

### Environment Variables

```bash
# Airflow configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0

# GPU pricing (in aggregate.py)
GPU_HOURLY_RATE=2.50          # USD per GPU-hour
MEMORY_OVERAGE_RATE=0.10      # USD per GB over 16GB
CLUSTER_OVERHEAD_RATE=0.05    # 5% overhead
```

### Data Directories

```bash
/opt/airflow/data/
├── raw/           # Raw GPU logs (CSV)
├── iceberg/       # Iceberg tables (CSV format)
└── aggregates/    # Aggregated data (CSV)
```

### Iceberg Catalog

The project uses a **mock Iceberg catalog** for development:
- **Location**: `/opt/airflow/data/iceberg/`
- **Format**: CSV files (easily replaceable with real Iceberg)
- **Tables**: `gpu_usage_fact`, `nodes_dim`, `jobs_dim`

## 🧪 Testing

### Run Unit Tests

```bash
# Test ingestion pipeline
cd airflow
docker-compose exec airflow-worker python -m pytest dags/test_ingest.py -v

# Test aggregation functions
docker-compose exec airflow-worker python -m pytest dags/test_aggregate.py -v
```

### Test Data Generation

```bash
# Generate test data
docker-compose exec airflow-worker python -c "
from dags.generate_gpu_logs import generate_logs
generate_logs(hours=1)
"
```

### Test Ingestion

```bash
# Test ingestion pipeline
docker-compose exec airflow-worker python -c "
from dags.ingest import ingest_raw_data
ingest_raw_data()
"
```

### Test Aggregation

```bash
# Test aggregation pipeline
docker-compose exec airflow-worker python -c "
from dags.aggregate import run_aggregation_pipeline
run_aggregation_pipeline()
"
```

## 📊 Dashboard

### Start Dashboard

```bash
# Install dashboard dependencies
pip install -r dashboard/requirements.txt

# Start Streamlit dashboard
streamlit run dashboard/app.py
```

Dashboard will be available at: http://localhost:8501

### Dashboard Features

- **Real-time Metrics**: GPU hours, utilization, costs
- **Interactive Charts**: Time series, bar charts, scatter plots
- **Filtering**: By date range, nodes, jobs
- **Cost Analysis**: Billing breakdown, cost trends
- **Performance Monitoring**: Node and job performance

## 🔍 Troubleshooting

### Common Issues

**1. Airflow containers not starting**
```bash
# Check logs
docker-compose logs airflow-init

# Restart services
docker-compose down
docker-compose up -d
```

**2. Permission errors**
```bash
# Fix data directory permissions
sudo chown -R 50000:0 data/
```

**3. DAG not appearing**
```bash
# Check DAG parsing
docker-compose exec airflow-scheduler airflow dags list

# Restart scheduler
docker-compose restart airflow-scheduler
```

**4. Mock catalog issues**
```bash
# Clear mock data
rm -rf data/iceberg/*
```

### Log Locations

- **Airflow logs**: `/opt/airflow/logs/`
- **Application logs**: Docker container logs
- **Data files**: `/opt/airflow/data/`

### Debug Mode

```bash
# Run with debug logging
docker-compose exec airflow-worker python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from dags.ingest import ingest_raw_data
ingest_raw_data()
"
```

## 🤝 Contributing

### Development Setup

1. **Fork the repository**
2. **Create a feature branch**
3. **Make changes**
4. **Add tests**
5. **Run test suite**
6. **Submit pull request**

### Code Style

- **Python**: PEP 8 with 88-character line length
- **Documentation**: Google-style docstrings
- **Tests**: Unit tests for all functions
- **Type hints**: Use type annotations

### Testing Checklist

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] DAG runs successfully
- [ ] Dashboard displays correctly
- [ ] Documentation updated

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Apache Airflow** for workflow orchestration
- **PyIceberg** for data lake functionality
- **Streamlit** for interactive dashboards
- **Plotly** for beautiful visualizations

---

**Happy GPU monitoring! 🚀**
