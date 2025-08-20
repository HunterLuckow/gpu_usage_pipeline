"""
Unit tests for the GPU usage aggregation module.

Tests cover:
- Time-based aggregation functions
- Node and job aggregation
- Billing calculations
- Data loading and saving
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
import sys

# Add the dags directory to the path
sys.path.append('/opt/airflow/dags')
from aggregate import (
    aggregate_by_time_interval, 
    aggregate_by_node, 
    aggregate_by_job, 
    calculate_billing,
    GPU_HOURLY_RATE,
    MEMORY_OVERAGE_RATE,
    CLUSTER_OVERHEAD_RATE
)


class TestAggregationFunctions(unittest.TestCase):
    """Test aggregation functions."""
    
    def setUp(self):
        """Set up test data."""
        # Create sample fact table data
        timestamps = pd.date_range('2024-12-19 10:00:00', periods=10, freq='H')
        
        self.sample_data = pd.DataFrame({
            'timestamp': timestamps,
            'node_id': [1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
            'gpu_id': [0, 0, 1, 1, 0, 0, 1, 1, 0, 0],
            'job_id': [1001, 1002, 1001, 1002, 1003, 1001, 1002, 1003, 1001, 1002],
            'gpu_utilization_percent': [25.5, 75.2, 45.8, 90.1, 15.3, 60.7, 85.4, 30.2, 55.9, 70.8],
            'memory_used_gb': [12.0, 18.5, 14.2, 20.1, 8.5, 16.8, 19.2, 11.3, 15.7, 17.9],
            'memory_total_gb': [24.0] * 10,
            'duration_seconds': [3600] * 10,
            'usage_category': ['low', 'medium', 'medium', 'high', 'low', 'medium', 'high', 'low', 'medium', 'medium'],
            'memory_efficiency': [0.5, 0.77, 0.59, 0.84, 0.35, 0.7, 0.8, 0.47, 0.65, 0.75],
            'gpu_efficiency_ratio': [0.255, 0.752, 0.458, 0.901, 0.153, 0.607, 0.854, 0.302, 0.559, 0.708]
        })
    
    def test_aggregate_by_time_interval_hourly(self):
        """Test hourly aggregation."""
        result = aggregate_by_time_interval(self.sample_data, 'hourly')
        
        # Check that result is not empty
        self.assertFalse(result.empty)
        
        # Check expected columns
        expected_columns = [
            'time_period', 'gpu_utilization_percent_mean', 'gpu_utilization_percent_max',
            'gpu_utilization_percent_min', 'memory_used_gb_mean', 'memory_used_gb_max',
            'memory_used_gb_sum', 'duration_seconds_sum', 'job_id_nunique', 'node_id_nunique',
            'gpu_id_nunique', 'gpu_hours', 'effective_gpu_hours'
        ]
        
        for col in expected_columns:
            self.assertIn(col, result.columns)
        
        # Check that we have hourly data points
        self.assertEqual(len(result), 10)  # 10 hours of data
        
        # Check GPU hours calculation
        expected_gpu_hours = 3600 / 3600  # 1 hour per record
        self.assertAlmostEqual(result['gpu_hours'].iloc[0], expected_gpu_hours, places=2)
    
    def test_aggregate_by_time_interval_daily(self):
        """Test daily aggregation."""
        result = aggregate_by_time_interval(self.sample_data, 'daily')
        
        # Check that result is not empty
        self.assertFalse(result.empty)
        
        # Should have 1 day of data
        self.assertEqual(len(result), 1)
        
        # Check that daily aggregation sums up correctly
        expected_duration_sum = 3600 * 10  # 10 records * 3600 seconds
        self.assertEqual(result['duration_seconds_sum'].iloc[0], expected_duration_sum)
    
    def test_aggregate_by_node(self):
        """Test node aggregation."""
        result = aggregate_by_node(self.sample_data)
        
        # Check that result is not empty
        self.assertFalse(result.empty)
        
        # Should have 2 nodes
        self.assertEqual(len(result), 2)
        
        # Check that node_ids are present
        self.assertIn(1, result['node_id'].values)
        self.assertIn(2, result['node_id'].values)
        
        # Check GPU hours calculation
        expected_gpu_hours_per_node = (3600 * 5) / 3600  # 5 records per node * 1 hour
        self.assertAlmostEqual(result['gpu_hours'].iloc[0], expected_gpu_hours_per_node, places=2)
    
    def test_aggregate_by_job(self):
        """Test job aggregation."""
        result = aggregate_by_job(self.sample_data)
        
        # Check that result is not empty
        self.assertFalse(result.empty)
        
        # Should have 3 unique jobs
        self.assertEqual(len(result), 3)
        
        # Check that job_ids are present
        self.assertIn(1001, result['job_id'].values)
        self.assertIn(1002, result['job_id'].values)
        self.assertIn(1003, result['job_id'].values)
    
    def test_calculate_billing(self):
        """Test billing calculations."""
        result = calculate_billing(self.sample_data)
        
        # Check that result is not empty
        self.assertFalse(result.empty)
        
        # Should have 3 jobs
        self.assertEqual(len(result), 3)
        
        # Check billing columns
        billing_columns = [
            'job_id', 'duration_seconds', 'gpu_utilization_percent', 'memory_used_gb',
            'gpu_id', 'gpu_hours', 'effective_gpu_hours', 'gpu_cost', 'memory_overage_gb',
            'memory_overage_cost', 'subtotal', 'cluster_overhead', 'total_cost', 'user_id'
        ]
        
        for col in billing_columns:
            self.assertIn(col, result.columns)
        
        # Check cost calculations for one job
        job_1001 = result[result['job_id'] == 1001].iloc[0]
        
        # GPU hours should be duration / 3600
        expected_gpu_hours = job_1001['duration_seconds'] / 3600
        self.assertAlmostEqual(job_1001['gpu_hours'], expected_gpu_hours, places=2)
        
        # Effective GPU hours should be weighted by utilization
        expected_effective_hours = expected_gpu_hours * (job_1001['gpu_utilization_percent'] / 100)
        self.assertAlmostEqual(job_1001['effective_gpu_hours'], expected_effective_hours, places=2)
        
        # GPU cost should be effective hours * rate
        expected_gpu_cost = expected_effective_hours * GPU_HOURLY_RATE
        self.assertAlmostEqual(job_1001['gpu_cost'], expected_gpu_cost, places=2)
        
        # Memory overage should be max(0, memory_used - 16)
        expected_memory_overage = max(0, job_1001['memory_used_gb'] - 16.0)
        self.assertAlmostEqual(job_1001['memory_overage_gb'], expected_memory_overage, places=2)
        
        # Subtotal should be GPU cost + memory overage cost
        expected_subtotal = job_1001['gpu_cost'] + job_1001['memory_overage_cost']
        self.assertAlmostEqual(job_1001['subtotal'], expected_subtotal, places=2)
        
        # Cluster overhead should be 5% of subtotal
        expected_overhead = expected_subtotal * CLUSTER_OVERHEAD_RATE
        self.assertAlmostEqual(job_1001['cluster_overhead'], expected_overhead, places=2)
        
        # Total cost should be subtotal + overhead
        expected_total = expected_subtotal + expected_overhead
        self.assertAlmostEqual(job_1001['total_cost'], expected_total, places=2)
    
    def test_calculate_billing_with_user_mapping(self):
        """Test billing calculations with user mapping."""
        user_mapping = {1001: 'user1', 1002: 'user2', 1003: 'user1'}
        result = calculate_billing(self.sample_data, user_mapping)
        
        # Check that user_id column is populated correctly
        job_1001 = result[result['job_id'] == 1001].iloc[0]
        self.assertEqual(job_1001['user_id'], 'user1')
        
        job_1002 = result[result['job_id'] == 1002].iloc[0]
        self.assertEqual(job_1002['user_id'], 'user2')
    
    def test_empty_data_handling(self):
        """Test that functions handle empty data gracefully."""
        empty_df = pd.DataFrame()
        
        # All functions should return empty DataFrame for empty input
        self.assertTrue(aggregate_by_time_interval(empty_df).empty)
        self.assertTrue(aggregate_by_node(empty_df).empty)
        self.assertTrue(aggregate_by_job(empty_df).empty)
        self.assertTrue(calculate_billing(empty_df).empty)


class TestConfiguration(unittest.TestCase):
    """Test configuration constants."""
    
    def test_billing_rates(self):
        """Test that billing rates are reasonable."""
        self.assertGreater(GPU_HOURLY_RATE, 0)
        self.assertLess(GPU_HOURLY_RATE, 100)  # Should be reasonable hourly rate
        
        self.assertGreater(MEMORY_OVERAGE_RATE, 0)
        self.assertLess(MEMORY_OVERAGE_RATE, 10)  # Should be reasonable per GB rate
        
        self.assertGreater(CLUSTER_OVERHEAD_RATE, 0)
        self.assertLess(CLUSTER_OVERHEAD_RATE, 1)  # Should be percentage less than 100%


if __name__ == '__main__':
    unittest.main(verbosity=2) 