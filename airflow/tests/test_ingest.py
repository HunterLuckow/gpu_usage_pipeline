"""
Unit tests for the GPU usage ingestion pipeline.

Tests cover:
- Data transformations (compute_derived_columns)
- Timestamp parsing with different formats
- Schema validation
- Deduplication logic
- Mock catalog functionality
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime
import tempfile
import os
import sys

# Add the dags directory to the path
sys.path.append('/opt/airflow/dags')
from ingest import compute_derived_columns, categorize_usage, get_unprocessed_files

# Add plugins directory to the path
sys.path.append('/opt/airflow/plugins')
from mock_iceberg_catalog import MockCatalog, MockTable


class TestDataTransformations(unittest.TestCase):
    """Test data transformation functions."""
    
    def setUp(self):
        """Set up test data."""
        self.sample_data = pd.DataFrame({
            'timestamp': [1734567890000, 1734567891000, 1734567892000],  # Epoch milliseconds
            'node_id': [1, 2, 3],
            'gpu_id': [0, 1, 2],
            'usage_percent': [25.5, 75.2, 95.8],
            'memory_mb': [12288, 18432, 20480],
            'job_id': [1001, 1002, 1003]
        })
    
    def test_compute_derived_columns(self):
        """Test that derived columns are computed correctly."""
        result = compute_derived_columns(self.sample_data.copy())
        
        # Check that all required columns exist
        expected_columns = [
            'job_id', 'node_id', 'gpu_id', 'timestamp', 'gpu_utilization_percent',
            'memory_used_gb', 'memory_total_gb', 'duration_seconds', 
            'usage_category', 'memory_efficiency'
        ]
        for col in expected_columns:
            self.assertIn(col, result.columns)
        
        # Check specific calculations
        self.assertEqual(result['gpu_utilization_percent'].iloc[0], 25.5)
        self.assertAlmostEqual(result['memory_used_gb'].iloc[0], 12288 / 1024.0, places=3)
        self.assertEqual(result['memory_total_gb'].iloc[0], 24.0)
        self.assertEqual(result['duration_seconds'].iloc[0], 60.0)
        
        # Check usage categories
        self.assertEqual(result['usage_category'].iloc[0], 'low')  # 25.5%
        self.assertEqual(result['usage_category'].iloc[1], 'medium')  # 75.2%
        self.assertEqual(result['usage_category'].iloc[2], 'high')  # 95.8%
        
        # Check memory efficiency
        expected_efficiency = (12288 / 1024.0) / 24.0
        self.assertAlmostEqual(result['memory_efficiency'].iloc[0], expected_efficiency, places=3)
    
    def test_timestamp_parsing_epoch(self):
        """Test timestamp parsing with epoch milliseconds."""
        data = pd.DataFrame({
            'timestamp': [1734567890000, 1734567891000],
            'node_id': [1, 2],
            'gpu_id': [0, 1],
            'usage_percent': [50, 60],
            'memory_mb': [12288, 18432],
            'job_id': [1001, 1002]
        })
        
        result = compute_derived_columns(data)
        
        # Check that timestamps are datetime objects
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['timestamp']))
        
        # Check specific timestamps (approximate)
        expected_time = pd.to_datetime('2024-12-19 12:31:30')  # Approximate
        self.assertTrue(abs(result['timestamp'].iloc[0] - expected_time) < pd.Timedelta(seconds=1))
    
    def test_timestamp_parsing_iso_string(self):
        """Test timestamp parsing with ISO datetime strings."""
        data = pd.DataFrame({
            'timestamp': ['2024-12-19T12:31:30.000Z', '2024-12-19T12:31:31.000Z'],
            'node_id': [1, 2],
            'gpu_id': [0, 1],
            'usage_percent': [50, 60],
            'memory_mb': [12288, 18432],
            'job_id': [1001, 1002]
        })
        
        result = compute_derived_columns(data)
        
        # Check that timestamps are datetime objects
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['timestamp']))
        
        # Check specific timestamps
        expected_time = pd.to_datetime('2024-12-19 12:31:30')
        self.assertEqual(result['timestamp'].iloc[0], expected_time)
    
    def test_string_id_conversion(self):
        """Test that ID columns are converted to strings."""
        result = compute_derived_columns(self.sample_data.copy())
        
        # Check that IDs are strings
        self.assertEqual(result['job_id'].dtype, 'object')
        self.assertEqual(result['node_id'].dtype, 'object')
        self.assertEqual(result['gpu_id'].dtype, 'object')
        
        # Check specific values
        self.assertEqual(result['job_id'].iloc[0], '1001')
        self.assertEqual(result['node_id'].iloc[0], '1')
        self.assertEqual(result['gpu_id'].iloc[0], '0')


class TestUsageCategorization(unittest.TestCase):
    """Test usage categorization logic."""
    
    def test_categorize_usage(self):
        """Test usage categorization function."""
        # Test low usage
        self.assertEqual(categorize_usage(10), 'low')
        self.assertEqual(categorize_usage(30), 'low')
        
        # Test medium usage
        self.assertEqual(categorize_usage(40), 'medium')
        self.assertEqual(categorize_usage(70), 'medium')
        
        # Test high usage
        self.assertEqual(categorize_usage(80), 'high')
        self.assertEqual(categorize_usage(95), 'high')
        
        # Test edge cases
        self.assertEqual(categorize_usage(0), 'low')
        self.assertEqual(categorize_usage(100), 'high')


class TestFileProcessing(unittest.TestCase):
    """Test file processing and deduplication logic."""
    
    def setUp(self):
        """Set up temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, 'ingestion_log.csv')
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_get_unprocessed_files_empty(self):
        """Test when no files exist."""
        files = get_unprocessed_files(self.temp_dir, self.log_file)
        self.assertEqual(files, [])
    
    def test_get_unprocessed_files_no_log(self):
        """Test when no log file exists."""
        # Create some test files
        test_files = ['file1.csv', 'file2.csv']
        for file in test_files:
            with open(os.path.join(self.temp_dir, file), 'w') as f:
                f.write('test')
        
        files = get_unprocessed_files(self.temp_dir, self.log_file)
        self.assertEqual(len(files), 2)
        self.assertTrue(all(f.endswith('.csv') for f in files))
    
    def test_get_unprocessed_files_with_log(self):
        """Test when log file exists with processed files."""
        # Create test files
        test_files = ['file1.csv', 'file2.csv', 'file3.csv']
        for file in test_files:
            with open(os.path.join(self.temp_dir, file), 'w') as f:
                f.write('test')
        
        # Create log file with processed files
        log_data = pd.DataFrame({
            'file_name': ['file1.csv'],
            'timestamp': ['2024-12-19T12:00:00'],
            'records_ingested': [100]
        })
        log_data.to_csv(self.log_file, index=False)
        
        files = get_unprocessed_files(self.temp_dir, self.log_file)
        self.assertEqual(len(files), 2)  # file2.csv and file3.csv
        self.assertNotIn('file1.csv', [os.path.basename(f) for f in files])


class TestMockCatalog(unittest.TestCase):
    """Test mock Iceberg catalog functionality."""
    
    def setUp(self):
        """Set up temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.catalog = MockCatalog(self.temp_dir)
    
    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_create_table(self):
        """Test table creation."""
        table = self.catalog.create_table('test_table', None, self.temp_dir)
        
        self.assertIsInstance(table, MockTable)
        self.assertEqual(table.name, 'test_table')
        self.assertTrue(os.path.exists(table.data_file))
    
    def test_load_table(self):
        """Test table loading."""
        # Load non-existent table (should create it)
        table = self.catalog.load_table('test_table')
        
        self.assertIsInstance(table, MockTable)
        self.assertEqual(table.name, 'test_table')
        self.assertTrue(os.path.exists(table.data_file))
    
    def test_table_transaction(self):
        """Test table transaction functionality."""
        table = self.catalog.create_table('test_table', None, self.temp_dir)
        
        # Test transaction
        with table.transaction() as tx:
            tx.append([
                {'id': 1, 'name': 'test1'},
                {'id': 2, 'name': 'test2'}
            ])
        
        # Check that data was written
        df = pd.read_csv(table.data_file)
        self.assertEqual(len(df), 2)
        self.assertEqual(df['id'].iloc[0], 1)
        self.assertEqual(df['name'].iloc[0], 'test1')
    
    def test_table_deduplication(self):
        """Test that duplicate records are handled correctly."""
        table = self.catalog.create_table('test_table', None, self.temp_dir)
        
        # Add initial data
        with table.transaction() as tx:
            tx.append([{'id': 1, 'name': 'test1'}])
        
        # Add more data (including duplicate)
        with table.transaction() as tx:
            tx.append([
                {'id': 1, 'name': 'test1'},  # Duplicate
                {'id': 2, 'name': 'test2'}   # New
            ])
        
        # Check that data was appended (mock catalog doesn't deduplicate)
        df = pd.read_csv(table.data_file)
        self.assertEqual(len(df), 3)  # All records including duplicate


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2) 