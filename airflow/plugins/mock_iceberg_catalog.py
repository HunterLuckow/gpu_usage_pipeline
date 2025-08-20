"""
Mock Iceberg Catalog for Testing

This module provides a mock implementation of Iceberg catalog functionality
for testing the ingestion pipeline without requiring a full Iceberg setup.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

class MockTable:
    """Mock Iceberg table that stores data in CSV files for testing."""
    
    def __init__(self, name: str, location: str):
        self.name = name
        self.location = location
        # Create files directly in the iceberg directory, not in subdirectories
        self.data_file = f"{location}/{name}.csv"
        self.metadata_file = f"{location}/{name}_metadata.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(location, exist_ok=True)
        
        # Initialize empty table if it doesn't exist
        if not os.path.exists(self.data_file):
            # Create an empty DataFrame with a dummy column to avoid pandas errors
            pd.DataFrame({'dummy': []}).to_csv(self.data_file, index=False)
    
    def transaction(self):
        """Mock transaction context manager."""
        return MockTransaction(self)
    
    def __repr__(self):
        return f"MockTable(name='{self.name}', location='{self.location}')"

class MockTransaction:
    """Mock transaction for appending data to mock tables."""
    
    def __init__(self, table: MockTable):
        self.table = table
        self.records = []
    
    def append(self, records: List[Dict[str, Any]]):
        """Append records to the transaction."""
        self.records.extend(records)
    
    def commit(self):
        """Commit the transaction by writing to CSV."""
        if self.records:
            # Read existing data
            if os.path.exists(self.table.data_file) and os.path.getsize(self.table.data_file) > 0:
                existing_df = pd.read_csv(self.table.data_file)
                # Remove dummy column if it exists
                if 'dummy' in existing_df.columns and len(existing_df) == 0:
                    existing_df = pd.DataFrame()
            else:
                existing_df = pd.DataFrame()
            
            # Convert new records to DataFrame
            new_df = pd.DataFrame(self.records)
            
            # Combine existing and new data
            if not existing_df.empty:
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Write to CSV
            combined_df.to_csv(self.table.data_file, index=False)
            
            # Log metadata
            self._log_metadata(len(self.records))
    
    def _log_metadata(self, record_count: int):
        """Log metadata about the transaction."""
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'records_appended': record_count,
            'table_name': self.table.name
        }
        
        metadata_df = pd.DataFrame([metadata])
        
        if os.path.exists(self.table.metadata_file):
            metadata_df.to_csv(self.table.metadata_file, mode='a', header=False, index=False)
        else:
            metadata_df.to_csv(self.table.metadata_file, index=False)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()

class MockCatalog:
    """Mock Iceberg catalog that manages tables."""
    
    def __init__(self, warehouse_path: str):
        self.warehouse_path = warehouse_path
        self.tables = {}
        os.makedirs(warehouse_path, exist_ok=True)
    
    def create_table(self, identifier: str, schema, location: str):
        """Create a mock table."""
        table = MockTable(identifier, self.warehouse_path)
        self.tables[identifier] = table
        print(f"[MOCK] Created table: {identifier} at {self.warehouse_path}")
        return table
    
    def load_table(self, identifier: str):
        """Load an existing mock table."""
        if identifier in self.tables:
            return self.tables[identifier]
        else:
            # Create table if it doesn't exist
            table = MockTable(identifier, self.warehouse_path)
            self.tables[identifier] = table
            print(f"[MOCK] Loaded/created table: {identifier}")
            return table
    
    def list_tables(self):
        """List all tables in the catalog."""
        return list(self.tables.keys())
    
    def get_table_info(self, identifier: str):
        """Get information about a table."""
        if identifier in self.tables:
            table = self.tables[identifier]
            if os.path.exists(table.data_file):
                df = pd.read_csv(table.data_file)
                return {
                    'name': identifier,
                    'location': table.location,
                    'record_count': len(df),
                    'columns': list(df.columns) if not df.empty else []
                }
        return None

# Create a global mock catalog instance
warehouse_path = "/opt/airflow/data/iceberg"
mock_catalog = MockCatalog(warehouse_path)

# Export the mock catalog as 'catalog' to match the expected interface
catalog = mock_catalog 