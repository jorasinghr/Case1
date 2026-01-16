import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import json

class LocalStorage:
    """Store data locally in parquet files"""
    def __init__(self, base_path='data'):
        self.lake_path = Path(base_path) 
        self.bronze_path = self.lake_path/'bronze'
        self.silver_path =  self.lake_path/'silver'
        self.gold_path =  self.lake_path/'gold'
        self.metadata_path =  self.lake_path/'_metadata'

        # Create directory structure
        for path in [self.bronze_path, self.silver_path, self.gold_path, self.metadata_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def save(self, df, layer, filename):
        import os
        path = getattr(self, layer)
        os.makedirs(path, exist_ok=True)
        df.to_parquet(f'{path}/{filename}.parquet', index=False)


    def get_data(self, url, keys):
        '''
        Get data from REST API
        '''
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        for key in keys:
            data = data[key]
            if data is None:
                return pd.DataFrame()  # Return empty DataFrame
        
        df = pd.DataFrame(data)
        return df

    def load_to_bronze(self, df, table_name, partition_by_date=True):
        # Add metadata
        df_bronze = df.copy()
        ingestion_time = datetime.now()
        df_bronze['_ingestion_timestamp'] = ingestion_time
        df_bronze['_ingestion_date'] = ingestion_time.date()
        
        # Create partition path
        if partition_by_date:
            partition_date = ingestion_time.strftime('%Y-%m-%d')
            table_path = self.bronze_path / table_name / f'date={partition_date}'
        else:
            table_path = self.bronze_path / table_name
        
        table_path.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet
        timestamp = ingestion_time.strftime('%Y%m%d_%H%M%S')
        file_path = table_path / f'{table_name}_{timestamp}.parquet'
        df_bronze.to_parquet(file_path, index=False, compression='snappy')
        
        # Save metadata
        self._save_metadata('bronze', table_name, {
            'records': len(df_bronze),
            'columns': list(df_bronze.columns),
            'file_path': str(file_path),
            'ingestion_timestamp': str(ingestion_time)
        })
        
        return df_bronze
    
    def _save_metadata(self, layer, table_name, metadata):
        """Save pipeline metadata for tracking and lineage"""
        metadata_file = self.metadata_path / f'{layer}_{table_name}_metadata.json'
        
        # Load existing metadata
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                existing = json.load(f)
        else:
            existing = {'runs': []}
        
        # Add new run
        existing['runs'].append(metadata)
        
        # Save
        with open(metadata_file, 'w') as f:
            json.dump(existing, f, indent=2)
    
    def quality_checks_1(self, df):
        """Data quality validation"""
        failed = []
        
        # Check for required columns
        required_cols = ['station_id', 'name']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            failed.append(f"Missing columns: {missing_cols}")
        
        # Check for nulls in key columns
        if 'station_id' in df.columns and df['station_id'].isnull().any():
            failed.append("Null values in station_id")
        
        return failed
    
    def quality_checks_2(self, df):
        """Data quality validation finance"""
        failed = []
        
        # Check for required columns
        required_cols = ['transaction_id', 'user_id']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            failed.append(f"Missing columns: {missing_cols}")
        
        # Check for nulls in key columns
        if 'transaction_id' in df.columns and df['transaction_id'].isnull().any():
            failed.append("Null values in transaction_id")
        
        return failed
    
    def transform_stations(self, df):
        """Custom transformations for bike data"""
        # Parse nested address if exists
        if 'address' in df.columns:
            df['address_str'] = df['address'].astype(str)
        
        # Add calculated fields
        if 'lat' in df.columns and 'lon' in df.columns:
            df['location'] = df['lat'].astype(str) + ',' + df['lon'].astype(str)
        
        return df
    
    def transform_to_silver(self, bronze_df, table_name, transformations=None, quality_checks=None):
        
        df = bronze_df.copy()
        
        # Remove metadata columns
        metadata_cols = [col for col in df.columns if col.startswith('_')]
        df = df.drop(columns=metadata_cols)
        
        # 1. Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_count:
            print(f"[SILVER] Removed {initial_count - len(df)} duplicates")
        
        # 2. Handle missing values
        df = df.dropna(how='all')
        
        # 3. Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-z0-9_]', '', regex=True)
        
        # 4. Apply custom transformations
        if transformations:
            df = transformations(df)
        
        # 5. Data quality checks
        if quality_checks:
            failed_checks = quality_checks(df)
            if failed_checks:
                print(f"[SILVER] Quality check failures: {failed_checks}")
        
        # Add processing metadata
        df['_processed_timestamp'] = datetime.now()
        
        # Save to Silver layer
        table_path = self.silver_path / table_name
        table_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = table_path / f'{table_name}_{timestamp}.parquet'
        df.to_parquet(file_path, index=False, compression='snappy')
        
        # Save metadata
        self._save_metadata('silver', table_name, {
            'records': len(df),
            'columns': list(df.columns),
            'file_path': str(file_path),
            'processing_timestamp': str(datetime.now())
        })
        
        return df
    
    def aggregate_to_gold(self, silver_df, table_name, aggregations=None):
        df = silver_df.copy()
        results = df.drop(columns=['_processed_timestamp'], errors='ignore')
        
        # Apply aggregations
        if aggregations:
            results = aggregations(results)
        
        # Handle single or multiple outputs
        if not isinstance(results, dict):
            results = {table_name: results}
        
        # Save each result
        for name, result_df in results.items():
            table_path = self.gold_path / name
            table_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = table_path / f'{name}_{timestamp}.parquet'
            result_df.to_parquet(file_path, index=False, compression='snappy')
            
            # Save metadata
            self._save_metadata('gold', name, {
                'records': len(result_df),
                'columns': list(result_df.columns),
                'file_path': str(file_path),
                'aggregation_timestamp': str(datetime.now())
            })              
        return results
        
    def read_latest(self, layer, table_name):
        """Read the most recent version of a table"""
        layer_path = getattr(self, f'{layer}_path')
        table_path = layer_path / table_name
        
        if not table_path.exists():
            return None
        
        # Find latest file
        parquet_files = list(table_path.rglob('*.parquet'))
        if not parquet_files:
            return None
        
        latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
        
        return pd.read_parquet(latest_file)