from typing import List, Dict, Any
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions
import pandas as pd
import json

class InfluxDBStorage:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.write_api = self.client.write_api(
            write_options=WriteOptions(
                batch_size=1000,
                flush_interval=10000,
                jitter_interval=2000,
                retry_interval=5000
            )
        )
        self.query_api = self.client.query_api()
    
    def preprocess_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preprocesses dictionary data to ensure all field values are InfluxDB-compatible.

        Args:
            data (Dict[str, Any]): Input dictionary with field data.

        Returns:
            Dict[str, Any]: Processed dictionary with lists and dictionaries converted to JSON strings.
        """
        processed_data = {}
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                processed_data[key] = json.dumps(value)
            elif isinstance(value, (str, int, float, bool)) or value is None:
                processed_data[key] = value
            else:
                processed_data[key] = str(value)
        return processed_data
    
    def preprocess_dataframe(self, df: pd.DataFrame, time_col: str = None) -> pd.DataFrame:
        """
        Preprocesses a DataFrame to ensure consistent numeric types for InfluxDB.

        Args:
            df (pd.DataFrame): Input DataFrame.
            time_col (str, optional): Column containing timestamps.

        Returns:
            pd.DataFrame: DataFrame with numeric columns converted to float.
        """
        df = df.copy()
        for col in df.columns:
            if col != time_col and df[col].dtype in ['int64', 'float64', 'object']:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                except Exception as e:
                    df[col] = df[col].astype(str)
        return df

    def write_dataframe(self, 
                       df: pd.DataFrame, 
                       measurement: str, 
                       tag_columns: list = None,
                       time_col: str = 'time'):
        """
        Write DataFrame to InfluxDB.
        
        Args:
            df: DataFrame with time index
            measurement: Influx measurement name (like SQL table)
            tag_columns: Columns to index as tags (high-cardinality)
            time_col: Column containing timestamps
        """
        try:
            self.write_api.write(
                bucket=self.bucket,
                record=df,
                data_frame_measurement_name=measurement,
                data_frame_tag_columns=tag_columns or [],
                data_frame_timestamp_column=time_col
            )
            self.write_api.flush()
        except Exception as e:
            raise ValueError(f"Influx write failed: {str(e)}")

    def query_to_dataframe(self, flux_query: str) -> pd.DataFrame:
        """Execute Flux query and return DataFrame."""
        return self.query_api.query_data_frame(flux_query)

    def write_unstructured(self, measurement: str, data: dict, metadata: dict):
        """Store flexible JSON data."""
        try:
            processed_data = self.preprocess_data(data)
            self.write_api.write(
                bucket=self.bucket,
                record=[{
                    "measurement": measurement,
                    "fields": processed_data,
                    "tags": metadata
                }]
            )
            self.write_api.flush()
        except Exception as e:
            raise ValueError(f"Influx write failed: {str(e)}")
        
    def write_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Writes multiple records to InfluxDB in a single batch.
        """
        try:
            for record in records:
                df = pd.DataFrame(record["fields"])
                self.write_api.write(
                    bucket=self.bucket,
                    record=df,
                    data_frame_measurement_name=record["measurement"],
                    data_frame_tag_columns=list(record["tags"].keys()),
                    data_frame_timestamp_column=record["time_col"]
                )
            self.write_api.flush()
        except Exception as e:
            raise ValueError(f"Batch write failed: {str(e)}")
    
    def close(self) -> None:
        """
        Closes the InfluxDB client and flushes pending writes.
        Ensures all write operations are completed before interpreter shutdown.
        """
        try:
            self.write_api.flush()
            self.client.close()
        except Exception as e:
            raise ValueError(f"Influxdb client closure failed: {str(e)}")