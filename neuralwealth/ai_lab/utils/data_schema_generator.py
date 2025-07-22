from influxdb_client import InfluxDBClient
from influxdb_client.domain.ready import Ready
from typing import Dict, List

class DataSchemaGenerator:
    def __init__(self, url: str, token: str, org: str):
        """
        Initialize the InfluxDB Schema Generator.
        
        Args:
            url: InfluxDB server URL
            token: Authentication token
            org: Organization name
        """
        self.url = url.rstrip('/')  # Remove trailing slash if present
        self.token = token
        self.org = org
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=30_000)
        self.query_api = self.client.query_api()

    def check_connection(self) -> bool:
        """
        Check if the connection to InfluxDB is successful.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            ready: Ready = self.client.ready()
            return ready.status == "ready"
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def get_buckets(self) -> List[Dict]:
        """
        Get list of all buckets in the organization.
        
        Returns:
            List[Dict]: List of bucket dictionaries with name, id and retention rules
        """
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        return [{"name": b.name, "id": b.id, "retention": b.retention_rules} for b in buckets]

    def get_measurements(self, bucket_name: str) -> List[str]:
        """
        Get list of measurements in a bucket.
        
        Args:
            bucket_name: Name of the bucket to query
            
        Returns:
            List[str]: List of measurement names
        """
        try:
            query = f'''
            import "influxdata/influxdb/schema"
            schema.measurements(bucket: "{bucket_name}")
            '''
            result = self.query_api.query(query=query, org=self.org)
            return [record.get_value() for table in result for record in table.records]
        except Exception as e:
            print(f"Warning: Failed to get measurements for bucket {bucket_name}: {str(e)}")
            return []

    def get_fields_and_types(self, bucket_name: str, measurement: str) -> Dict[str, str]:
        """
        Get fields and their types for a measurement with multiple fallback methods.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            Dict[str, str]: Dictionary mapping field names to their types
        """
        fields = {}

        if not fields:
            # If no fields found, try sampling actual data
            fields = self._sample_data_for_fields(bucket_name, measurement)

        if not fields:
            # If still no fields, try InfluxQL approach
            fields = self._try_influxql_field_keys(bucket_name, measurement)

        return fields or {}  # Return empty dict if no fields found

    def _sample_data_for_fields(self, bucket_name: str, measurement: str) -> Dict[str, str]:
        """
        Fallback method to determine fields by sampling actual data.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            Dict[str, str]: Dictionary mapping field names to their inferred types
        """
        try:
            # First get all field names
            query = f'''
            from(bucket: "{bucket_name}")
              |> range(start: -100y, stop: 4d)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> keep(columns: ["_field"])
              |> distinct()
            '''

            result = self.query_api.query(query=query, org=self.org)

            fields = {}
            for table in result:
                for record in table.records:
                    field_name = record.get_field()
                    if field_name:
                        # Get type information by sampling one value
                        type_query = f'''
                        from(bucket: "{bucket_name}")
                          |> range(start: -100y, stop: 4d)
                          |> filter(fn: (r) => r._measurement == "{measurement}")
                          |> filter(fn: (r) => r._field == "{field_name}")
                          |> limit(n: 1)
                        '''
                        type_result = self.query_api.query(query=type_query, org=self.org)
                        for type_table in type_result:
                            for type_record in type_table.records:
                                fields[field_name] = self._infer_type(type_record.get_value())
                                break
            return fields
        except Exception as e:
            print(f"Warning: field sampling failed for {measurement}: {str(e)}")
            return {}

    def _try_influxql_field_keys(self, bucket_name: str, measurement: str) -> Dict[str, str]:
        """
        Alternative field detection using InfluxQL syntax.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            Dict[str, str]: Dictionary mapping field names to their types
        """
        try:
            query = f'SHOW FIELD KEYS FROM "{measurement}"'
            result = self.query_api.query(query=query, org=self.org)
            return {
                record.values["fieldKey"]: record.values["fieldType"]
                for table in result
                for record in table.records
            }
        except Exception as e:
            print(f"Warning: InfluxQL field detection failed for {measurement}: {str(e)}")
            return {}

    def _infer_type(self, value) -> str:
        """
        Infer the type of a field value.
        
        Args:
            value: The value to infer type from
            
        Returns:
            str: Inferred type ('number', 'boolean', 'string', or 'unknown')
        """
        if isinstance(value, float) or isinstance(value, int):
            return "number"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, str):
            return "string"
        return "unknown"

    def get_tags(self, bucket_name: str, measurement: str) -> List[str]:
        """
        Get tag keys for a measurement with multiple fallback methods.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            List[str]: List of tag keys
        """
        # Try standard schema approach first
        tags = []

        if not tags:
            # If no tags found, try sampling actual data
            tags = self._sample_data_for_tags(bucket_name, measurement)

        if not tags:
            # If still no tags, try InfluxQL approach
            tags = self._try_influxql_tag_keys(bucket_name, measurement)

        return tags or []  # Return empty list if no tags found

    def _sample_data_for_tags(self, bucket_name: str, measurement: str) -> List[str]:
        """
        Fallback method to determine tags by sampling actual data.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            List[str]: List of tag keys found in the data
        """
        try:
            # First get all tag names
            query = f'''
            from(bucket: "{bucket_name}")
              |> range(start: -10y, stop: 4d)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> keys()
              |> keep(columns: ["_value"])
              |> distinct()
              |> filter(fn: (r) => not r._value =~ /^_/)
            '''
            result = self.query_api.query(query=query, org=self.org)

            # Get all possible keys
            all_keys = [record.get_value() for table in result for record in table.records]

            return all_keys
        except Exception as e:
            print(f"Warning: tag sampling failed for {measurement}: {str(e)}")
            return []

    def _try_influxql_tag_keys(self, bucket_name: str, measurement: str) -> List[str]:
        """
        Alternative tag detection using InfluxQL syntax.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            List[str]: List of tag keys
        """
        try:
            query = f'SHOW TAG KEYS FROM "{measurement}"'
            result = self.query_api.query(query=query, org=self.org)
            return [record.values["tagKey"] for table in result for record in table.records]
        except Exception as e:
            print(f"Warning: InfluxQL tag detection failed for {measurement}: {str(e)}")
            return []

    def get_time_range(self, bucket_name: str, measurement: str) -> str:
        """
        Get the time range of data available for a measurement.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            
        Returns:
            str: Time range string (YYYY-MM-DD to YYYY-MM-DD) or 
                 'non timeseries unstructured data, each row unique' or 
                 'No data' if no data found
        """
        try:
            # First check if this is likely time series data
            is_timeseries = self._is_time_series_measurement(bucket_name, measurement)

            if not is_timeseries:
                return "non timeseries unstructured data, each row unique"

            # Proceed with time range query for time series data
            earliest_query = f'''
            from(bucket: "{bucket_name}")
                |> range(start: -100y, stop: now())
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> keep(columns: ["_time"])
                |> sort(columns: ["_time"])
                |> limit(n: 1)
            '''
            earliest_result = self.query_api.query(query=earliest_query, org=self.org)

            latest_query = f'''
            from(bucket: "{bucket_name}")
                |> range(start: -100y, stop: now())
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> keep(columns: ["_time"])
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: 1)
            '''
            latest_result = self.query_api.query(query=latest_query, org=self.org)

            earliest_time = earliest_result[0].records[0].values["_time"].strftime("%Y-%m-%d") if earliest_result and earliest_result[0].records else None
            latest_time = latest_result[0].records[0].values["_time"].strftime("%Y-%m-%d") if latest_result and latest_result[0].records else None

            if earliest_time and latest_time:
                return f"{earliest_time} to {latest_time}"
            return "Unstructured Data, Point in Time Data"
        except Exception as e:
            print(f"Warning: Failed to get time range for {measurement}: {str(e)}")
            return "No data"
    def _is_time_series_measurement(self, bucket_name: str, measurement: str, sample_size: int = 10) -> bool:
        """
        Simplified detection that relies on the _unstructured tag to identify non-time-series data.
        
        Args:
            bucket_name: Name of the bucket
            measurement: Name of the measurement
            sample_size: Number of records to check for the tag (default: 10)
            
        Returns:
            bool: True if data appears to be time series (no unstructured tag found), 
                False if unstructured tag is present
        """
        try:
            # Check for presence of the unstructured tag in a sample of records
            query = f'''
            from(bucket: "{bucket_name}")
            |> range(start: -100y, stop: now())
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => exists r._unstructured)
            |> limit(n: {sample_size})
            '''
            result = self.query_api.query(query=query, org=self.org)
            
            # If we find any records with the unstructured tag, it's not time series
            if result and len(result) > 0 and len(result[0].records) > 0:
                return False
                
            # Default to time series if no unstructured tag is found
            return True
            
        except Exception as e:
            print(f"Warning: Failed to check measurement type for {measurement}: {str(e)}")
            return True  # Default to time series if there's an error
    
    def generate_schema(self, bucket_name: str) -> Dict:
        """
        Generate full schema for a bucket including measurements, fields, tags and time ranges.
        
        Args:
            bucket_name: Name of the bucket to analyze
            
        Returns:
            Dict: Complete schema dictionary containing:
                - bucket name
                - organization
                - measurements with their fields, tags and time ranges
        """
        schema = {
            "bucket": bucket_name,
            "organization": self.org,
            "measurements": {}
        }

        measurements = self.get_measurements(bucket_name)

        for measurement in measurements:
            schema["measurements"][measurement] = {
                "fields": self.get_fields_and_types(bucket_name, measurement),
                "tags": self.get_tags(bucket_name, measurement),
                "time_range": self.get_time_range(bucket_name, measurement)
            }

        return schema