import argparse
from influxdb_client import InfluxDBClient
from influxdb_client.domain.ready import Ready
from typing import Dict, List
import pandas as pd

class InfluxDBSchemaGenerator:
    def __init__(self, url: str, token: str, org: str):
        """
        Initialize the InfluxDB Schema Generator.
        
        :param url: InfluxDB server URL
        :param token: Authentication token
        :param org: Organization name
        """
        self.url = url.rstrip('/')  # Remove trailing slash if present
        self.token = token
        self.org = org
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=30_000)
        self.query_api = self.client.query_api()
        
    def check_connection(self) -> bool:
        """
        Check if the connection to InfluxDB is successful.
        
        :return: True if connection is successful, False otherwise
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
        
        :return: List of bucket dictionaries
        """
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        return [{"name": b.name, "id": b.id, "retention": b.retention_rules} for b in buckets]
    
    def get_measurements(self, bucket_name: str) -> List[str]:
        """
        Get list of measurements in a bucket.
        
        :param bucket_name: Name of the bucket
        :return: List of measurement names
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
        
        :param bucket_name: Name of the bucket
        :param measurement: Name of the measurement
        :return: Dictionary of field names and their types
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
        """Fallback method by sampling actual data"""
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
        """Alternative using InfluxQL syntax"""
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
        """Infer the type of a field value"""
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
        
        :param bucket_name: Name of the bucket
        :param measurement: Name of the measurement
        :return: List of tag keys
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
        """Fallback method by sampling actual data for tags"""
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
        """Alternative using InfluxQL syntax"""
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
        
        :param bucket_name: Name of the bucket
        :param measurement: Name of the measurement
        :return: String representing the time range (YYYY-MM-DD to YYYY-MM-DD) or 'No data'
        """
        try:
            # Query earliest timestamp
            earliest_query = f'''
            from(bucket: "{bucket_name}")
                |> range(start: -100y, stop: now())
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> keep(columns: ["_time"])
                |> sort(columns: ["_time"])
                |> limit(n: 1)
            '''
            earliest_result = self.query_api.query(query=earliest_query, org=self.org)
            
            # Query latest timestamp
            latest_query = f'''
            from(bucket: "{bucket_name}")
                |> range(start: -100y, stop: 4d)
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
            return "No data"
        except Exception as e:
            print(f"Warning: Failed to get time range for {measurement}: {str(e)}")
            return "No data"
    
    def generate_schema(self, bucket_name: str) -> Dict:
        """
        Generate full schema for a bucket with robust field detection and time ranges.
        
        :param bucket_name: Name of the bucket
        :return: Dictionary containing the full schema
        """
        schema = {
            "bucket": bucket_name,
            "organization": self.org,
            "measurements": {},
            "generated_at": pd.Timestamp.now().isoformat()
        }
        
        measurements = self.get_measurements(bucket_name)
        
        for measurement in measurements:
            schema["measurements"][measurement] = {
                "fields": self.get_fields_and_types(bucket_name, measurement),
                "tags": self.get_tags(bucket_name, measurement),
                "time_range": self.get_time_range(bucket_name, measurement)
            }
        
        return schema
    
    def generate_markdown(self, schema: Dict) -> str:
        """
        Generate Markdown representation of the schema in YAML-like format.
        
        :param schema: Schema dictionary
        :return: Markdown formatted string
        """
        markdown = f"# InfluxDB Schema v2.4\nbucket: {schema['bucket']}\n\n## Measurements Hierarchy\nmeasurements:\n"
        for measurement, details in schema["measurements"].items():
            # Format fields as a bullet list
            fields_str = "      - None" if not details["fields"] else "\n" + "\n".join([f"      - {field}: {type_}" for field, type_ in details["fields"].items()])
            # Format tags as a YAML array
            tags_str = f"[{', '.join(details['tags'])}]" if details["tags"] else "[]"
            time_range = details["time_range"]
            
            markdown += f"  - name: {measurement}\n"
            markdown += f"    time_range: {time_range}\n"
            markdown += f"    tags: {tags_str}\n"
            markdown += f"    fields:{fields_str}\n\n"
        
        return markdown
    
    def save_schema_to_file(self, markdown: str, filename: str) -> None:
        """
        Save schema to a Markdown file with UTF-8 encoding.
        
        :param markdown: Markdown formatted schema
        :param filename: Output filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(markdown)
            print(f"Schema saved to {filename}")
        except Exception as e:
            print(f"Error saving schema to {filename}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Generate a full schema for an InfluxDB bucket in Markdown format")
    parser.add_argument("--url", required=True, help="InfluxDB server URL")
    parser.add_argument("--token", required=True, help="Authentication token")
    parser.add_argument("--org", required=True, help="Organization name")
    parser.add_argument("--bucket", required=True, help="Bucket name to generate schema for")
    parser.add_argument("--output", default="neuralwealth/influxdb_schema.md", help="Output Markdown file name")
    
    args = parser.parse_args()
    
    generator = InfluxDBSchemaGenerator(args.url, args.token, args.org)
    
    if not generator.check_connection():
        print("Failed to connect to InfluxDB. Please check your credentials and connection.")
        return
    
    print(f"Generating schema for bucket: {args.bucket}")
    schema = generator.generate_schema(args.bucket)
    markdown = generator.generate_markdown(schema)
    generator.save_schema_to_file(markdown, args.output)

if __name__ == "__main__":
    main()

##python C:\Users\MyPC\myprojects\neuralwealth\scripts\generate_influx_schema.py --url http://localhost:8086 --token FMfR3almj9B_9tlubDIaJbGrx3mR91McfNRKnd4Cg8R_8CIDSgYYrgFEIjRWzii-ZLLYEITdgchboPpIPH4uCQ== --org neuralwealth --bucket neuralwealth