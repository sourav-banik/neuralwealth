from typing import Dict, List
from influxdb_client import InfluxDBClient

class InfluxDBQuery:
    """Queries market data from InfluxDB."""
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        """
        Initialize InfluxDBQuery client.

        Args:
            url: InfluxDB URL.
            token: Authentication token.
            org: Organization name.
            bucket: Data bucket name.
        """
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket

    def get_asset_data(self, assets: List[str], fields: List[str], start_date: str, end_date: str) -> Dict:
        """
        Fetch market data for specified assets and fields.

        Args:
            assets: List of asset tickers.
            fields: List of data fields (e.g., close, rsi_14, volume, esg_score).
            start_date: Start date (e.g., "2023-01-01").
            end_date: End date (e.g., "2025-07-31").

        Returns:
            Dict: Market data for each asset and field.
        """
        try:
            query_api = self.client.query_api()
            data = {}
            for asset in assets:
                data[asset] = {}
                for field in fields:
                    query = f'''
                        from(bucket: "{self.bucket}")
                        |> range(start: {start_date}, stop: {end_date})
                        |> filter(fn: (r) => r["_measurement"] == "{asset}")
                        |> filter(fn: (r) => r["_field"] == "{field}")
                        |> yield()
                    '''
                    result = query_api.query(query)
                    values = [record["_value"] for table in result for record in table.records]
                    data[asset][field] = values[-1] if values else 0.0
            return data
        except Exception as e:
            return {}