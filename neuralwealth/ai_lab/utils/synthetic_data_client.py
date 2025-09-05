import pandas as pd
from typing import List, Dict, Optional, Any
from influxdb_client import InfluxDBClient

class SyntheticDataClient:
    """Client to retrieve synthetic crash scenarios from InfluxDB"""
    
    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str
    ):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.org = org
        
    def get_synthetic_scenarios(
        self,
        assets: List[Dict],
        scenario_types: Optional[List[str]] = None,
        lookback_days: int = 365
    ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieve synthetic scenarios for a list of assets.

        Args:
            assets: List of dictionaries, each with keys [ticker, asset_class, market] to get scenarios for.
            scenario_types: List of scenario types to retrieve (None for all).
            lookback_days: How many days of synthetic data to retrieve.

        Returns:
            Dictionary of {scenario_type: {ticker: DataFrame}}, where each inner dictionary maps asset tickers
            to their synthetic scenario data as a DataFrame.
        """
        if scenario_types is None:
            scenario_types = ["findiff_2008", "findiff_2020", "findiff_flash", "stress_scenarios"]
        
        scenarios = {}
        fields = ['open', 'high', 'low', 'close', 'volume']
        field_filters = ' or '.join([f'r._field == "{f}"' for f in fields])
        
        for scenario_type in scenario_types:
            # Initialize inner dictionary for this scenario type
            scenarios[scenario_type] = {}
            
            for asset in assets:
                query = f'''
                from(bucket:"{self.bucket}")
                  |> range(start: -{lookback_days}d)
                  |> filter(fn: (r) => r._measurement == "findiff_scenarios")
                  |> filter(fn: (r) => r.ticker == "{asset['ticker']}")
                  |> filter(fn: (r) => r.asset_class == "{asset['asset_class']}")
                  |> filter(fn: (r) => r.market == "{asset['market']}")
                  |> filter(fn: (r) => r.scenario_type == "{scenario_type}")
                  |> filter(fn: (r) => {field_filters})
                  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                '''
                
                try:
                    result = self.client.query_api().query_data_frame(query)
                    if not result.empty:
                        # Clean up the DataFrame
                        result = result.drop(columns=['result', 'table', '_start', '_stop', '_measurement', 
                                                    'asset_class', 'market', 'scenario_type', 'ticker'], errors='ignore')
                        result = result.rename(columns={'_time': 'time'})
                        result.set_index('time', inplace=True)
                        # Store the DataFrame under the asset's ticker
                        scenarios[scenario_type][asset['ticker']] = result
                except Exception as e:
                    print(f"Warning: Could not retrieve {scenario_type} for {asset['ticker']}: {e}")
            
            if not scenarios[scenario_type]:
                del scenarios[scenario_type]
        
        return scenarios