import pandas as pd
from fredapi import Fred
from neuralwealth.data_layer.collectors.resources.fred_series import fred_series
from typing import Dict

class FREDCollector:
    """
    Engine that collects macro economic data series from FRED
    - get timeseries by id
    - get 100 predefined macroeconomic indicator that are relevant to financial research
    """
    def __init__(self, api_key: str):
        """
        Sets the api key.

        Args:
            api_key: fred api key
        """
        self.client = Fred(api_key=api_key)
        # FRED Series IDs for critical indicators
        self.series_ids = fred_series

    def fetch_series(self, series_id: str, **kwargs) -> pd.DataFrame:
        """
        Fetch time series data from FRED.

        Args:
            series_id: FRED series id
            **kwargs: Passed to get series timeseries

        Returns:
            A dataframe containing time series of the indicator.
        """
        try:
            data = self.client.get_series(series_id, **kwargs)
            return pd.DataFrame({series_id: data})
        except Exception as e:
            raise ValueError(f"FRED API failed for {series_id}: {str(e)}")

    def fetch_all(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch all predefined macroeconomic indicators.

        Returns:
            List of dict containing indicator name and associated timeframe for that indicator. 
        """
        try:
            macro_data = {}
            for series in self.series_ids:
                macro_data[series['name']] = self.fetch_series(series['id'])
            return macro_data
        except Exception as e:
            raise ValueError(f"FRED API failed: {str(e)}")