import backtrader as bt

class DynamicDataFeed:
    """A class to create dynamic data feeds from pandas DataFrames for Backtrader.

    This class generates a Backtrader PandasData feed that includes standard OHLCV
    (Open, High, Low, Close, Volume) columns and dynamically incorporates additional
    columns from the input DataFrame as custom lines.
    """

    def __init__(self, dataframe, name=None):
        """Initialize the DynamicDataFeed with a DataFrame and optional name.

        Args:
            dataframe (pandas.DataFrame): The input DataFrame containing financial data.
            name (str, optional): Name for the data feed. Defaults to None.
        """
        self.dataframe = dataframe
        self.name = name
        self.base_cols = {
            'datetime': None,
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'openinterest': None
        }

    def get_extra_columns(self):
        """Identify non-standard columns in the DataFrame for use as custom lines.

        Returns:
            list: A list of column names not included in the standard OHLCV columns.
        """
        return [
            col for col in self.dataframe.columns
            if col.lower() not in [k for k, v in self.base_cols.items() if v]
        ]

    def create_dynamic_class(self):
        """Create a dynamic Backtrader PandasData class with custom lines.

        Returns:
            type: A dynamically created subclass of `bt.feeds.PandasData` with
                  additional lines for non-standard DataFrame columns.
        """
        extra_cols = self.get_extra_columns()

        class DynamicPandasData(bt.feeds.PandasData):
            lines = tuple(extra_cols)
            params = tuple((col, -1) for col in extra_cols)

        return DynamicPandasData

    def create_feed(self):
        """Create and return the dynamic Backtrader data feed.

        Returns:
            bt.feeds.PandasData: A configured Backtrader data feed instance.
        """
        params = {k: v for k, v in self.base_cols.items()}
        params.update({'name': self.name})
        dynamic_class = self.create_dynamic_class()
        return dynamic_class(dataname=self.dataframe, **params)