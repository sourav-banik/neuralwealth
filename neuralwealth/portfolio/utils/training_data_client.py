import pandas as pd
import dask.dataframe as dd
from typing import List, Dict, Union
from influxdb_client import InfluxDBClient

class RLTrainingDataClient:
    """High-performance InfluxDB client for RL training with schema-based measurements."""
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        
        self.measurement_fields = {
            "market_info": ["open", "high", "low", "close", "volume", "rsi_14", "macd", "vwap", "ema_20", "sma_20"],
            "basic_info": ["sector", "industry", "currency", "marketCap", "currentPrice", "beta"],
            "financial_ratios": ["roe", "current_ratio", "debt_equity_ratio", "gross_margin", "roa"],
            "macro_data": ["fed_funds_rate", "cpi_inflation", "vix_volatility_index", "sp500_index"],
            "balance_sheet": ["total_assets", "total_liabilities", "total_share_holder_equity"],
            "income_statement": ["revenue", "net_income", "ebitda"],
            "cashflow_statement": ["net_cash_flow", "operating_cash_flow"],
            "quarterly_balance_sheet": ["total_assets", "total_liabilities", "total_share_holder_equity"],
            "quarterly_income_statement": ["revenue", "net_income", "ebitda"],
            "quarterly_cashflow_statement": ["net_cash_flow", "operating_cash_flow"],
            "quarterly_financial_ratios": ["roe", "current_ratio", "debt_equity_ratio"],
            "news_sentiment": ["score"]
        }

    def _build_asset_filter(self, assets: List[Dict], measurement: str) -> str:
        if measurement == "macro_data":
            return 'true'
            
        filters = []
        for asset in assets:
            if not all(k in asset for k in ["ticker", "asset_class", "market"]):
                raise ValueError(f"Asset missing required fields: {asset}")
                
            filters.append(f'(r["ticker"] == "{asset["ticker"]}" and r["asset_class"] == "{asset["asset_class"]}" and r["market"] == "{asset["market"]}")')
        return " or ".join(filters) if filters else 'false'

    def _quantize_data(self, df: Union[pd.DataFrame, dd.DataFrame]) -> dd.DataFrame:
        if isinstance(df, pd.DataFrame):
            ddf = dd.from_pandas(df, chunksize=250_000)
        else:
            ddf = df
            
        for col in ddf.columns:
            if ddf[col].dtype == 'float64':
                ddf[col] = ddf[col].astype('float32')
            elif ddf[col].dtype == 'int64':
                ddf[col] = ddf[col].astype('int32')
            elif ddf[col].dtype == 'object':
                try:
                    ddf[col] = ddf[col].astype('float32')
                except (ValueError, TypeError):
                    pass
        return ddf

    def _fetch_measurement_data(self, measurement: str, assets: List[Dict], start: str, end: str) -> dd.DataFrame:
        asset_filter = self._build_asset_filter(assets, measurement)
        fields = self.measurement_fields.get(measurement, [])
        
        if not fields:
            return dd.from_pandas(pd.DataFrame(), npartitions=1)

        if measurement == "macro_data":
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: {start}, stop: {end})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["_field"] =~ /^({"|".join(fields)})$/)
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''
        else:
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: {start}, stop: {end})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => {asset_filter})
                |> filter(fn: (r) => r["_field"] =~ /^({"|".join(fields)})$/)
                |> pivot(rowKey:["_time"], columnKey: ["ticker", "asset_class", "market", "_field"], valueColumn: "_value")
            '''
        
        try:
            df = self.query_api.query_data_frame(query)
            
            if len(df.columns) == 0 or '_time' not in df.columns:
                return dd.from_pandas(pd.DataFrame(), npartitions=1)
                
            df = df.set_index('_time')
            df = df.drop(columns=['result', 'table', '_start', '_stop', '_measurement', '_unstructured'], errors='ignore')
            return dd.from_pandas(df, npartitions=1)
            
        except Exception as e:
            print(f"Error querying {measurement}: {str(e)}")
            return dd.from_pandas(pd.DataFrame(), npartitions=1)

    def get_feature_matrix(self, assets: List[Dict], start: str, end: str) -> dd.DataFrame:
        if not assets:
            return dd.from_pandas(pd.DataFrame(), npartitions=1)
            
        measurements = list(self.measurement_fields.keys())
        dfs = {meas: self._fetch_measurement_data(meas, assets, start, end) for meas in measurements}
        
        processed_dfs = []
        for meas, df in dfs.items():
            if len(df.columns) == 0:
                continue
                
            pdf = df.compute() if isinstance(df, dd.DataFrame) else df
            
            if meas == "macro_data":
                pdf.columns = pd.MultiIndex.from_tuples(
                    [('macro', meas, col) for col in pdf.columns],
                    names=['asset_id', 'measurement', 'field']
                )
            else:
                new_columns = []
                for col in pdf.columns:
                    parts = col.split('_')
                    if len(parts) >= 4:
                        asset_id = f"{parts[0]}_{parts[1]}_{parts[2]}"
                        field = '_'.join(parts[3:])
                        new_columns.append((asset_id, meas, field))
                    else:
                        new_columns.append(('unknown', meas, col))
                
                pdf.columns = pd.MultiIndex.from_tuples(new_columns, names=['asset_id', 'measurement', 'field'])
            
            processed_dfs.append(pdf)
        
        if not processed_dfs:
            return dd.from_pandas(pd.DataFrame(), npartitions=1)
            
        full_df = pd.concat(processed_dfs, axis=1)
        return dd.from_pandas(full_df, npartitions=1)

    def get_training_window(self, assets: List[Dict], window_days: int = 30) -> dd.DataFrame:
        end = pd.Timestamp.now().strftime('%Y-%m-%d')
        start = (pd.Timestamp.now() - pd.Timedelta(days=window_days)).strftime('%Y-%m-%d')
        return self.get_feature_matrix(assets, start, end)
    
    def close(self):
        self.client.close()
