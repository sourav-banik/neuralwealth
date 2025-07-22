from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxTable

class BackTestDataFeed:
    """Simplified data feed for Backtrader that combines market data and fundamentals"""
    
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.org = org
        
    
    def get_asset_data(self, 
                  assets: List[Dict], 
                  fields: Dict[str, List[str]],
                  start_date: str, 
                  end_date: str) -> Dict[str, pd.DataFrame]:
        """
        Get combined data for assets in Backtrader-ready format
        
        Args:
            assets: List of asset dicts with keys [ticker, asset_class, market]
            fields: Dict of {measurement: [field1, field2]} 
            start_date: ISO format start date
            end_date: ISO format end date
            
        Returns:
            Dict of {ticker: pd.DataFrame} with combined data
        """
        results = {}
        
        for asset in assets:
            try:
                # 1. First get all market_info data (required OHLCV + any additional fields)
                market_fields = ['Open', 'High', 'Low', 'Close', 'Volume']
                if 'market_info' in fields:
                    # Add any extra requested market_info fields, avoiding duplicates
                    market_fields.extend(f for f in fields['market_info'] if f not in market_fields)
                
                ohlcv = self._get_market_data(asset, market_fields, start_date, end_date)
                if ohlcv.empty:
                    print(f"No market data found for {asset['ticker']}")
                    continue
                
                # 2. Get all additional data points from other measurements
                all_data = [ohlcv]
                
                for measurement, meas_fields in fields.items():
                    if measurement == "market_info":
                        continue  # Already handled above
                        
                    for field in meas_fields:
                        try:
                            if measurement == "macro_data":
                                data = self._get_macro_data(field, start_date, end_date)
                            elif measurement == "news_sentiment":
                                data = self._get_news_sentiment(asset, field, start_date, end_date)
                            else:
                                data = self._get_asset_data(asset, measurement, field, start_date, end_date)
                            
                            if data is not None and not data.empty:
                                # Convert Series to DataFrame with column name
                                df = data.to_frame()
                                # Rename OHLCV columns to lowercase for Backtrader
                                if field in ['Open', 'High', 'Low', 'Close', 'Volume']:
                                    df.columns = [field.lower()]
                                all_data.append(df)
                        except Exception as e:
                            print(f"Failed to get {field} from {measurement}: {e}")
                            continue
                
                # 3. Combine all data on datetime index
                if len(all_data) > 1:
                    try:
                        # Merge all DataFrames on index
                        merged = pd.concat(all_data, axis=1)
                        # Forward fill fundamental data (like quarterly financials)
                        for col in merged.columns:
                            if col not in ['open', 'high', 'low', 'close', 'volume']:
                                merged[col] = merged[col].ffill()
                        
                        # Ensure we have all required OHLCV columns
                        for req_col in ['open', 'high', 'low', 'close', 'volume']:
                            if req_col not in merged.columns:
                                merged[req_col] = np.nan
                        
                        results[asset['ticker']] = merged
                    except Exception as e:
                        print(f"Failed to merge data for {asset['ticker']}: {e}")
                        results[asset['ticker']] = ohlcv
                else:
                    results[asset['ticker']] = ohlcv
                    
            except Exception as e:
                print(f"Failed to process {asset['ticker']}: {e}")
                continue
            
        return results

    def _get_market_data(self, 
                        asset: Dict, 
                        fields: List[str],
                        start_date: str, 
                        end_date: str) -> pd.DataFrame:
        """Get market data including OHLCV and any additional fields"""
        # Convert field list to Flux filter
        field_filters = ' or '.join([f'r._field == "{f}"' for f in fields])
        
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_date}, stop: {end_date})
            |> filter(fn: (r) => r._measurement == "market_info")
            |> filter(fn: (r) => r.ticker == "{asset['ticker']}")
            |> filter(fn: (r) => r.asset_class == "{asset['asset_class']}")
            |> filter(fn: (r) => r.market == "{asset['market']}")
            |> filter(fn: (r) => {field_filters})
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        tables = self.query_api.query(query, org=self.org)
        df = self._flux_to_dataframe(tables)
        
        # Rename OHLCV columns to lowercase for Backtrader
        if not df.empty:
            column_mapping = {
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
        
        return df

    def _get_asset_data(self, 
                       asset: Dict, 
                       measurement: str, 
                       field: str, 
                       start_date: str, 
                       end_date: str) -> Optional[pd.Series]:
        """Get specific field from asset-specific measurement"""
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_date}, stop: {end_date})
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => r.ticker == "{asset['ticker']}")
            |> filter(fn: (r) => r.asset_class == "{asset['asset_class']}")
            |> filter(fn: (r) => r.market == "{asset['market']}")
            |> filter(fn: (r) => r._field == "{field}")
        '''
        
        tables = self.query_api.query(query, org=self.org)
        df = self._flux_to_dataframe(tables)
        return df[field] if not df.empty else None
    
    def _get_macro_data(self, 
                       field: str, 
                       start_date: str, 
                       end_date: str) -> Optional[pd.Series]:
        """Get macro data (no asset filtering)"""
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_date}, stop: {end_date})
            |> filter(fn: (r) => r._measurement == "macro_data")
            |> filter(fn: (r) => r._field == "{field}")
        '''
        
        tables = self.query_api.query(query, org=self.org)
        df = self._flux_to_dataframe(tables)
        return df[field] if not df.empty else None
    
    def _get_news_sentiment(self, 
                          asset: Dict, 
                          field: str, 
                          start_date: str, 
                          end_date: str) -> Optional[pd.Series]:
        """Get averaged daily news sentiment"""
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_date}, stop: {end_date})
            |> filter(fn: (r) => r._measurement == "news_sentiment")
            |> filter(fn: (r) => r.ticker == "{asset['ticker']}")
            |> filter(fn: (r) => r.asset_class == "{asset['asset_class']}")
            |> filter(fn: (r) => r.market == "{asset['market']}")
            |> filter(fn: (r) => r._field == "{field}")
            |> aggregateWindow(every: 1d, fn: mean)
        '''
        
        tables = self.query_api.query(query, org=self.org)
        df = self._flux_to_dataframe(tables)
        return df[field] if not df.empty else None
    
    def _flux_to_dataframe(self, tables: List[FluxTable]) -> pd.DataFrame:
        """Convert Flux query result to pandas DataFrame, handling both pivoted and long formats"""
        records = []
        
        for table in tables:
            for record in table.records:
                try:
                    # Check if this is already a pivoted record (contains OHLCV columns)
                    if 'Open' in record.values or 'Close' in record.values:
                        # Handle pivoted format
                        time = record.get_time()
                        record_dict = {'time': time}
                        
                        # Add all numeric fields from the record
                        for k, v in record.values.items():
                            if isinstance(v, (int, float)):
                                record_dict[k] = v
                        
                        records.append(record_dict)
                    else:
                        # Handle long format (field-value pairs)
                        field = record.values.get("_field")
                        if field is None:
                            continue
                            
                        records.append((
                            record.get_time(),
                            field,
                            record.get_value()
                        ))
                except Exception as e:
                    print(f"Skipping malformed record: {e}")
                    continue
        
        if not records:
            return pd.DataFrame()
        
        try:
            # If we have dictionary records (pivoted format)
            if isinstance(records[0], dict):
                df = pd.DataFrame(records)
                df = df.set_index('time')
                df.index = pd.to_datetime(df.index)
                return df
            
            # If we have tuple records (long format)
            df = pd.DataFrame(records, columns=['time', 'field', 'value'])
            df = df.pivot(index='time', columns='field', values='value')
            df.index = pd.to_datetime(df.index)
            return df
            
        except Exception as e:
            print(f"Failed to create DataFrame: {e}")
            return pd.DataFrame()