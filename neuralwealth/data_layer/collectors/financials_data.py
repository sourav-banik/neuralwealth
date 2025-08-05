import time
import pandas as pd
import requests, re, json
from bs4 import BeautifulSoup
from typing import List, Dict
import yfinance as yf
from neuralwealth.data_layer.collectors.resources.macrotrends_screener import macrotrends_screener 

class FinancialsCollector:
    """
    Engine that collects financials - income statement, balance sheet, cash flow statement, financial ratios.
    Uses macrotrends.net as primary source for long-term data, otherwise yFinance for short-term data.
    """
    def __init__(self):
        self.yf_client = yf
        self.macrotrends_client = dict(
            headers={"User-Agent": "Mozilla/5.0"},
            base_url="https://www.macrotrends.net",
            screener=macrotrends_screener,
            pages={
                'income_statement': {"id": "income-statement", "period": "A"},
                'balance_sheet': {"id": "balance-sheet", "period": "A"},
                'cashflow_statement': {"id": "cash-flow-statement", "period": "A"},
                'financial_ratios': {"id": "financial-ratios", "period": "A"},
                'quarterly_income_statement': {"id": "income-statement", "period": "Q"},
                'quarterly_balance_sheet': {"id": "balance-sheet", "period": "Q"},
                'quarterly_cashflow_statement': {"id": "cash-flow-statement", "period": "Q"},
                'quarterly_financial_ratios': {"id": "financial-ratios", "period": "Q"},
            }
        )

    def get_financials(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """
        Retrieves financial statements for a given ticker symbol from MacroTrends or Yahoo Finance.

        Args:
            symbol (str): The stock ticker symbol to fetch financials for.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing financial statements as transposed DataFrames.
                Keys may include 'income_statement', 'balance_sheet', 'cash_flow', etc.

        Notes:
            - Checks if the ticker exists in the MacroTrends screener.
            - If found, fetches detailed financials from MacroTrends.
            - If not found, falls back to short-term financials from Yahoo Finance.
        """
        # Search for the ticker in the MacroTrends screener
        macrotrends_symbol = next(
            (ticker for ticker in self.macrotrends_client['screener'] if ticker['symbol'] == symbol.upper()),
            None
        )

        # Fetch financials based on availability in MacroTrends
        if macrotrends_symbol:
            return self.scrape_macrotrends_financials(
                symbol=macrotrends_symbol['symbol'],
                company_name=macrotrends_symbol['name']
            )
        return self.scrape_yfinance_financials(ticker=symbol.upper())

    def scrape_yfinance_financials(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """
        Scrapes financial data for a given stock symbol from yFinance with delays between requests,
        transposing each DataFrame into a time series format with dates as the first unnamed column
        and field names as other columns in lowercase with underscores.
        
        Args:
            symbol (str): Stock ticker symbol (e.g., 'NVDA')
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing financial statements as transposed DataFrames
        """
        def to_snake_case(name: str) -> str:
            """
            Converts a string to snake_case (lowercase with underscores), preserving acronyms.
            
            Args:
                name (str): Input string (e.g., 'Tax Effect Of Unusual Items', 'Normalized EBITDA')
                
            Returns:
                str: Snake case string (e.g., 'tax_effect_of_unusual_items', 'normalized_ebitda')
            """
            name = re.sub(r'(?<!^)(?=[A-Z][a-z])', '_', name)
            name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
            name = name.lower()
            name = re.sub(r'\s+', '_', name)
            name = re.sub(r'_+', '_', name)
            return name.strip('_')

        def transpose_to_timeseries(df: pd.DataFrame) -> pd.DataFrame:
            """
            Transposes a DataFrame to time series format with dates as the first unnamed column
            and original row indices as column headers in lowercase with underscores.
            
            Args:
                df (pd.DataFrame): Input DataFrame with dates as columns and fields as rows
                
            Returns:
                pd.DataFrame: Transposed DataFrame with dates in first unnamed column
            """
            if df.empty:
                return df
            # Rename index (row headers) to snake_case
            df.index = [to_snake_case(str(idx)) for idx in df.index]
            df_transposed = df.transpose()
            df_transposed = df_transposed.reset_index()
            df_transposed.columns = [''] + list(df_transposed.columns[1:])
            df_transposed = df_transposed.sort_values(by='', ascending=True)
            return df_transposed

        try:
            data = self.yf_client.Ticker(symbol)
            result = {}
            result['income_statement'] = transpose_to_timeseries(data.income_stmt)
            time.sleep(5)
            result['balance_sheet'] = transpose_to_timeseries(data.balance_sheet)
            time.sleep(5)
            result['cashflow_statement'] = transpose_to_timeseries(data.cash_flow)
            time.sleep(5)
            result['financial_ratios'] = pd.DataFrame()
            time.sleep(5)
            result['quarterly_income_statement'] = transpose_to_timeseries(data.quarterly_income_stmt)
            time.sleep(5)
            result['quarterly_balance_sheet'] = transpose_to_timeseries(data.quarterly_balance_sheet)
            time.sleep(5)
            result['quarterly_cashflow_statement'] = transpose_to_timeseries(data.quarterly_cash_flow)
            time.sleep(5)
            result['quarterly_financial_ratios'] = pd.DataFrame()
            return result
        except Exception as e:
            raise ValueError(f"yFinance failed for {symbol} financials: {str(e)}")
        
    def scrape_macrotrends_financials(self, symbol: str, company_name: str) -> Dict[str, pd.DataFrame]:
        """
        Args:
            symbol (str): Stock ticker symbol (e.g., 'NVDA')
            company_name (str): Company name for URL construction
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing financial statements as DataFrames
        """
        result = {}
        for page in self.macrotrends_client['pages'].keys():
            result[page] = self._scrape_macrotrends_page(
                symbol, 
                company_name, 
                self.macrotrends_client['pages'][page]['id'], 
                self.macrotrends_client['pages'][page]['period']
            )
            time.sleep(5)
        return result

    def _scrape_macrotrends_page(self, 
        symbol: str, 
        company_name: str, 
        page_type: str, 
        frequency: str
    ) -> pd.DataFrame:
        """
        Scrapes a financial page (e.g., income statement, quarterly) for a ticker.
        
        Args:
            symbol (str): Stock ticker symbol
            company_name (str): Company name for URL construction
            page_type (str): Type of financial page (e.g., income-statement)
            frequency (str): Data frequency (A for annual, Q for quarterly)
            
        Returns:
            pd.DataFrame: DataFrame with financial data
        """
        url = f"{self.macrotrends_client['base_url']}/stocks/charts/{symbol}/{company_name}/{page_type}?freq={frequency}"
        try:
            response = requests.get(url, headers=self.macrotrends_client['headers'])
            soup = BeautifulSoup(response.text, "html.parser")
            for script in soup.find_all("script"):
                results = re.search("var originalData = (.*);", script.text)
                if results is not None:
                    return self._process_macrotrends_raw_data(results[1])
            return pd.DataFrame()
        except Exception as e:
            raise ValueError(f"macrotrends.net failed for {symbol} - {company_name} - {page_type} - {frequency}: {str(e)}")

    def _process_macrotrends_raw_data(self, raw_json: List[Dict]) -> pd.DataFrame:
        """
        Processes financial data JSON into a pandas DataFrame in time series format by:
        1. Extracting the ID from the HTML field_name's href attribute to use as field names
        2. Converting data into time series format with date values as the first unnamed column
        3. Converting numeric strings to appropriate numeric types
        4. Removing empty fields
        
        Args:
            raw_json (List[Dict]): List of dictionaries containing the raw financial data
            
        Returns:
            pd.DataFrame: DataFrame with date values as the first unnamed column and columns for each field ID
        """
        try:
            data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except json.JSONDecodeError:
            raise ValueError("Input must be valid JSON string or list")
        
        dates = set()
        field_data = {}
        for item in data:
            field_name_html = item.get("field_name", "")
            soup = BeautifulSoup(field_name_html, 'html.parser')
            a_tag = soup.find('a')
            if a_tag and 'href' in a_tag.attrs:
                field_id = a_tag['href'].split('/')[-1].replace('-', '_')
            else:
                field_id = soup.get_text().strip().lower().replace(' ', '-')
            field_data[field_id] = {}
            for key, value in item.items():
                if key in ["field_name", "popup_icon"] or value == "":
                    continue
                try:
                    cleaned_value = float(value)
                    if cleaned_value.is_integer():
                        cleaned_value = int(cleaned_value)
                except (ValueError, TypeError):
                    cleaned_value = value
                field_data[field_id][key] = cleaned_value
                dates.add(key)
        
        time_series_data = []
        for date in sorted(dates):
            record = {None: date}
            for field_id in field_data:
                if date in field_data[field_id]:
                    record[field_id] = field_data[field_id][date]
            time_series_data.append(record)
        
        df = pd.DataFrame(time_series_data)
        df.columns = ['' if col is None else col for col in df.columns]
        return df