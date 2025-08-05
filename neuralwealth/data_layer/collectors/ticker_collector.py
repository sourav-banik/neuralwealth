from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import csv
import re

class settings:
    yahoo_finance_base_url = "https://finance.yahoo.com"
    yahoo_finance_header = { 
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" 
    }

class TickerCollector:
    """
    Engine that collects ticker symbols from various markets and asset classes.
    """
    def __init__(self):
        """
        Initializes the TickerCollector with no arguments.
        """
        pass

    def collect_tickers(self) -> List[Dict]:
        """
        Scrapes ticker symbols for various markets (e.g., NASDAQ, NYSE) and asset classes from sources.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for each symbol.
        """
        tickers = []
        try:
            tickers.extend(self._scrape_nasdaq_stocks())
            tickers.extend(self._scrape_nyse_stocks())
            tickers.extend(self._scrape_indices())
            tickers.extend(self._scrape_currencies())
            tickers.extend(self._scrape_commodities())
            tickers.extend(self._scrape_cryptos())
            tickers.extend(self._scrape_us_treasury())
            tickers.extend(self._scrape_private_companies())
            return tickers
        except Exception as e:
            raise ValueError(f"Error scraping tickers: {str(e)}")

    def _scrape_nasdaq_stocks(self) -> List[Dict]:
        """
        Scrapes common stock tickers listed on NASDAQ from nasdaqtrader.com.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for NASDAQ stocks.
        """
        tickers = []
        url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
        response = requests.get(url)
        response.raise_for_status()
        content = response.text.split('\n')[:-1]
        reader = csv.DictReader(content, delimiter='|')
        stock_patterns = [
            re.compile(r"common stock", re.IGNORECASE),
            re.compile(r"ordinary shares", re.IGNORECASE),
            re.compile(r"american depositary shares", re.IGNORECASE)
        ]
        for row in reader:
            if (row['Test Issue'] == 'N' and
                row['Financial Status'] == 'N' and
                row['ETF'] == 'N' and
                row['NextShares'] == 'N' and
                any(pattern.search(row['Security Name']) for pattern in stock_patterns)):
                tickers.append({
                    "ticker": row['Symbol'],
                    "asset_class": "stock",
                    "market": "NASDAQ"
                })
        return tickers

    def _scrape_nyse_stocks(self) -> List[Dict]:
        """
        Scrapes common stock tickers listed on NYSE from nasdaqtrader.com's otherlisted.txt.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for NYSE stocks.
        """
        tickers = []
        url = "https://nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
        response = requests.get(url)
        response.raise_for_status()
        content = response.text.split('\n')[:-1]
        fieldnames = [
            'ACT_Symbol', 'Security_Name', 'Exchange', 'CQS_Symbol',
            'ETF', 'Round_Lot_Size', 'Test_Issue', 'NASDAQ_Symbol'
        ]
        reader = csv.DictReader(content, fieldnames=fieldnames, delimiter='|')
        stock_patterns = [
            re.compile(r"common stock", re.IGNORECASE),
            re.compile(r"ordinary shares", re.IGNORECASE),
            re.compile(r"american depositary shares", re.IGNORECASE),
            re.compile(r"inc$", re.IGNORECASE),
            re.compile(r"ltd$", re.IGNORECASE),
            re.compile(r"corp$", re.IGNORECASE)
        ]
        exclude_patterns = [
            re.compile(r"preferred", re.IGNORECASE),
            re.compile(r"depositary receipts", re.IGNORECASE),
            re.compile(r"warrant", re.IGNORECASE),
            re.compile(r"etn", re.IGNORECASE),
            re.compile(r"note", re.IGNORECASE),
            re.compile(r"bond", re.IGNORECASE),
            re.compile(r"trust", re.IGNORECASE)
        ]
        for row in reader:
            if (row['Exchange'] == 'N' and
                row['Test_Issue'] == 'N' and
                row['ETF'] == 'N' and
                any(pattern.search(row['Security_Name']) for pattern in stock_patterns) and
                not any(exclude.search(row['Security_Name']) for exclude in exclude_patterns)):
                tickers.append({
                    "ticker": row['ACT_Symbol'],
                    "asset_class": "stock",
                    "market": "NYSE"
                })
        return tickers

    def _scrape_indices(self) -> List[Dict]:
        """
        Scrapes index tickers from Yahoo Finance world indices page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for indices.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/world-indices/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        indices_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            indices_data.append({
                "ticker": symbol[1:],
                "asset_class": "index",
                "market": "GLOBAL"
            })
        return indices_data

    def _scrape_currencies(self) -> List[Dict]:
        """
        Scrapes currency pair tickers from Yahoo Finance currencies page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for currencies.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/currencies/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        currencies_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            currencies_data.append({
                "ticker": symbol[:-2],
                "asset_class": "forex",
                "market": "GLOBAL"
            })
        return currencies_data

    def _scrape_commodities(self) -> List[Dict]:
        """
        Scrapes commodity futures tickers from Yahoo Finance commodities page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for commodities.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/commodities/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        commodities_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            commodities_data.append({
                "ticker": symbol[:-2],
                "asset_class": "future",
                "market": "GLOBAL"
            })
        return commodities_data

    def _scrape_cryptos(self) -> List[Dict]:
        """
        Scrapes cryptocurrency tickers from Yahoo Finance most active cryptocurrencies page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for cryptocurrencies.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/crypto/most-active/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        cryptos_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            cryptos_data.append({
                "ticker": symbol.split('-')[0],
                "asset_class": "crypto",
                "market": "GLOBAL"
            })
        return cryptos_data

    def _scrape_us_treasury(self) -> List[Dict]:
        """
        Scrapes US treasury bond tickers from Yahoo Finance bonds page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for US treasury bonds.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/bonds/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        treasury_bonds_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            if symbol.startswith('^'):
                treasury_bonds_data.append({
                    "ticker": symbol[1:],
                    "asset_class": "treasury",
                    "market": "US"
                })
        return treasury_bonds_data

    def _scrape_private_companies(self) -> List[Dict]:
        """
        Scrapes private company tickers from Yahoo Finance highest-valuation private companies page.

        Returns:
            List of dictionaries containing ticker, asset_class, and market for private companies.
        """
        url = f"{settings.yahoo_finance_base_url}/markets/private-companies/highest-valuation/"
        response = requests.get(url, headers=settings.yahoo_finance_header)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.row")
        private_companies_data = []
        for row in rows:
            symbol = row.select_one('span.symbol').text.strip()
            private_companies_data.append({
                "ticker": symbol.split('.')[0],
                "asset_class": "private",
                "market": "GLOBAL"
            })
        return private_companies_data