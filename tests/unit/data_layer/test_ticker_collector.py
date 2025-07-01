import time
from neuralwealth.data_layer.collectors.ticker_collector import TickerCollector, settings

def test_collect_tickers_success():
    """Test collect_tickers returns a non-empty list of tickers from all sources."""
    collector = TickerCollector()
    tickers = collector.collect_tickers()
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(set(t.keys()) == {"ticker", "asset_class", "market"} for t in tickers), "Invalid dictionary keys"
    # Check for known tickers (may vary, so we test a few likely ones)
    assert any(t["ticker"] == "NVDA" and t["market"] == "NASDAQ" for t in tickers) or \
           any(t["ticker"] == "JPM" and t["market"] == "NYSE" for t in tickers), \
           "Expected at least one known ticker (NVDA or JPM)"

def test_collect_tickers_error():
    """Test collect_tickers raises ValueError on invalid URL."""
    original_url = settings.yahoo_finance_base_url
    settings.yahoo_finance_base_url = "https://invalid-url.example.com"  # Invalid URL
    collector = TickerCollector()
    try:
        collector.collect_tickers()
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "Error scraping tickers" in str(e), "Unexpected error message"
    finally:
        settings.yahoo_finance_base_url = original_url  # Restore original URL

def test_scrape_nasdaq_stocks_success():
    """Test _scrape_nasdaq_stocks returns valid NASDAQ stock tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_nasdaq_stocks()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "stock" and t["market"] == "NASDAQ" for t in tickers), "Invalid asset_class or market"
    assert any(t["ticker"] == "NVDA" for t in tickers), "Expected NVDA in NASDAQ tickers"

def test_scrape_nyse_stocks_success():
    """Test _scrape_nyse_stocks returns valid NYSE stock tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_nyse_stocks()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "stock" and t["market"] == "NYSE" for t in tickers), "Invalid asset_class or market"
    assert any(t["ticker"] == "JPM" for t in tickers), "Expected JPM in NYSE tickers"

def test_scrape_indices_success():
    """Test _scrape_indices returns valid index tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_indices()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "index" and t["market"] == "GLOBAL" for t in tickers), "Invalid asset_class or market"
    assert any(t["ticker"] == "GSPC" for t in tickers), "Expected GSPC in indices"

def test_scrape_currencies_success():
    """Test _scrape_currencies returns valid currency tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_currencies()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "forex" and t["market"] == "GLOBAL" for t in tickers), "Invalid asset_class or market"

def test_scrape_commodities_success():
    """Test _scrape_commodities returns valid commodity tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_commodities()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "future" and t["market"] == "GLOBAL" for t in tickers), "Invalid asset_class or market"

def test_scrape_cryptos_success():
    """Test _scrape_cryptos returns valid cryptocurrency tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_cryptos()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "crypto" and t["market"] == "GLOBAL" for t in tickers), "Invalid asset_class or market"

def test_scrape_us_treasury_success():
    """Test _scrape_us_treasury returns valid treasury bond tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_us_treasury()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "treasury" and t["market"] == "US" for t in tickers), "Invalid asset_class or market"

def test_scrape_private_companies_success():
    """Test _scrape_private_companies returns valid private company tickers."""
    collector = TickerCollector()
    tickers = collector._scrape_private_companies()
    time.sleep(1)  # Avoid rate limiting
    assert isinstance(tickers, list), "Output is not a list"
    assert len(tickers) > 0, "No tickers returned"
    assert all(isinstance(t, dict) for t in tickers), "Not all items are dictionaries"
    assert all(t["asset_class"] == "private" and t["market"] == "GLOBAL" for t in tickers), "Invalid asset_class or market"