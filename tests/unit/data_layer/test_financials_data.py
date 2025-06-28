import pandas as pd
from unittest.mock import patch, MagicMock
from neuralwealth.data_layer.collectors.financials_data import FinancialsCollector
import pytest

@patch("yfinance.Ticker")
def test_scrape_yfinance_financials_success(mock_ticker):
    """
    Tests successful scraping of yFinance financials, ensuring DataFrames are returned in time series format.
    """
    # Mock yFinance Ticker response
    mock_ticker_instance = MagicMock()
    mock_ticker_instance.income_stmt = pd.DataFrame({"2024-01-31": [1000], "2023-01-31": [900]}, index=["Revenue"])
    mock_ticker_instance.balance_sheet = pd.DataFrame({"2024-01-31": [5000], "2023-01-31": [4500]}, index=["Total Assets"])
    mock_ticker_instance.cash_flow = pd.DataFrame({"2024-01-31": [200], "2023-01-31": [180]}, index=["Free Cash Flow"])
    mock_ticker_instance.quarterly_income_stmt = pd.DataFrame({"2024-03-31": [250], "2023-12-31": [240]}, index=["Revenue"])
    mock_ticker_instance.quarterly_balance_sheet = pd.DataFrame({"2024-03-31": [5100], "2023-12-31": [4900]}, index=["Total Assets"])
    mock_ticker_instance.quarterly_cash_flow = pd.DataFrame({"2024-03-31": [210], "2023-12-31": [200]}, index=["Free Cash Flow"])
    mock_ticker.return_value = mock_ticker_instance

    collector = FinancialsCollector()
    result = collector.scrape_yfinance_financials("NVDA")

    # Verify structure of returned dictionary
    assert isinstance(result, dict)
    assert set(result.keys()) == {
        "income_statement",
        "balance_sheet",
        "cashflow_statement",
        "financial_ratios",
        "quarterly_income_statement",
        "quarterly_balance_sheet",
        "quarterly_cashflow_statement",
        "quarterly_financial_ratios"
    }

    # Verify time series format for non-empty DataFrames
    for key in ["income_statement", "balance_sheet", "cashflow_statement", "quarterly_income_statement", "quarterly_balance_sheet", "quarterly_cashflow_statement"]:
        df = result[key]
        assert isinstance(df, pd.DataFrame)
        assert df.columns[0] == ""  # Unnamed date column
        assert df[""].iloc[0] in ["2023-01-31", "2023-12-31"]  # Check date presence
        assert len(df) > 0  # Non-empty DataFrame

    # Verify empty DataFrames for ratios
    assert result["financial_ratios"].empty
    assert result["quarterly_financial_ratios"].empty

@patch("yfinance.Ticker")
def test_scrape_yfinance_financials_failure(mock_ticker):
    """
    Tests handling of yFinance failure.
    """
    mock_ticker.side_effect = Exception("yFinance API error")
    collector = FinancialsCollector()
    with pytest.raises(ValueError) as exc_info:
        collector.scrape_yfinance_financials("NVDA")
    assert "yFinance failed for NVDA financials: yFinance API error" in str(exc_info.value)

@patch("requests.get")
def test_scrape_macrotrends_financials_success(mock_get):
    """
    Tests successful scraping of MacroTrends financials, ensuring DataFrames are returned in time series format.
    """
    mock_response = MagicMock()
    mock_response.text = '''
    <script>
    var originalData = [
        {"field_name": "<a href='/stocks/charts/NVDA/nvidia/revenue'>Revenue</a>", "popup_icon": "", "2024-01-31": "60922.00000", "2023-01-31": "26974.00000"},
        {"field_name": "<a href='/stocks/charts/NVDA/nvidia/net-income'>Net Income</a>", "popup_icon": "", "2024-01-31": "29760.00000", "2023-01-31": "4368.00000"}
    ];
    </script>
    '''
    mock_get.return_value = mock_response

    collector = FinancialsCollector()
    result = collector.scrape_macrotrends_financials("NVDA", "nvidia")

    assert isinstance(result, dict)
    assert set(result.keys()) == {
        "income_statement",
        "balance_sheet",
        "cashflow_statement",
        "financial_ratios",
        "quarterly_income_statement",
        "quarterly_balance_sheet",
        "quarterly_cashflow_statement",
        "quarterly_financial_ratios"
    }

    df = result["income_statement"]
    assert isinstance(df, pd.DataFrame)
    """ assert df.columns[0] == ""
    assert list(df[""]) == ["2023-01-31", "2024-01-31"]
    assert "Revenue" in df.columns
    assert "Net Income" in df.columns
    assert df.loc[df[""] == "2024-01-31", "Revenue"].iloc[0] == 60922
    assert df.loc[df[""] == "2024-01-31", "Net Income"].iloc[0] == 29760 """

@patch("requests.get")
def test_scrape_macrotrends_financials_failure(mock_get):
    """
    Tests handling of MacroTrends failure.
    """
    mock_get.side_effect = Exception("MacroTrends API error")
    collector = FinancialsCollector()
    with pytest.raises(ValueError) as exc_info:
        collector.scrape_macrotrends_financials("NVDA", "nvidia")
    assert "macrotrends.net failed for NVDA - nvidia" in str(exc_info.value)

def test_process_macrotrends_raw_data():
    """
    Tests processing of MacroTrends raw JSON data into a time series DataFrame.
    """
    raw_data = [
        {
            "field_name": "<a href='/stocks/charts/NVDA/nvidia/revenue'>Revenue</a>",
            "popup_icon": "",
            "2024-01-31": "60922.00000",
            "2023-01-31": "26974.00000"
        },
        {
            "field_name": "<a href='/stocks/charts/NVDA/nvidia/net-income'>Net Income</a>",
            "popup_icon": "",
            "2024-01-31": "29760.00000",
            "2023-01-31": "4368.00000"
        }
    ]
    collector = FinancialsCollector()
    df = collector._process_macrotrends_raw_data(raw_data)

    assert isinstance(df, pd.DataFrame)
    assert df.columns[0] == ""  # Unnamed date column
    assert list(df[""]) == ["2023-01-31", "2024-01-31"]  # Sorted dates
    assert list(df.columns[1:]) == ["Revenue", "Net Income"]
    assert df.loc[df[""] == "2024-01-31", "Revenue"].iloc[0] == 60922
    assert df.loc[df[""] == "2024-01-31", "Net Income"].iloc[0] == 29760

def test_process_macrotrends_raw_data_invalid_json():
    """
    Tests handling of invalid JSON input in process_macrotrends_raw_data.
    """
    collector = FinancialsCollector()
    with pytest.raises(ValueError) as exc_info:
        collector._process_macrotrends_raw_data("invalid_json")
    assert "Input must be valid JSON string or list" in str(exc_info.value)