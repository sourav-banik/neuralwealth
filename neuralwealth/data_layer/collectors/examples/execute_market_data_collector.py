from neuralwealth.data_layer.collectors.market_data import MarketDataCollector

def main():
    """
    Executes the MarketDataCollector to fetch and display market data and basic info for a ticker.
    """
    try:
        collector = MarketDataCollector()

        print("Fetching basic information for NVDA...")
        basic_info = collector.get_basic_info("NVDA")
        
        # Display selected fields from basic info
        print("\nBasic Information for NVDA:")
        selected_fields = {
            "longName": "Company Name",
            "exchange": "Exchange",
            "sector": "Sector",
            "industry": "Industry",
            "fiftyTwoWeekHigh": "52 Week High",
            "fiftyTwoWeekLow": "52 Week Low",
            "marketCap": "Market Cap"
        }
        for key, label in selected_fields.items():
            print(f"{label}: {basic_info.get(key, 'N/A')}")

        # Fetch and display OHLCV data for NVDA (1 month period)
        print("\nFetching OHLCV data for NVDA (1 month)...")
        market_data = collector.get_market_data("NVDA", period="1mo")
        
        # Display first few rows of the DataFrame
        print("\nOHLCV Data for NVDA (first 5 rows):")
        print(market_data.head().to_string())

    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()