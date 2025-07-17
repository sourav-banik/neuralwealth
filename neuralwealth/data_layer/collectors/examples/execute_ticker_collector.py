from neuralwealth.data_layer.collectors.ticker_collector import TickerCollector

def main():
    """
    Executes the TickerCollector to fetch and display ticker symbols from various markets and asset classes.
    """
    try:
        # Initialize the collector
        collector = TickerCollector()

        # Fetch all tickers
        print("Fetching ticker symbols from all sources...")
        tickers = collector.collect_tickers()

        # Group tickers by market and asset class for summary
        grouped_tickers = {}
        for ticker in tickers:
            key = (ticker['market'], ticker['asset_class'])
            if key not in grouped_tickers:
                grouped_tickers[key] = []
            grouped_tickers[key].append(ticker['ticker'])

        # Display summary and samples
        print("\nSummary of Collected Tickers:")
        for (market, asset_class), ticker_list in grouped_tickers.items():
            print(f"\nMarket: {market}, Asset Class: {asset_class} ({len(ticker_list)} tickers)")
            print(f"Sample (first 5): {ticker_list[:5]}")

    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()