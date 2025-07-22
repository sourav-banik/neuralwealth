from neuralwealth.data_layer.collectors.financials_data import FinancialsCollector

def main():
    """
    Executes the FinancialsCollector to fetch and display financial data from yFinance and MacroTrends for a ticker.
    """
    try:
        # Initialize the collector
        collector = FinancialsCollector()

        # Fetch and display yFinance financials for NVDA
        print("Fetching yFinance financial data for NVDA...")
        yfinance_data = collector.scrape_yfinance_financials("NVDA")
        
        # Display first few rows of selected yFinance DataFrames
        print("\nAnnual Income Statement (yFinance, first 5 rows):")
        print(yfinance_data["income_statement"].head().to_string())
        
        print("\nQuarterly Income Statement (yFinance, first 5 rows):")
        print(yfinance_data["quarterly_income_statement"].head().to_string())

        # Fetch and display MacroTrends financials for NVDA
        print("\nFetching MacroTrends financial data for NVDA...")
        macrotrends_data = collector.scrape_macrotrends_financials("NVDA", "nvidia")
        
        # Display first few rows of MacroTrends Income Statement
        print("\nAnnual Income Statement (MacroTrends, first 5 rows):")
        print(macrotrends_data["income_statement"].head().to_string())

    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()