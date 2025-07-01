import sys
sys.path.append(r"C:\Users\MyPC\myprojects\neuralwealth")

from neuralwealth.data_layer.collectors.macro_data import FREDCollector
from neuralwealth.env import data_pipeline_env

def main():
    """
    Executes the FREDCollector to fetch and display macroeconomic data for a series and all predefined indicators.
    """
    try:
        
        collector = FREDCollector(api_key=data_pipeline_env['fred_api_key'])

        # Fetch and display data for a single series (e.g., GDP)
        print("Fetching time series data for GDP...")
        gdp_data = collector.fetch_series("GDP")
        
        print("\nGDP Time Series Data (first 5 rows):")
        print(gdp_data.head().to_string())

        # Fetch and display data for all predefined indicators
        print("\nFetching all predefined macroeconomic indicators...")
        all_data = collector.fetch_all()
        
        # Display summary of all indicators
        print("\nAvailable Indicators:", list(all_data.keys()))
        print("\nSample Data for First Indicator (first 5 rows):")
        first_indicator = list(all_data.keys())[0]
        print(f"Indicator: {first_indicator}")
        print(all_data[first_indicator].head().to_string())

    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()