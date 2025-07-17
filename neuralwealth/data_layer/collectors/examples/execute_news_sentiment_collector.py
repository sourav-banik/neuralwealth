from neuralwealth.data_layer.collectors.news_sentiment import NewsSentimentCollector
from neuralwealth.env import data_pipeline_env

def main():
    """
    Executes the NewsSentimentCollector to fetch and display Twitter and news sentiment data for a query.
    """
    try:
        
        # Initialize the collector
        collector = NewsSentimentCollector(twitter_bearer_token=data_pipeline_env['twitter_bearer_token'])

        # Fetch and display Twitter sentiment data for NVDA
        print("Fetching Twitter sentiment data for NVDA...")
        twitter_data = collector.scrape_twitter_sentiment("NVDA", limit=10)
        
        # Display first few rows of the Twitter DataFrame
        print("\nTwitter Sentiment Data for NVDA (first 5 rows):")
        print(twitter_data.head().to_string())

        # Fetch and display news sentiment data for NVDA
        print("\nFetching news sentiment data for NVDA...")
        news_data = collector.scrape_news_sentiment("NVDA", limit=10)
        
        # Display first few rows of the News DataFrame
        print("\nNews Sentiment Data for NVDA (first 5 rows):")
        print(news_data.head().to_string())

    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()