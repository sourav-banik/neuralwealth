import pandas as pd
from tweepy import Client
import feedparser
from transformers import pipeline

class NewsSentimentCollector:
    """
    Engine that collects news from public news feed and social media platform and extracts sentiment in numerical range.
    - Google RSS feed for general news
    - X (Twitter) for social media sentiment
    """
    def __init__(self, twitter_bearer_token: str):
        """
        Args:
            twitter_bearer_token: Bearer token for Twitter API
        """
        self.twitter_client = Client(bearer_token=twitter_bearer_token)
        self.sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert", return_all_scores=True)

    def _analyze_sentiment(self, text: str) -> float:
        """
        Args:
            text: Input text for sentiment analysis
        
        Returns:
            Numerical value of sentiment on a scale of -1 to 1, where -1 is extremely negative and +1 is extremely positive
        """
        result = self.sentiment_pipeline(text)[0]
        positive_score = next(r['score'] for r in result if r['label'] == 'positive')
        negative_score = next(r['score'] for r in result if r['label'] == 'negative')
        compound_score = positive_score - negative_score
        return compound_score

    def scrape_twitter_sentiment(self, query: str, limit: int = 100) -> pd.DataFrame:
        """
        Fetches tweets and analyzes sentiment, returning a DataFrame with timestamps and sentiment data.
        
        Args:
            query: Search keyword (e.g., symbol, term)
            limit: Maximum number of tweets to fetch
        
        Returns:
            DataFrame with unnamed timestamp column, id, text, and sentiment score
        """
        tweets = self.twitter_client.search_recent_tweets(query, max_results=limit)
        results = []
        for tweet in tweets.data:
            results.append({
                "timestamp": tweet.created_at,
                "id": tweet.id,
                "text": tweet.text,
                "score": self._analyze_sentiment(tweet.text)
            })
        df = pd.DataFrame(results)
        return df
    
    def scrape_news_sentiment(self, query: str, limit: int = 100) -> pd.DataFrame:
        """
        Fetches news and analyzes sentiment, returning a DataFrame with timestamps and sentiment data.
        
        Args:
            query: Search keyword (e.g., symbol, term)
            limit: Maximum number of news items to fetch
        
        Returns:
            DataFrame with unnamed timestamp column, id, text, and sentiment score
        """
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={query}&region=US&lang=en-US"
        feed = feedparser.parse(rss_url)
        results = []
        for entry in feed.entries[:limit]:
            results.append({
                "timestamp": entry.get("published"),
                "id": entry.id,
                "text": entry.get("title"),
                "score": self._analyze_sentiment(entry.get("title"))
            })
        df = pd.DataFrame(results)
        return df