from tweepy import Client
import feedparser
from transformers import pipeline
from typing import List, Dict

class NewsSentimentCollector:
    """
    Engine that collect news  from public news feed and social media platform and extract sentiment in numerical range.
    - google rss feed for general news
    - X (twitter) for social media sentiment
    """
    def __init__(self, twitter_bearer_token: str):
        """
        setup credentials for X

        Args:
            twitter_bearer_token: bearer token of twitter api
        """
        self.twitter_client = Client(bearer_token=twitter_bearer_token)
        self.sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert", return_all_scores=True)

    def _analyze_sentiment(self, text: str) -> float:
        """
        Analyze sentiment of text using pipeline method

        Args:
            text: input text for sentiment analysis

        Returns:
            numerical value of sentiment in a scale of -1 to 1 where -1 being extreme negative and +1 implies extreme positive
        """
        result = self.sentiment_pipeline(text)[0]
        positive_score = next(r['score'] for r in result if r['label'] == 'positive')
        negative_score = next(r['score'] for r in result if r['label'] == 'negative')
        compound_score = positive_score - negative_score
        return compound_score

    def scrape_twitter_sentiment(self, query: str, limit: int = 100) -> List[Dict]:
        """
        Fetch tweets and analyze sentiment.
        
        Args:
            query: search keyword (e.g., symbol, term)
            limit: maximum number of tweets to be fetched

        Returns:
            A list of dict containing text and sentiment of the scraped tweets 
        """
        tweets = self.twitter_client.search_recent_tweets(query, max_results=limit)
        results = []
        # analyze sentiments of tweet text
        for tweet in tweets.data:
            results.append({"id": tweet.id, "text": tweet.text, "score": self._analyze_sentiment(tweet.text) })
        return results
    
    def scrape_news_sentiment(self, query:str, limit: int = 100) -> List[Dict]:
        """
        Fetch news and analyze sentiment.
        
        Args:
            query: search keyword (e.g., symbol, term)
            limit: maximum number of news item to be fetched

        Returns:
            A list of dict containing text and sentiment of the scraped news 
        """
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(rss_url)
        results = []
        # parsing the feed items and analyze sentiments
        for entry in feed.entries:
            results.append({"id": entry.id, "text": entry.get("title"), "score": self._analyze_sentiment(entry.get("title")) })
        return results
