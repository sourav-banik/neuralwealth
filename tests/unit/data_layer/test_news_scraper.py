import pandas as pd
from unittest.mock import patch, MagicMock
from neuralwealth.data_layer.collectors.news_sentiment import NewsSentimentCollector
import pytest
from datetime import datetime

@patch("neuralwealth.data_layer.collectors.news_sentiment.Client")
@patch("neuralwealth.data_layer.collectors.news_sentiment.pipeline")
def test_scrape_twitter_sentiment_success(mock_pipeline, mock_client):
    """
    Test scrape_twitter_sentiment for successful data retrieval.
    """
    mock_tweet = MagicMock()
    mock_tweet.id = 12345
    mock_tweet.text = "Test tweet"
    mock_tweet.created_at = datetime(2025, 6, 28, 10, 0, 0)
    mock_client.return_value.search_recent_tweets.return_value = MagicMock(data=[mock_tweet])
    mock_pipeline.return_value.return_value = [
        [{"label": "positive", "score": 0.6}, {"label": "negative", "score": 0.3}, {"label": "neutral", "score": 0.1}]
    ]
    collector = NewsSentimentCollector(twitter_bearer_token="mock_token")
    df = collector.scrape_twitter_sentiment("AAPL")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.columns[0] == "timestamp"
    assert list(df.columns[1:]) == ["id", "text", "score"]
    assert df.iloc[0]["id"] == 12345
    assert df.iloc[0]["text"] == "Test tweet"
    assert df.iloc[0]["score"] == 0.3  # positive (0.6) - negative (0.3)
    assert df.iloc[0]["timestamp"] == mock_tweet.created_at

@patch("neuralwealth.data_layer.collectors.news_sentiment.feedparser.parse")
@patch("neuralwealth.data_layer.collectors.news_sentiment.pipeline")
def test_scrape_news_sentiment_success(mock_pipeline, mock_feedparser):
    """
    Test scrape_news_sentiment for successful data retrieval.
    """
    mock_entry = MagicMock()
    mock_entry.id = "news_id_123"
    mock_entry.get.side_effect = lambda x: "Test news title" if x == "title" else "2025-06-28T10:00:00Z"
    mock_feedparser.return_value = MagicMock(entries=[mock_entry])
    mock_pipeline.return_value.return_value = [
        [{"label": "positive", "score": 0.7}, {"label": "negative", "score": 0.2}, {"label": "neutral", "score": 0.1}]
    ]
    collector = NewsSentimentCollector(twitter_bearer_token="mock_token")
    df = collector.scrape_news_sentiment("AAPL")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.columns[0] == "timestamp"
    assert list(df.columns[1:]) == ["id", "text", "score"]
    assert df.iloc[0]["id"] == "news_id_123"
    assert df.iloc[0]["text"] == "Test news title"
    assert pytest.approx(df.iloc[0]["score"], 0.1) == 0.5  # positive (0.7) - negative (0.2)
    assert df.iloc[0]["timestamp"] == "2025-06-28T10:00:00Z"