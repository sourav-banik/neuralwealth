import pytest
from unittest.mock import patch, MagicMock
from neuralwealth.data_layer.collectors.news_sentiment import NewsSentimentCollector

@patch("neuralwealth.data_layer.collectors.news_sentiment.Client")
@patch("neuralwealth.data_layer.collectors.news_sentiment.pipeline")
def test_scrape_twitter_sentiment_success(mock_pipeline, mock_client):
    """Test scrape_twitter_sentiment for successful data retrieval."""
    # Mock tweet data
    mock_tweet = MagicMock()
    mock_tweet.id = 12345
    mock_tweet.text = "Test tweet"
    mock_client.return_value.search_recent_tweets.return_value = MagicMock(data=[mock_tweet])
    # Mock sentiment pipeline
    mock_pipeline.return_value.return_value = [
        [{"label": "positive", "score": 0.6}, {"label": "negative", "score": 0.3}, {"label": "neutral", "score": 0.1}]
    ]
    collector = NewsSentimentCollector(twitter_bearer_token="mock_token")
    data = collector.scrape_twitter_sentiment("AAPL")
    assert len(data) == 1
    assert data[0]["id"] == 12345
    assert data[0]["text"] == "Test tweet"
    assert data[0]["score"] == 0.3  # positive (0.6) - negative (0.3)

@patch("neuralwealth.data_layer.collectors.news_sentiment.feedparser.parse")
@patch("neuralwealth.data_layer.collectors.news_sentiment.pipeline")
def test_scrape_news_sentiment_success(mock_pipeline, mock_feedparser):
    """Test scrape_news_sentiment for successful data retrieval."""
    # Mock RSS feed data
    mock_entry = MagicMock()
    mock_entry.id = "news_id_123"
    mock_entry.get.return_value = "Test news title"
    mock_feedparser.return_value = MagicMock(entries=[mock_entry])
    # Mock sentiment pipeline
    mock_pipeline.return_value.return_value = [
        [{"label": "positive", "score": 0.7}, {"label": "negative", "score": 0.2}, {"label": "neutral", "score": 0.1}]
    ]
    collector = NewsSentimentCollector(twitter_bearer_token="mock_token")
    data = collector.scrape_news_sentiment("AAPL")
    assert len(data) == 1
    assert data[0]["id"] == "news_id_123"
    assert data[0]["text"] == "Test news title"
    assert pytest.approx(data[0]["score"], 0.1) == 0.5  # positive (0.7) - negative (0.2)