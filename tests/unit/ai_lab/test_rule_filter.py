from unittest.mock import patch
import pytest
from neuralwealth.ai_lab.hypothesis.rule_filter import RuleBasedFilter


def test_init():
    filter = RuleBasedFilter()
    assert isinstance(filter, RuleBasedFilter)


def test_apply_no_excluded_assets():
    filter = RuleBasedFilter()
    hypotheses = [
        {
            "hypothesis": "Test hypothesis",
            "assets": [
                {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}
            ],
            "criteria": {"max_pe_ratio": 30, "min_market_cap": 1e9}
        }
    ]
    rules = {"max_pe_ratio": 30, "min_market_cap": 1e9}
    result = filter.apply(hypotheses, rules)
    assert len(result) == 1
    assert result[0]["hypothesis"] == "Test hypothesis"
    assert len(result[0]["assets"]) == 2
    assert result[0]["assets"] == hypotheses[0]["assets"]


def test_apply_with_excluded_assets():
    filter = RuleBasedFilter()
    hypotheses = [
        {
            "hypothesis": "Test hypothesis",
            "assets": [
                {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "GME", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}
            ],
            "criteria": {"max_pe_ratio": 30, "min_market_cap": 1e9}
        }
    ]
    rules = {"max_pe_ratio": 30, "min_market_cap": 1e9}
    excluded_assets = [{"ticker": "GME", "asset_class": "stock", "market": "NYSE"}]
    result = filter.apply(hypotheses, rules, excluded_assets)
    assert len(result) == 1
    assert len(result[0]["assets"]) == 2
    assert all(asset["ticker"] != "GME" for asset in result[0]["assets"])
    assert all(asset["ticker"] in ["AAPL", "MSFT"] for asset in result[0]["assets"])


def test_filter_assets_empty_criteria():
    filter = RuleBasedFilter()
    assets = [
        {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"},
        {"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}
    ]
    criteria = {}
    # Set excluded_assets explicitly for the test
    filter.excluded_assets = set()
    result = filter._filter_assets(assets, criteria)
    assert len(result) == 2
    assert result == assets


def test_filter_assets_excluded_ticker():
    filter = RuleBasedFilter()
    filter.excluded_assets = {"GME"}
    assets = [
        {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"},
        {"ticker": "GME", "asset_class": "stock", "market": "NYSE"},
        {"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"}
    ]
    criteria = {"max_pe_ratio": 30}
    result = filter._filter_assets(assets, criteria)
    assert len(result) == 2
    assert all(asset["ticker"] != "GME" for asset in result)


def test_apply_empty_hypotheses():
    filter = RuleBasedFilter()
    hypotheses = []
    rules = {"max_pe_ratio": 30, "min_market_cap": 1e9}
    result = filter.apply(hypotheses, rules)
    assert result == []


def test_filter_assets_no_ticker():
    filter = RuleBasedFilter()
    filter.excluded_assets = set()
    assets = [
        {"ticker": "", "asset_class": "stock", "market": "NASDAQ"},
        {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}
    ]
    criteria = {"max_pe_ratio": 30}
    result = filter._filter_assets(assets, criteria)
    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL"