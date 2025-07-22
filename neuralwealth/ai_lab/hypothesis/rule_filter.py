from typing import List, Dict

class RuleBasedFilter:
    """Simplified filter for investment hypotheses based on static financial rules."""

    def __init__(self):
        pass

    def apply(self, hypotheses: List[Dict], rules: Dict, excluded_assets: List[Dict] = None) -> List[Dict]:
        """Filter hypotheses based on static rules and optional criteria.

        Args:
            hypotheses: List of hypothesis dictionaries with fields like assets, criteria.
            rules: Dictionary with financial thresholds (e.g., {"max_pe_ratio": 30, "min_market_cap": 1e9}).
            excluded_assets: List of asset dictionaries to exclude (e.g., [{"ticker": "GME", "asset_class": "stock", "market": "NYSE"}]).

        Returns:
            List of filtered hypotheses with valid assets.
        """
        self.excluded_assets = {asset["ticker"] for asset in excluded_assets} if excluded_assets else set()
        valid_hypotheses = []
        for h in hypotheses:
            assets = self._filter_assets(h["assets"], h.get("criteria", rules))
            if assets:
                valid_hypotheses.append({**h, "assets": assets})
        return valid_hypotheses

    def _filter_assets(self, assets: List[Dict], criteria: Dict) -> List[Dict]:
        """Apply fundamental screening to assets.

        Args:
            assets: List of asset dictionaries with 'ticker', 'asset_class', 'market'.
            criteria: Optional hypothesis-specific criteria (e.g., {"min_roe": 0.20}).

        Returns:
            List of valid asset dictionaries passing all filters.
        """
        valid = []
        for asset in assets:
            ticker = asset.get("ticker", "")
            if not ticker or ticker in self.excluded_assets:
                continue
            # Apply hypothesis-specific criteria
            #####
            valid.append(asset)
        return valid