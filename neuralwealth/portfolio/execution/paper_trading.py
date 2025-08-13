from typing import Dict

class PaperTradingEngine:
    """Simulates trades for testing without real money."""

    def __init__(self, initial_cash: float = 100000):
        """
        Initialize the PaperTradingEngine.

        Args:
            initial_cash: Starting cash balance.
        """
        self.portfolio = {"CASH": initial_cash}
        self.trade_log = []

    def execute(self, asset: str, action: str, quantity: float, price: float) -> Dict:
        """
        Simulate a trade.

        Args:
            asset: Asset ticker.
            action: "buy" or "sell".
            quantity: Number of shares/contracts.
            price: Current market price.

        Returns:
            Dict: Simulated trade details.
        """
        try:
            if action == "buy":
                cost = quantity * price
                if self.portfolio["CASH"] < cost:
                    return {"status": "failed", "reason": "insufficient_cash"}
                self.portfolio["CASH"] -= cost
                self.portfolio[asset] = self.portfolio.get(asset, 0) + quantity
            elif action == "sell":
                if self.portfolio.get(asset, 0) < quantity:
                    return {"status": "failed", "reason": "insufficient_holdings"}
                self.portfolio["CASH"] += quantity * price
                self.portfolio[asset] = self.portfolio.get(asset, 0) - quantity
            self.trade_log.append({"asset": asset, "action": action, "quantity": quantity, "price": price})
            return {"status": "success", "portfolio": self.portfolio.copy()}
        except Exception as e:
            return {"status": "failed", "reason": str(e)}

    def get_portfolio(self) -> Dict:
        """
        Fetch current simulated portfolio.

        Returns:
            Dict: Current holdings and cash balance.
        """
        return self.portfolio.copy()