from typing import Dict
from datetime import datetime

class PaperTradingEngine:
    """Simulates trades for testing without real money."""

    def __init__(self, initial_cash: float = 100000):
        """
        Initialize the PaperTradingEngine.

        Args:
            initial_cash: Starting cash balance.
        """
        self.cash = initial_cash
        self.positions = {}
        self.trade_log = []
        self._update_portfolio_value()

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
                if self.cash < cost:
                    return {"status": "failed", "reason": "insufficient_cash"}
                self.cash -= cost
                self.positions[asset] = self.positions.get(asset, 0) + quantity
                
            elif action == "sell":
                if self.positions.get(asset, 0) < quantity:
                    return {"status": "failed", "reason": "insufficient_holdings"}
                self.cash += quantity * price
                self.positions[asset] = self.positions.get(asset, 0) - quantity
                if self.positions[asset] <= 0:
                    del self.positions[asset]
            
            self.trade_log.append({
                "asset": asset, 
                "action": action, 
                "quantity": quantity, 
                "price": price,
                "timestamp": self._get_timestamp()
            })
            
            self._update_portfolio_value()
            return {"status": "success"}

        except Exception as e:
            return {"status": "failed", "reason": str(e)}

    def get_portfolio(self) -> Dict:
        """
        Fetch current simulated portfolio.

        Returns:
            Dict: Current holdings and cash balance.
        """
        self._update_portfolio_value()
        return {
            'cash': self.cash,
            'positions': self.positions.copy(),
            'total_value': self.total_value,
            'weights': self.weights.copy()
        }

    def _update_portfolio_value(self):
        """Update portfolio total value and weights"""
        position_values = {}
        for asset, quantity in self.positions.items():
            # Simplified: assume $100 per unit for all assets
            position_values[asset] = quantity * 100
        
        self.total_value = self.cash + sum(position_values.values())
        
        self.weights = {}
        for asset, value in position_values.items():
            self.weights[asset] = value / self.total_value
        if self.total_value > 0:
            self.weights['CASH'] = self.cash / self.total_value

    def _get_timestamp(self):
        """Get current timestamp string"""
        return datetime.now().isoformat()