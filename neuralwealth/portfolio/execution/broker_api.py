from ib_insync import IB, Stock
from typing import Dict

class InteractiveBrokersClient:
    """Executes trades through Interactive Brokers API."""

    def __init__(self, host: str = '127.0.0.1', port: int = 7497):
        """
        Initialize the InteractiveBrokersClient.

        Args:
            host: IBKR host address.
            port: IBKR port number.
        """
        self.ib = IB()
        try:
            self.ib.connect(host, port)
        except Exception as e:
            raise

    def execute_order(self, asset: str, action: str, quantity: float) -> Dict:
        """
        Place market order through IBKR.

        Args:
            asset: Asset ticker.
            action: "buy" or "sell".
            quantity: Number of shares/contracts.

        Returns:
            Dict: Trade execution details.
        """
        try:
            contract = Stock(asset, 'SMART', 'USD')
            order = self.ib.marketOrder(action.upper(), quantity)
            trade = self.ib.placeOrder(contract, order)
            return {"status": "success", "trade_id": trade.order.orderId}
        except Exception as e:
            return {"status": "failed", "reason": str(e)}

    def get_portfolio(self) -> Dict:
        """
        Fetch current portfolio holdings.

        Returns:
            Dict: Current holdings with asset symbols and quantities.
        """
        try:
            portfolio = {pos.contract.symbol: pos.position for pos in self.ib.portfolio()}
            return portfolio
        except Exception as e:
            return {}