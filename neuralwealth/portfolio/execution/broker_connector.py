from typing import Dict, List
from neuralwealth.portfolio.execution.broker_api import InteractiveBrokersClient
from neuralwealth.portfolio.execution.paper_trading import PaperTradingEngine

class BrokerConnector:
    """Unified interface for paper and live trading using your existing classes"""
    
    def __init__(self, broker_type: str = "paper", **kwargs):
        self.broker_type = broker_type
        
        if broker_type == "live":
            host = kwargs.get('host', '127.0.0.1')
            port = kwargs.get('port', 7497)
            self.broker = InteractiveBrokersClient(host, port)
        else:
            initial_cash = kwargs.get('initial_cash', 100000)
            self.broker = PaperTradingEngine(initial_cash)
    
    def get_portfolio(self) -> Dict:
        """Get current portfolio state"""
        try:
            if self.broker_type == "live":
                positions = self.broker.get_portfolio()
                # Convert to standardized format
                portfolio = {
                    'positions': positions,
                    'cash': 0,  # Would need to get cash balance from IBKR
                    'total_value': sum(positions.values()),  # Simplified
                    'weights': {asset: 1.0/len(positions) for asset in positions} if positions else {}
                }
                return portfolio
            else:
                return self.broker.get_portfolio()
                
        except Exception as e:
            print(f"Error getting portfolio: {e}")
            return {'error': str(e)}
    
    def execute_orders(self, orders: List[Dict]) -> List[Dict]:
        """Execute orders with slippage modeling"""
        executed_orders = []
        
        for order in orders:
            try:
                asset = order['asset']
                action = order['action'].lower()
                quantity = order['quantity']
                estimated_price = order.get('estimated_price', order.get('current_price', 100))
                
                if self.broker_type == "live":
                    # Live trading - use estimated price with slippage
                    result = self.broker.execute_order(asset, action, quantity)
                    result['estimated_price'] = estimated_price
                    result['slippage'] = order.get('estimated_slippage', 0)
                else:
                    # Paper trading - execute with estimated price
                    result = self.broker.execute(asset, action, quantity, estimated_price)
                    result['slippage'] = order.get('estimated_slippage', 0)
                
                executed_orders.append({
                    'asset': asset,
                    'action': action,
                    'quantity': quantity,
                    'estimated_price': estimated_price,
                    'executed_price': estimated_price,  # For paper trading
                    'slippage': result.get('slippage', 0),
                    'status': result.get('status', 'unknown'),
                    'details': result
                })
                
            except Exception as e:
                executed_orders.append({
                    'asset': order.get('asset', 'unknown'),
                    'action': order.get('action', 'unknown'),
                    'status': 'FAILED',
                    'error': str(e)
                })
        
        return executed_orders
    
    def disconnect(self):
        """Disconnect from broker if needed"""
        if self.broker_type == "live" and hasattr(self.broker, 'disconnect'):
            self.broker.disconnect()