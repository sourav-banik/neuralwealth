from typing import Dict, List
import pandas as pd

class RiskManager:
    """Enhanced risk manager with pre-trade checks and slippage modeling"""
    
    def __init__(self, constraints: Dict):
        self.constraints = constraints
        self.slippage_models = {}
    
    def validate_weights(self, weights: Dict, market_data: pd.DataFrame) -> bool:
        """Validate portfolio weights against constraints"""
        try:
            total_weight = sum(weights.values())
            if abs(total_weight - 1.0) > 0.01:
                return False
            
            max_position = self.constraints.get('max_position_size', 0.2)
            for asset, weight in weights.items():
                if weight > max_position:
                    return False
            
            sector_limits = self.constraints.get('sector_limits', {})
            if sector_limits:
                sector_exposure = self._calculate_sector_exposure(weights, market_data)
                for sector, exposure in sector_exposure.items():
                    limit = sector_limits.get(sector, 1.0)
                    if exposure > limit:
                        return False
            
            leverage_limit = self.constraints.get('max_leverage', 1.0)
            if total_weight > leverage_limit:
                return False
            
            return True
            
        except Exception as e:
            print(f"Weight validation error: {e}")
            return False
    
    def pre_trade_check(self, orders: List[Dict], market_data: pd.DataFrame) -> List[Dict]:
        """Perform pre-trade checks on orders"""
        validated_orders = []
        
        for order in orders:
            try:
                asset = order['asset']
                action = order['action']
                quantity = order['quantity']
                price = order.get('current_price', 100)
                
                # Check liquidity
                if not self._check_liquidity(asset, quantity, market_data):
                    order['status'] = 'REJECTED'
                    order['reason'] = 'insufficient_liquidity'
                    validated_orders.append(order)
                    continue
                
                # Check volatility
                if not self._check_volatility(asset, market_data):
                    order['status'] = 'REJECTED'
                    order['reason'] = 'high_volatility'
                    validated_orders.append(order)
                    continue
                
                # Check market hours (simplified)
                if not self._check_market_hours():
                    order['status'] = 'REJECTED'
                    order['reason'] = 'market_closed'
                    validated_orders.append(order)
                    continue
                
                # Estimate slippage
                slippage = self.estimate_slippage(asset, action, quantity, market_data)
                order['estimated_slippage'] = slippage
                order['estimated_price'] = price + (slippage if action == 'BUY' else -slippage)
                order['status'] = 'APPROVED'
                
                validated_orders.append(order)
                
            except Exception as e:
                order['status'] = 'REJECTED'
                order['reason'] = f'validation_error: {str(e)}'
                validated_orders.append(order)
        
        return validated_orders
    
    def estimate_slippage(self, asset: str, action: str, quantity: float, market_data: pd.DataFrame) -> float:
        """Estimate slippage for a trade"""
        try:
            # Simple slippage model based on volume and order size
            if market_data.empty or asset not in market_data.index:
                return 0.02  # 2% default slippage
            
            asset_data = market_data.loc[asset]
            daily_volume = asset_data.get('volume', 1000000)
            price = asset_data.get('price', 100)
            
            # Order size as percentage of daily volume
            order_size_pct = (quantity * price) / daily_volume
            
            # Basic slippage model
            base_slippage = 0.0005  # 5 basis points
            size_impact = 0.001 * order_size_pct  # 10bps per 1% of daily volume
            
            # Market impact (higher for buys in rising markets, sells in falling markets)
            market_impact = 0.0
            if 'prev_close' in asset_data:
                price_change = (price - asset_data['prev_close']) / asset_data['prev_close']
                if action == 'BUY' and price_change > 0:
                    market_impact = 0.0002 * price_change
                elif action == 'SELL' and price_change < 0:
                    market_impact = 0.0002 * abs(price_change)
            
            total_slippage = base_slippage + size_impact + market_impact
            return min(total_slippage, 0.05)  # Cap at 5%
            
        except Exception as e:
            print(f"Slippage estimation error for {asset}: {e}")
            return 0.02  # Default 2% slippage
    
    def _check_liquidity(self, asset: str, quantity: float, market_data: pd.DataFrame) -> bool:
        """Check if sufficient liquidity exists for the trade"""
        try:
            if market_data.empty or asset not in market_data.index:
                return True  # Assume sufficient liquidity if no data
            
            asset_data = market_data.loc[asset]
            daily_volume = asset_data.get('volume', 1000000)
            
            # Reject if order > 5% of daily volume
            return (quantity / daily_volume) <= 0.05
            
        except:
            return True
    
    def _check_volatility(self, asset: str, market_data: pd.DataFrame) -> bool:
        """Check if asset volatility is within acceptable limits"""
        try:
            if market_data.empty or asset not in market_data.index:
                return True
            
            asset_data = market_data.loc[asset]
            volatility = asset_data.get('volatility', 0.02)  # Default 2%
            
            # Reject if volatility > 10%
            return volatility <= 0.10
            
        except:
            return True
    
    def _check_market_hours(self) -> bool:
        """Check if markets are open (simplified)"""
        from datetime import datetime, time
        now = datetime.now()
        market_open = time(9, 30)  # 9:30 AM
        market_close = time(16, 0)  # 4:00 PM
        
        # Simple check: Monday-Friday, 9:30-4:00
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        return market_open <= now.time() <= market_close
    
    def _calculate_sector_exposure(self, weights: Dict, market_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate sector exposure from weights"""
        sector_exposure = {}
        
        for asset, weight in weights.items():
            sector = "unknown"
            if not market_data.empty and asset in market_data.index:
                sector = market_data.loc[asset].get('sector', 'unknown')
            sector_exposure[sector] = sector_exposure.get(sector, 0) + weight
        
        return sector_exposure