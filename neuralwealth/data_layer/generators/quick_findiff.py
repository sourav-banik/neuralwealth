import numpy as np
import pandas as pd

class QuickFinDiff:
    """Quick-start version without training"""
    
    def __init__(self):
        self.crash_patterns = {
            '2008': {'duration': 180, 'max_drawdown': -0.55, 'volatility_inc': 3.0},
            '2020': {'duration': 30, 'max_drawdown': -0.35, 'volatility_inc': 4.0},
            'flash': {'duration': 1, 'max_drawdown': -0.15, 'volatility_inc': 6.0}
        }
    
    def generate_scenario(self, 
                        market_data: pd.DataFrame,
                        scenario_type: str = "2008") -> pd.DataFrame:
        """Generate scenario without model training"""
        if scenario_type not in self.crash_patterns:
            raise ValueError(f"Unknown scenario: {scenario_type}")
        
        pattern = self.crash_patterns[scenario_type]
        synthetic = market_data.copy()
        duration = min(pattern['duration'], len(synthetic))
        
        # Apply crash pattern
        crash_start = synthetic.index[-duration]
        crash_period = synthetic.loc[crash_start:].copy()
        
        # Simple crash trajectory
        drawdown_path = np.linspace(0, pattern['max_drawdown'], duration)
        volatility_multiplier = np.linspace(1.0, pattern['volatility_inc'], duration)
        
        for i, (idx, row) in enumerate(crash_period.iterrows()):
            # Apply price crash
            synthetic.loc[idx, 'close'] = row['close'] * (1 + drawdown_path[i])
            
            # Increase volatility
            if 'high' in synthetic.columns and 'low' in synthetic.columns:
                spread = (row['high'] - row['low']) * volatility_multiplier[i]
                synthetic.loc[idx, 'high'] = row['close'] + spread/2
                synthetic.loc[idx, 'low'] = row['close'] - spread/2
            
            # Volume spike
            if 'volume' in synthetic.columns:
                synthetic.loc[idx, 'volume'] = row['volume'] * (1 + volatility_multiplier[i]/2)
        
        synthetic['scenario_type'] = f"findiff_{scenario_type}"
        synthetic['is_synthetic'] = True
        
        return synthetic