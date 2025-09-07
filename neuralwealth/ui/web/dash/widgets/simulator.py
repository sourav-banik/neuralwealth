import plotly.graph_objects as go
from typing import Dict, List, Any
import numpy as np
from datetime import datetime, timedelta

class ScenarioSimulator:
    """Scenario simulator for crash analysis"""
    
    def __init__(self):
        self.scenarios = self._load_scenarios()
        
    def _load_scenarios(self) -> Dict[str, Dict]:
        """Load pre-defined crash scenarios"""
        return {
            "crash_2008": {
                "name": "2008 Financial Crisis",
                "description": "Global financial crisis scenario",
                "max_drawdown": -0.55,
                "duration_days": 180,
                "recovery_days": 720,
                "affected_sectors": ["financial", "real_estate", "consumer"],
                "volatility_multiplier": 3.5
            },
            "crash_2020": {
                "name": "2020 COVID Crash", 
                "description": "COVID-19 pandemic market crash",
                "max_drawdown": -0.35,
                "duration_days": 33,
                "recovery_days": 180,
                "affected_sectors": ["travel", "energy", "small_cap"],
                "volatility_multiplier": 4.0
            },
            "crash_flash": {
                "name": "Flash Crash",
                "description": "2010-style flash crash scenario", 
                "max_drawdown": -0.15,
                "duration_days": 1,
                "recovery_days": 7,
                "affected_sectors": ["all"],
                "volatility_multiplier": 6.0
            },
            "crash_inflation": {
                "name": "High Inflation",
                "description": "1970s-style stagflation scenario",
                "max_drawdown": -0.45,
                "duration_days": 504,
                "recovery_days": 1008,
                "affected_sectors": ["growth", "technology", "bonds"],
                "volatility_multiplier": 2.5
            }
        }
    
    def simulate_scenario(self, portfolio_value: float, scenario_id: str, 
                         current_weights: Dict) -> Dict[str, Any]:
        """Simulate portfolio performance in a crash scenario"""
        scenario = self.scenarios.get(scenario_id, self.scenarios["crash_2008"])
        
        # Generate crash trajectory
        crash_duration = scenario["duration_days"]
        recovery_duration = scenario["recovery_days"]
        total_days = crash_duration + recovery_duration
        
        # Create timeline
        dates = [datetime.now() + timedelta(days=i) for i in range(total_days)]
        
        # Generate crash phase
        crash_returns = np.random.normal(
            -scenario["max_drawdown"] / crash_duration, 
            scenario["volatility_multiplier"] * 0.02, 
            crash_duration
        )
        
        # Generate recovery phase  
        recovery_returns = np.random.normal(
            scenario["max_drawdown"] / recovery_duration,
            scenario["volatility_multiplier"] * 0.015,
            recovery_duration
        )
        
        # Combine returns
        all_returns = np.concatenate([crash_returns, recovery_returns])
        
        # Calculate portfolio values
        values = [portfolio_value]
        for ret in all_returns:
            values.append(values[-1] * (1 + ret))
        
        # Calculate metrics
        min_value = min(values)
        max_drawdown = (min_value - portfolio_value) / portfolio_value
        recovery_day = next((i for i, v in enumerate(values) if v >= portfolio_value), total_days)
        
        return {
            "scenario_name": scenario["name"],
            "dates": [d.strftime("%Y-%m-%d") for d in dates],
            "values": values[1:],  # Exclude initial value
            "max_drawdown": max_drawdown,
            "recovery_days": recovery_day - crash_duration if recovery_day > crash_duration else 0,
            "affected_assets": self._get_affected_assets(current_weights, scenario),
            "summary": self._generate_summary(portfolio_value, min_value, max_drawdown, recovery_day)
        }
    
    def _get_affected_assets(self, weights: Dict, scenario: Dict) -> List[Dict]:
        """Get assets most affected by the scenario"""
        # Simplified: assume technology assets are more affected in most crashes
        tech_assets = [asset for asset in weights.keys() if any(x in asset.lower() for x in ['aapl', 'msft', 'goog', 'tech'])]
        
        affected = []
        for asset, weight in weights.items():
            impact_multiplier = 1.5 if asset in tech_assets else 1.0
            impact = weight * impact_multiplier * abs(scenario["max_drawdown"])
            affected.append({
                "asset": asset,
                "current_weight": weight,
                "estimated_impact": round(impact, 4),
                "impact_percentage": round(impact * 100, 1)
            })
        
        # Sort by impact
        return sorted(affected, key=lambda x: x["estimated_impact"], reverse=True)[:5]
    
    def _generate_summary(self, initial_value: float, min_value: float, 
                         max_drawdown: float, recovery_day: int) -> str:
        """Generate scenario summary text"""
        return f"""
        In this scenario, your portfolio would:
        - Decline from ${initial_value:,.0f} to ${min_value:,.0f}
        - Experience a maximum drawdown of {max_drawdown:.1%}
        - Take approximately {recovery_day} days to recover
        - Be most affected by technology and growth assets
        """
    
    def create_simulation_chart(self, simulation_data: Dict) -> go.Figure:
        """Create visualization for simulation results"""
        fig = go.Figure()
        
        # Add scenario trajectory
        fig.add_trace(go.Scatter(
            x=simulation_data["dates"],
            y=simulation_data["values"],
            mode='lines',
            name=simulation_data["scenario_name"],
            line=dict(color='red', width=2)
        ))
        
        # Add reference line at initial value
        initial_value = simulation_data["values"][0] / (1 + simulation_data["values"][0] * 0.0001)  # Approximate initial
        fig.add_hline(
            y=initial_value,
            line_dash="dash",
            line_color="green",
            annotation_text="Initial Value"
        )
        
        fig.update_layout(
            title=f"Scenario Simulation: {simulation_data['scenario_name']}",
            xaxis_title="Date",
            yaxis_title="Portfolio Value ($)",
            hovermode="x unified",
            height=500
        )
        
        return fig
    
    def create_impact_chart(self, affected_assets: List[Dict]) -> go.Figure:
        """Create chart showing asset impact"""
        assets = [a["asset"] for a in affected_assets]
        impacts = [a["impact_percentage"] for a in affected_assets]
        
        fig = go.Figure(data=[go.Bar(x=assets, y=impacts)])
        fig.update_layout(
            title="Most Affected Assets",
            xaxis_title="Asset",
            yaxis_title="Impact (%)",
            height=300
        )
        
        return fig

# Global simulator instance
scenario_simulator = ScenarioSimulator()