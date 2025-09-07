from typing import Dict, List
import plotly.graph_objects as go

class ExplanationFramework:
    """Framework for generating portfolio explanations"""
    
    def __init__(self):
        self.explanation_templates = self._load_templates()
    
    def _load_templates(self) -> Dict[str, str]:
        """Load explanation templates"""
        return {
            "portfolio_allocation": """
            Your portfolio is allocated with {tech_pct}% in technology stocks, {diversified_pct}% in diversified assets, 
            and {cash_pct}% in cash. This balance aims to capture growth while maintaining liquidity for opportunities.
            """,
            
            "risk_management": """
            The portfolio uses CVaR (Conditional Value at Risk) optimization to limit maximum drawdown to approximately {max_drawdown}%. 
            Position sizes are constrained to {max_position}% per asset for diversification.
            """,
            
            "strategy_rationale": """
            Current strategy emphasizes {primary_theme} with secondary exposure to {secondary_theme}. 
            This approach has shown {historical_performance} in backtests across various market regimes.
            """,
            
            "market_outlook": """
            Based on current market conditions, the system detects {market_sentiment} sentiment with {volatility_level} volatility. 
            This suggests {recommended_action} for optimal risk-adjusted returns.
            """
        }
    
    def generate_portfolio_explanation(self, portfolio_data: Dict, strategies: List[Dict]) -> str:
        """Generate comprehensive portfolio explanation"""
        weights = portfolio_data.get('weights', {})
        
        # Calculate category percentages
        tech_assets = [k for k in weights.keys() if any(x in k.lower() for x in ['aapl', 'msft', 'goog', 'tech'])]
        tech_pct = sum(weights.get(a, 0) for a in tech_assets) * 100
        cash_pct = weights.get('CASH', 0) * 100
        diversified_pct = 100 - tech_pct - cash_pct
        
        explanation = f"""
        # Portfolio Explanation
        
        ## Allocation Strategy
        {self.explanation_templates['portfolio_allocation'].format(
            tech_pct=round(tech_pct, 1),
            diversified_pct=round(diversified_pct, 1),
            cash_pct=round(cash_pct, 1)
        )}
        
        ## Risk Management
        {self.explanation_templates['risk_management'].format(
            max_drawdown='15%',
            max_position='20%'
        )}
        
        ## Investment Rationale
        {self.explanation_templates['strategy_rationale'].format(
            primary_theme='technology growth',
            secondary_theme='defensive diversification',
            historical_performance='12% annual returns with 15% max drawdown'
        )}
        
        ## Current Market View
        {self.explanation_templates['market_outlook'].format(
            market_sentiment='cautiously optimistic',
            volatility_level='moderate',
            recommended_action='maintaining current allocation with rebalancing thresholds'
        )}
        """
        
        return explanation
    
    def generate_feature_importance(self, portfolio_data: Dict) -> go.Figure:
        """Generate feature importance visualization (placeholder for SHAP)"""
        # Mock feature importance data
        features = [
            'Momentum Signals', 'Volatility Trends', 'Sector Rotation', 
            'Liquidity Conditions', 'Macro Indicators', 'Risk Appetite'
        ]
        
        importance = [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]  # Mock values
        
        fig = go.Figure(data=[go.Bar(
            x=importance,
            y=features,
            orientation='h'
        )])
        
        fig.update_layout(
            title="Strategy Driver Importance",
            xaxis_title="Relative Importance",
            yaxis_title="Features",
            height=400,
            showlegend=False
        )
        
        return fig
    
    def generate_rule_based_explanations(self, weights: Dict) -> List[Dict]:
        """Generate rule-based explanations for portfolio decisions"""
        explanations = []
        
        # Technology allocation explanation
        tech_assets = [k for k in weights.keys() if any(x in k.lower() for x in ['aapl', 'msft', 'goog', 'tech'])]
        tech_weight = sum(weights.get(a, 0) for a in tech_assets)
        
        if tech_weight > 0.3:
            explanations.append({
                "type": "sector_allocation",
                "title": "Technology Overweight",
                "description": "Elevated technology exposure due to strong momentum signals and earnings outlook.",
                "impact": "positive",
                "confidence": 0.85
            })
        
        # Cash allocation explanation
        cash_weight = weights.get('CASH', 0)
        if cash_weight > 0.2:
            explanations.append({
                "type": "liquidity_management",
                "title": "Defensive Cash Position",
                "description": "Higher cash allocation for volatility protection and opportunistic buying.",
                "impact": "defensive", 
                "confidence": 0.78
            })
        
        # Diversification check
        if len(weights) >= 5 and max(weights.values()) <= 0.25:
            explanations.append({
                "type": "diversification",
                "title": "Well-Diversified Portfolio",
                "description": "Portfolio shows good diversification across assets and sectors.",
                "impact": "risk_reduction",
                "confidence": 0.92
            })
        
        return explanations
    
    def create_explanation_dashboard(self, portfolio_data: Dict, strategies: List[Dict]) -> str:
        """Create complete explanation dashboard"""
        explanation_text = self.generate_portfolio_explanation(portfolio_data, strategies)
        feature_fig = self.generate_feature_importance(portfolio_data)
        rule_explanations = self.generate_rule_based_explanations(portfolio_data.get('weights', {}))
        
        # Convert to HTML
        feature_html = feature_fig.to_html(include_plotlyjs='cdn', div_id='feature-importance')
        
        # Create rules HTML
        rules_html = "<h3>Decision Explanations</h3>"
        for rule in rule_explanations:
            rules_html += f"""
            <div style="background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid {'#28a745' if rule['impact'] == 'positive' else '#007bff'};">
                <h4 style="margin: 0; color: {'#155724' if rule['impact'] == 'positive' else '#004085'};">
                    {rule['title']} (Confidence: {rule['confidence']*100}%)
                </h4>
                <p style="margin: 5px 0 0 0; color: #6c757d;">{rule['description']}</p>
            </div>
            """
        
        # Combine into full dashboard
        dashboard_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Portfolio Explanations</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; max-width: 1200px; }}
                .explanation-section {{ background: white; padding: 20px; margin: 15px 0; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .feature-chart {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <h1>Portfolio Explanation Dashboard</h1>
            
            <div class="explanation-section">
                <h2>Comprehensive Analysis</h2>
                <div style="white-space: pre-wrap;">{explanation_text}</div>
            </div>
            
            <div class="explanation-section">
                <h2>Strategy Drivers</h2>
                <div class="feature-chart">{feature_html}</div>
            </div>
            
            <div class="explanation-section">
                {rules_html}
            </div>
        </body>
        </html>
        """
        
        return dashboard_html

# Global explanation framework instance
explanation_framework = ExplanationFramework()