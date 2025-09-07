import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, List
from neuralwealth.ui.ui_orchestrator import ui_orchestrator

class Dashboard:
    """Basic Plotly dashboard for portfolio visualization"""
    
    def __init__(self):
        self.figures = {}
    
    def create_portfolio_allocation_chart(self, weights: Dict) -> go.Figure:
        """Create portfolio allocation pie chart"""
        labels = list(weights.keys()) if weights else []
        values = list(weights.values()) if weights else []
        
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.3)])
        fig.update_layout(
            title="Portfolio Allocation",
            showlegend=True,
            height=400
        )
        return fig
    
    def create_performance_chart(self, performance_data: Dict) -> go.Figure:
        """Create performance time series chart"""
        # Sample performance data
        dates = pd.date_range(end=pd.Timestamp.today(), periods=30, freq='D')
        values = [100000 * (1 + i * 0.0015) for i in range(30)]  # Simulated growth
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dates, y=values, mode='lines', name='Portfolio Value'))
        
        fig.update_layout(
            title="Portfolio Performance",
            xaxis_title="Date",
            yaxis_title="Value ($)",
            height=400
        )
        return fig
    
    def create_strategy_metrics(self, strategies: List[Dict]) -> go.Figure:
        """Create strategy metrics visualization"""
        strategy_names = [s.get('name', 'Unknown') for s in strategies or []]
        sharpe_ratios = [s.get('performance', {}).get('sharpe', 0) for s in strategies or []]
        drawdowns = [abs(s.get('performance', {}).get('max_drawdown', 0)) for s in strategies or []]
        
        fig = make_subplots(rows=1, cols=2, subplot_titles=('Sharpe Ratio', 'Max Drawdown'))
        
        fig.add_trace(go.Bar(x=strategy_names, y=sharpe_ratios, name='Sharpe'), 1, 1)
        fig.add_trace(go.Bar(x=strategy_names, y=drawdowns, name='Drawdown'), 1, 2)
        
        fig.update_layout(
            title="Strategy Performance Metrics",
            showlegend=False,
            height=400
        )
        return fig
    
    def generate_explanation_section(self) -> str:
        """Generate static explanation section (placeholder for SHAP/LIME)"""
        return """
        <div style="padding: 20px; background: #f8f9fa; border-radius: 10px;">
            <h3>Portfolio Explanation</h3>
            <p><strong>Why this allocation?</strong><br>
            Your portfolio is optimized for growth with risk control. Technology stocks provide growth potential, while cash reserves offer stability during market volatility.</p>
            
            <p><strong>Key Drivers:</strong><br>
            • Momentum signals in technology sector<br>
            • Low correlation between assets<br>
            • CVaR-based risk management<br>
            • Liquidity considerations</p>
            
            <p><strong>Expected Performance:</strong><br>
            • Annual return: 8-12%<br>
            • Max drawdown: -15%<br>
            • Sharpe ratio: 1.0-1.3</p>
        </div>
        """
    
    def create_scenario_simulator(self, portfolio_value: float, weights: Dict) -> str:
        """Create scenario simulator section"""
        try:
            from .widgets.simulator import scenario_simulator
            scenarios = getattr(scenario_simulator, 'scenarios', {})
        except ImportError:
            scenarios = {}
        
        # Create dropdown options
        options_html = "".join(
            f'<option value="{scenario_id}">{scenario.get("name", "Unknown Scenario")}</option>'
            for scenario_id, scenario in scenarios.items()
        )
        
        simulator_html = f"""
        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>Scenario Simulator</h3>
            <p>Test your portfolio against historical crash scenarios:</p>
            
            <select id="scenario-select" style="padding: 10px; margin: 10px 0; width: 100%;">
                {options_html or '<option value="">No scenarios available</option>'}
            </select>
            
            <button onclick="runSimulation()" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer;">
                Run Simulation
            </button>
            
            <div id="simulation-results" style="margin-top: 20px;"></div>
        </div>
        
        <script>
        function runSimulation() {{
            const scenarioId = document.getElementById('scenario-select').value;
            document.getElementById('simulation-results').innerHTML =
                '<p style="color: #6c757d;">Simulation would run for ' + scenarioId + '</p>';
        }}
        </script>
        """
        return simulator_html
    
    def create_interactive_adjustment(self, current_weights: Dict) -> str:
        """Create interactive portfolio adjustment sliders"""
        sliders_html = "".join(
            f"""
            <div style="margin: 10px 0;">
                <label for="slider-{asset.lower()}" style="display: block; margin-bottom: 5px;">
                    {asset}: <span id="value-{asset.lower()}">{weight:.1%}</span>
                </label>
                <input type="range" id="slider-{asset.lower()}" min="0" max="100" value="{weight*100}"
                    style="width: 100%;" oninput="updateSliderValue('{asset.lower()}', this.value)">
            </div>
            """
            for asset, weight in (current_weights or {}).items()
        )
        
        return f"""
        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>Interactive Portfolio Adjustment</h3>
            <p>Adjust your portfolio weights and see the impact:</p>
            
            {sliders_html or '<p>No assets available for adjustment.</p>'}
            
            <button onclick="analyzeAdjustment()" style="padding: 10px 20px; background: #28a745; color: white; border: none; border-radius: 5px; cursor: pointer; margin-top: 15px;">
                Analyze Impact
            </button>
            
            <div id="adjustment-results" style="margin-top: 20px;"></div>
        </div>
        
        <script>
        function updateSliderValue(asset, value) {{
            document.getElementById('value-' + asset).textContent = (value / 100).toFixed(1) + '%';
        }}
        
        function analyzeAdjustment() {{
            const adjustments = {{}};
            {";".join(f"adjustments['{asset}'] = parseFloat(document.getElementById('slider-{asset.lower()}').value) / 100"
                    for asset in (current_weights or {}).keys())}
            document.getElementById('adjustment-results').innerHTML =
                '<p style="color: #6c757d;">Analysis would show impact of: ' + JSON.stringify(adjustments) + '</p>';
        }}
        </script>
        """
    
    def create_what_if_analysis(self) -> str:
        """Create what-if analysis section"""
        return """
        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>What-If Analysis</h3>
            
            <div style="margin: 15px 0;">
                <label style="display: block; margin-bottom: 5px;">Market Condition:</label>
                <select id="market-condition" style="padding: 8px; width: 100%;">
                    <option value="bull">Bull Market (+20%)</option>
                    <option value="neutral">Neutral Market (+5%)</option>
                    <option value="bear">Bear Market (-15%)</option>
                    <option value="crash">Market Crash (-30%)</option>
                </select>
            </div>
            
            <div style="margin: 15px 0;">
                <label style="display: block; margin-bottom: 5px;">Time Horizon:</label>
                <select id="time-horizon" style="padding: 8px; width: 100%;">
                    <option value="1m">1 Month</option>
                    <option value="3m">3 Months</option>
                    <option value="1y">1 Year</option>
                    <option value="3y">3 Years</option>
                </select>
            </div>
            
            <button onclick="runWhatIfAnalysis()" style="padding: 10px 20px; background: #17a2b8; color: white; border: none; border-radius: 5px; cursor: pointer;">
                Run Analysis
            </button>
            
            <div id="what-if-results" style="margin-top: 20px;"></div>
        </div>
        
        <script>
        function runWhatIfAnalysis() {
            const condition = document.getElementById('market-condition').value;
            const horizon = document.getElementById('time-horizon').value;
            document.getElementById('what-if-results').innerHTML =
                '<p style="color: #6c757d;">Analysis for ' + condition + ' market over ' + horizon + '</p>';
        }
        </script>
        """
    
    def create_download_section(self, session_id: str) -> str:
        """Create report download section"""
        return f"""
        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>Reports & Exports</h3>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 15px 0;">
                <button onclick="downloadReport('html')" style="padding: 10px; background: #6c757d; color: white; border: none; border-radius: 5px; cursor: pointer;">
                    HTML Report
                </button>
                
                <button onclick="downloadReport('json')" style="padding: 10px; background: #6c757d; color: white; border: none; border-radius: 5px; cursor: pointer;">
                    JSON Data
                </button>
                
                <button onclick="downloadReport('csv')" style="padding: 10px; background: #6c757d; color: white; border: none; border-radius: 5px; cursor: pointer;">
                    CSV Export
                </button>
                
                <button onclick="downloadReport('pdf')" style="padding: 10px; background: #6c757d; color: white; border: none; border-radius: 5px; cursor: pointer;">
                    PDF Summary
                </button>
            </div>
            
            <div id="download-status" style="margin-top: 15px;"></div>
        </div>
        
        <script>
        function downloadReport(format) {{
            document.getElementById('download-status').innerHTML =
                '<p style="color: #28a745;">Preparing ' + format.toUpperCase() + ' download for session {session_id}...</p>';
        }}
        </script>
        """
    
    def generate_full_dashboard(self, portfolio_data: Dict, strategies: List[Dict], session_id: str) -> str:
        """Generate complete dashboard HTML"""
        portfolio_data = portfolio_data or {}
        strategies = strategies or []
        session_id = session_id or "default-session"
        
        # Create charts
        allocation_chart = self.create_portfolio_allocation_chart(portfolio_data.get('weights', {}))
        performance_chart = self.create_performance_chart(portfolio_data.get('performance', {}))
        metrics_chart = self.create_strategy_metrics(strategies)
        
        # Convert to HTML
        allocation_html = allocation_chart.to_html(include_plotlyjs='cdn', div_id='allocation-chart')
        performance_html = performance_chart.to_html(include_plotlyjs=False, div_id='performance-chart')
        metrics_html = metrics_chart.to_html(include_plotlyjs=False, div_id='metrics-chart')
        explanations_html = self.generate_explanation_section()
        simulator_html = self.create_scenario_simulator(
            portfolio_data.get('total_value', 100000),
            portfolio_data.get('weights', {})
        )
        adjustment_html = self.create_interactive_adjustment(portfolio_data.get('weights', {}))
        what_if_html = self.create_what_if_analysis()
        download_html = self.create_download_section(session_id)
        
        # Combine into full dashboard
        dashboard_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>NeuralWealth Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .dashboard {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
                .chart {{ background: white; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .interactive, .explanations, .simulator {{ grid-column: 1 / -1; }}
            </style>
        </head>
        <body>
            <h1>NeuralWealth Portfolio Dashboard</h1>
            <div class="dashboard">
                <div class="chart">{allocation_html}</div>
                <div class="chart">{performance_html}</div>
                <div class="chart">{metrics_html}</div>
                
                <div class="chart interactive">{adjustment_html}</div>
                <div class="chart interactive">{what_if_html}</div>
                
                <div class="chart explanations">{explanations_html}</div>
                <div class="chart simulator">{simulator_html}</div>
                <div class="chart interactive">{download_html}</div>
            </div>
        </body>
        </html>
        """
        return dashboard_html

# Global dashboard instance
dashboard = Dashboard()

# Example usage
if __name__ == "__main__":
    # Sample data
    portfolio_data = {
        "weights": {"AAPL": 0.15, "MSFT": 0.15, "GOOGL": 0.14, "CASH": 0.25},
        "performance": {"1d": 0.002, "1w": 0.015, "1m": 0.045}
    }
    
    strategies = [
        {
            "name": "Tech Momentum",
            "performance": {"sharpe": 1.2, "max_drawdown": -0.15}
        },
        {
            "name": "Balanced Growth",
            "performance": {"sharpe": 0.9, "max_drawdown": -0.12}
        }
    ]
    
    # Generate session ID using ui_orchestrator
    session_id = ui_orchestrator.create_session()
    
    # Generate and save dashboard
    html_content = dashboard.generate_full_dashboard(portfolio_data, strategies, session_id)
    with open("dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("Dashboard generated: dashboard.html")