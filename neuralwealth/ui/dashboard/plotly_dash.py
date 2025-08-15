from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface
from neuralwealth.ui.dashboard.risk_metrics import RiskMetrics

class PlotlyDashApp:
    """Interactive portfolio dashboard using Plotly Dash."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize Dash app.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.app = Dash(__name__)
        self.portfolio_interface = portfolio_interface
        self.risk_metrics = RiskMetrics(portfolio_interface)
        self.setup_layout()

    def setup_layout(self):
        """Set up the Dash layout."""
        try:
            portfolio = self.portfolio_interface.get_portfolio()
            df = pd.DataFrame({
                "Asset": list(portfolio.keys()),
                "Weight": [portfolio[k] for k in portfolio]
            })
            var = self.risk_metrics.calculate_var(portfolio)
            sector_exposure = self.risk_metrics.calculate_sector_exposure()
            esg_score = self.risk_metrics.calculate_esg_score()

            fig_pie = px.pie(df, values="Weight", names="Asset", title="Portfolio Allocation")
            fig_bar = px.bar(
                x=list(sector_exposure.keys()),
                y=list(sector_exposure.values()),
                title="Sector Exposure",
                labels={"x": "Sector", "y": "Exposure"}
            )

            self.app.layout = html.Div([
                html.H1("NeuralWealth Portfolio Dashboard"),
                dcc.Graph(figure=fig_pie),
                dcc.Graph(figure=fig_bar),
                html.Div([
                    html.H3("Risk Metrics"),
                    html.P(f"Value-at-Risk (95%): {var:.2%}"),
                    html.P(f"Average ESG Score: {esg_score:.1f}"),
                    html.P(f"Key Drivers: Fed Rate Impact, Sentiment Shift")  # Mock
                ])
            ])
        except Exception as e:
            self.app.layout = html.Div([html.H3(f"Dashboard error: {str(e)}")])

    def run(self, **kwargs):
        """Run the Dash server."""
        self.app.run_server(**kwargs)