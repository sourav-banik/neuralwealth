import shap
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

class Explainability:
    """Handles trade explanations with SHAP and backtest visuals."""

    def explain_trade(self, asset: str, model, data: pd.DataFrame) -> tuple[str, str]:
        """
        Generate SHAP explanation and backtest chart.

        Args:
            asset: Asset ticker (e.g., "TSLA").
            model: Trained model for SHAP (None for mock).
            data: DataFrame with market data.

        Returns:
            Tuple: Paths to SHAP plot and backtest chart.
        """
        try:
            # SHAP explanation
            explainer = shap.Explainer(model) if model else shap.KernelExplainer(lambda x: x[:, 0], data)
            shap_values = explainer(data)
            plt.figure()
            shap.plots.waterfall(shap_values[0])
            shap_path = f"shap_{asset}_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
            plt.savefig(shap_path)
            plt.close()

            # Backtest chart
            dates = pd.date_range(end="2025-08-01", periods=180, freq="D")
            returns = np.random.normal(0, 0.01, 180).cumsum()  # Mock
            df = pd.DataFrame({"Date": dates, "Returns": returns})
            fig = px.line(df, x="Date", y="Returns", title=f"{asset} Backtest Performance")
            backtest_path = f"backtest_{asset}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
            fig.write_html(backtest_path)

            return shap_path, backtest_path
        except Exception:
            return None, None