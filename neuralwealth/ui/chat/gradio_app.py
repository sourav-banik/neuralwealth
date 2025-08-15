import gradio as gr
import pandas as pd
import requests
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface
from neuralwealth.ui.chat.explainability import Explainability

class GradioApp:
    """Gradio-based chat interface for NeuralWealth."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize Gradio app.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.portfolio_interface = portfolio_interface
        self.explainability = Explainability()
        self.demo = self.create_interface()

    def create_interface(self):
        """Create Gradio interface."""
        with gr.Blocks() as demo:
            gr.Markdown("# NeuralWealth Chat Interface")
            query_input = gr.Textbox(label="Ask NeuralWealth...")
            output_text = gr.Markdown()
            output_image = gr.Image(label="SHAP Plot")
            output_html = gr.HTML(label="Backtest Chart")
            feedback = gr.Radio(["Thumbs Up", "Thumbs Down", "Too Risky"], label="Feedback")
            feedback_button = gr.Button("Submit Feedback")
            feedback_output = gr.Textbox(label="Feedback Status")

            query_input.submit(self.respond_to_query, inputs=query_input, outputs=[output_text, output_image, output_html])
            feedback_button.click(self.submit_feedback, inputs=[query_input, feedback], outputs=feedback_output)

        return demo

    def respond_to_query(self, query: str):
        """Handle user query and return explanation with visuals."""
        try:
            response = requests.post("http://localhost:8000/query", json={"text": query})
            response.raise_for_status()
            data = response.json()

            asset = query.split()[-1].upper() if "sell" in query.lower() or "buy" in query.lower() else None
            shap_plot, backtest_plot = None, None
            if asset:
                data_df = self.portfolio_interface.get_asset_data(
                    [asset], ["close", "rsi_14", "volume"], "2025-07-01", "2025-08-01"
                )
                df = pd.DataFrame({
                    "close": [data_df.get(asset, {}).get("close", 150.0)],
                    "rsi_14": [data_df.get(asset, {}).get("rsi_14", 65.0)],
                    "volume": [data_df.get(asset, {}).get("volume", 200000)]
                })
                shap_plot, backtest_plot = self.explainability.explain_trade(asset, None, df)

            markdown = f"""
            **Action:** {data['response']['action'].upper()}
            **Reason:** {data['response']['reason']}
            - Confidence: {data['response']['confidence']*100:.1f}%
            - Backtest: {data['response']['supporting_data']['backtest_performance']}
            - Sentiment Δ: {data['response']['supporting_data']['sentiment_change']}
            """
            return markdown, shap_plot, backtest_plot
        except Exception as e:
            return f"Error: {str(e)}", None, None

    def submit_feedback(self, query: str, feedback: str):
        """Submit user feedback to FastAPI."""
        try:
            response = requests.post(
                "http://localhost:8000/feedback",
                json={"query": query, "feedback": feedback}
            )
            response.raise_for_status()
            return "Feedback submitted successfully!"
        except Exception as e:
            return f"Feedback submission failed: {str(e)}"

    def launch(self, **kwargs):
        """Launch the Gradio app."""
        self.demo.launch(**kwargs)