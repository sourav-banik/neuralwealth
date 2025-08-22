import uvicorn
import threading
import signal
import sys
from neuralwealth.ui.api.fastapi_app import FastAPIApp
from neuralwealth.ui.chat.gradio_app import GradioApp
from neuralwealth.ui.dashboard.plotly_dash import PlotlyDashApp
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface

class UIOrchestrator:
    """Orchestrates FastAPI, Gradio, and Dash for NeuralWealth UI."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize UI components.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.portfolio_interface = portfolio_interface
        self.fastapi_app = FastAPIApp(portfolio_interface)
        self.gradio_app = GradioApp(portfolio_interface)
        self.dash_app = PlotlyDashApp(portfolio_interface)
        self.shutdown_event = threading.Event()

    def run(self):
        """Run FastAPI, Gradio, and Dash in separate threads."""
        try:
            # FastAPI thread
            api_thread = threading.Thread(
                target=uvicorn.run,
                args=(self.fastapi_app.app,),
                kwargs={"host": "0.0.0.0", "port": 8000},
                daemon=True
            )
            api_thread.start()

            # Gradio thread
            gradio_thread = threading.Thread(
                target=self.gradio_app.launch,
                args=(),
                kwargs={"server_name": "0.0.0.0", "server_port": 7860},
                daemon=True
            )
            gradio_thread.start()

            # Dash thread
            dash_thread = threading.Thread(
                target=self.dash_app.run,
                args=(),
                kwargs={"host": "0.0.0.0", "port": 8050},
                daemon=True
            )
            dash_thread.start()

            # Handle shutdown
            def signal_handler(sig, frame):
                self.shutdown_event.set()
                print("Shutting down UI servers...")
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            self.shutdown_event.wait()

        except Exception as e:
            print(f"UIOrchestrator failed: {str(e)}")
            self.shutdown_event.set()