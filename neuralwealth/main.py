#!/usr/bin/env python3
"""
NeuralWealth - AI-Powered Financial Advisor
Usage:
  main.py run [--live] [--debug]
  main.py (-h | --help)
"""
import logging
from docopt import docopt
from typing import Dict, Any

# Module imports
from neuralwealth.data_layer.data_pipeline import DataPipeline

from neuralwealth.env import data_pipeline_env

class NeuralWealth:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.setup_logging()
        
        # Initialize modules
        self.data_pipeline = DataPipeline(data_pipeline_env)

    def setup_logging(self):
        """Configure structured logging."""
        logging.basicConfig(
            level=logging.DEBUG if self.config["debug"] else logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("neuralwealth.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def run(self, live_mode: bool = False):
        """Main event loop."""
        self.logger.info("Starting NeuralWealth in %s mode", 
                        "LIVE" if live_mode else "PAPER")
        
        try:
            # 1. Data ingestion
            self.data_pipeline.run_pipeline(self.config["data"]["tickers"])
            
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
        except Exception as e:
            self.logger.critical(f"Fatal error: {str(e)}", exc_info=True)

    def backtest(self, strategy_id: str):
        """Backtest a specific strategy from the Knowledge Graph."""
        self.logger.info(f"Backtesting strategy {strategy_id}")
        # Mock: Replace with actual backtest logic
        return {"sharpe": 1.8, "max_drawdown": -0.12}

if __name__ == "__main__":
    args = docopt(__doc__)
    
    # Load config (mock - replace with YAML loader)
    config = {
        "data": {
            "tickers": [
                {"ticker": "MSFT", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "NVDA", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "IRTC", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "VIAV", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "ACLS", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "EURUSD", "asset_class": "forex", "market": "GLOBAL"},
                {"ticker": "GBPUSD", "asset_class": "forex", "market": "GLOBAL"},
                {"ticker": "GSPC", "asset_class": "index", "market": "GLOBAL"},
                {"ticker": "DJI", "asset_class": "index", "market": "GLOBAL"},
                {"ticker": "CL", "asset_class": "future", "market": "GLOBAL"},
                {"ticker": "ES", "asset_class": "future", "market": "GLOBAL"},
            ],
        },
        "ui": {"enabled": True},
        "debug": args["--debug"]
    }
    
    app = NeuralWealth(config)
    
    if args["run"]:
        app.run(live_mode=args["--live"])
    elif args["backtest"]:
        result = app.backtest(args["<strategy_id>"])
        print(f"Backtest result: {result}")