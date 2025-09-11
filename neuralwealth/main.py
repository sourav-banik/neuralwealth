#!/usr/bin/env python3
"""
NeuralWealth - AI-Powered Financial Advisor
Usage:
  main.py run [--live] [--debug]
  main.py backtest <strategy_id>
  main.py (-h | --help)
"""
import logging
from docopt import docopt
from typing import Dict, Any
import pandas as pd
import numpy as np

# Module imports
from neuralwealth.data_layer.data_pipeline import DataPipeline
from neuralwealth.ai_lab.research_loop import RobotScientist
from neuralwealth.portfolio.portfolio_manager import PortfolioManager
from neuralwealth.ui.ui_orchestrator import UIOrchestrator

from neuralwealth.env import data_pipeline_env, ai_lab_env, portfolio_env

class NeuralWealth:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.setup_logging()
        
        # Initialize modules
        self.data_pipeline = DataPipeline(data_pipeline_env)
        self.robot_scientist = RobotScientist(ai_lab_env)
        self.portfolio_manager = PortfolioManager(portfolio_env)
        self.ui = UIOrchestrator()

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

            # 2. AI research cycle
            strategies = self.robot_scientist.run_research_cycle(self.config["ai_lab"])


            # 3. Portfolio rebalancing
            # Load historical returns for CVaR optimization
            historical_returns = pd.read_csv('historical_returns.csv', index_col=0)

            result = self.portfolio_manager.rebalance_portfolio(
                strategies,
                self.config["portfolio"]["market_data"],
                historical_returns=historical_returns
            )

            # Get recommendation info
            recommendation_info = self.portfolio_manager.get_recommendation_info()
            print(f"User model info: {recommendation_info}")

            # Get personalized recommendation separately
            personalized = self.portfolio_manager.recommender.get_personalized_recommendation(self.config["portfolio"]["market_data"], result)
            global_rec = self.portfolio_manager.recommender.get_global_recommendation(self.config["portfolio"]["market_data"])

            print(f"Personalized: {personalized}")
            print(f"Global: {global_rec}")

            # Check federated system status
            from neuralwealth.portfolio.optimization.federated import get_federated_status
            fed_status = get_federated_status()
            print(f"Federated status: {fed_status}")
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
        except Exception as e:
            self.logger.critical(f"Fatal error: {str(e)}", exc_info=True)

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
        "ai_lab": {
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
            "timeframe": "01-01-1990 to 01-01-2025",
            "analysis_focus": "technical and fundamental",
            "constraints": """
                The hypotheses should be ticker specific, not group focused like sector, industry etc. 
                Don't use any other indicator or parameter that are not mentioned in the data schema. 
                The hypotheses should be testable using python backtrader framework.
            """,
            "excluded_assets": [
                # Meme Stocks (mostly NYSE/NASDAQ)
                {"ticker": "GME", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "AMC", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "BBBY", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "KOSS", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "BB", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "NOK", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "EXPR", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "RDBX", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "MULN", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "HYMC", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "SDC", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "WISH", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "CLOV", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "RIDE", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "WKHS", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "SNDL", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "TLRY", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "SPCE", "asset_class": "stock", "market": "NYSE"},
                {"ticker": "TSLA", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "DWAC", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "PHUN", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "BYND", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "COIN", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "HOOD", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "SOFI", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "LCID", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "RIVN", "asset_class": "stock", "market": "NASDAQ"},

                # Crypto/Meme Coins
                {"ticker": "DOGE", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "SHIB", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "PEPE", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "FLOKI", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "BONK", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "MEME", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "WIF", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "BOME", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "TURBO", "asset_class": "crypto", "market": "GLOBAL"},
                {"ticker": "LADYS", "asset_class": "crypto", "market": "GLOBAL"},

                # Other social media-hyped assets
                {"ticker": "CFVI", "asset_class": "stock", "market": "NASDAQ"},
                {"ticker": "APE", "asset_class": "stock", "market": "NYSE"}
            ],
            "rules": {},
            "neo4j": {"uri": "bolt://localhost:7687", "user": "neo4j", "password": "neo4j"}
        },
        "portfolio": {
            "market_data": pd.DataFrame({
                'price': [150.25, 305.50, 2750.75, 3400.20, 710.30],
                'volume': [5000000, 3000000, 1500000, 2000000, 4500000],
                'volatility': [0.018, 0.016, 0.020, 0.025, 0.045],
                'momentum': [0.005, 0.003, 0.007, 0.002, 0.010]
            }, index=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'])
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