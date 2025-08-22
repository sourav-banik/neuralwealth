from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from datetime import datetime
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface
from typing import Dict, List

class UserQuery(BaseModel):
    text: str

class UserFeedback(BaseModel):
    query: str
    feedback: str

class FastAPIApp:
    """FastAPI application for handling UI requests."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize FastAPI with portfolio interface.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.app = FastAPI()
        self.portfolio_interface = portfolio_interface
        self.setup_cors()
        self.setup_routes()
        self.init_db()

    def setup_cors(self):
        """Configure CORS for the FastAPI app."""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def init_db(self):
        """Initialize SQLite database for trades and feedback."""
        try:
            conn = sqlite3.connect("portfolio.db")
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset TEXT,
                    action TEXT,
                    quantity REAL,
                    price REAL,
                    timestamp TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT,
                    feedback TEXT,
                    timestamp TEXT
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            raise Exception(f"Database initialization failed: {str(e)}")

    def setup_routes(self):
        """Set up FastAPI routes."""
        @self.app.post("/query")
        async def handle_query(query: UserQuery):
            try:
                asset = query.text.split()[-1].upper() if "sell" in query.text.lower() or "buy" in query.text.lower() else None
                conn = sqlite3.connect("portfolio.db")
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM trades WHERE asset=? ORDER BY timestamp DESC LIMIT 1", (asset,) if asset else ("",))
                trade = cursor.fetchone()
                conn.close()

                # Mock market state and hypotheses (replace with InfluxDBClient in production)
                market_state = {
                    "market": {
                        "AAPL": {"close": 150.0, "rsi_14": 65.0, "volume": 200000, "esg_score": 60},
                        "MSFT": {"close": 300.0, "rsi_14": 70.0, "volume": 150000, "esg_score": 55},
                        "EURUSD": {"close": 1.2, "rsi_14": 50.0, "volume": 500000, "esg_score": 0}
                    },
                    "macro": {"fed_funds_rate": 0.02, "cpi_inflation": 0.03}
                }
                hypotheses = [
                    {
                        "hypothesis": {
                            "thesis": f"{'Buy' if 'buy' in query.text.lower() else 'Sell'} {asset or 'assets'} based on RSI",
                            "assets": [{"ticker": asset, "asset_class": "stock", "market": "NASDAQ", "sector": "Tech"}] if asset else [],
                            "confidence": 0.85,
                            "id": "H20250801-1",
                            "expected_return": 0.15
                        },
                        "backtest_results": {
                            asset: {"sharpe": {"sharperatio": 1.5}, "returns": {"rtot": 0.15}, "drawdown": {"maxdrawdown": 6.5}}
                        } if asset else {},
                        "crash_results": {
                            "Dot-Com Bubble": {asset: {"sharpe": {"sharperatio": 0.8}, "returns": {"rtot": -0.02}}} if asset else {}
                        }
                    }
                ]
                result = self.portfolio_interface.rebalance(market_state, hypotheses)

                explanation = {
                    "action": result.get("status", "hold"),
                    "reason": f"{'Target price reached' if result.get('status') == 'success' else result.get('reason', 'No action taken')}",
                    "confidence": hypotheses[0]["hypothesis"]["confidence"],
                    "supporting_data": {
                        "backtest_performance": hypotheses[0]["backtest_results"].get(asset, {}).get("sharpe", {}).get("sharperatio", 0.0) if asset else 0.0,
                        "sentiment_change": -0.2  # Mock
                    }
                }
                return {"response": explanation}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

        @self.app.post("/feedback")
        async def handle_feedback(feedback: UserFeedback):
            try:
                conn = sqlite3.connect("portfolio.db")
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO feedback (query, feedback, timestamp) VALUES (?, ?, ?)",
                    (feedback.query, feedback.feedback, datetime.now().isoformat())
                )
                conn.commit()
                conn.close()

                if feedback.feedback == "too_risky":
                    self.portfolio_interface.train_rl_agent()
                return {"status": "Feedback recorded"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Feedback processing failed: {str(e)}")

        @self.app.get("/portfolio")
        async def get_portfolio():
            try:
                portfolio = self.portfolio_interface.get_portfolio()
                return {"holdings": portfolio}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Portfolio fetch failed: {str(e)}")