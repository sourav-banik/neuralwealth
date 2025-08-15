from fastapi import WebSocket
import json
from datetime import datetime
from neuralwealth.ui.utils.portfolio_interface import PortfolioInterface
from typing import Dict

class WebSocketManager:
    """Manages WebSocket connections for real-time portfolio updates."""

    def __init__(self, portfolio_interface: PortfolioInterface):
        """
        Initialize WebSocket manager.

        Args:
            portfolio_interface: Interface for portfolio management.
        """
        self.portfolio_interface = portfolio_interface
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        """Accept a WebSocket connection."""
        try:
            await websocket.accept()
            self.active_connections.append(websocket)
            # Send initial portfolio state
            await self.broadcast({"portfolio": self.portfolio_interface.get_portfolio()})
        except Exception:
            pass

    async def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        try:
            self.active_connections.remove(websocket)
        except Exception:
            pass

    async def broadcast(self, message: Dict):
        """Broadcast portfolio updates to all connections."""
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(json.dumps(message))
            except Exception:
                self.active_connections.remove(connection)

    async def notify_rebalance(self, rebalance_result: Dict):
        """Notify clients of rebalance results."""
        try:
            portfolio = self.portfolio_interface.get_portfolio()
            message = {
                "event": "rebalance",
                "status": rebalance_result.get("status", "unknown"),
                "portfolio": portfolio,
                "timestamp": datetime.now().isoformat()
            }
            await self.broadcast(message)
        except Exception:
            pass