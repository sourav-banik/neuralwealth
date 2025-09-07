from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.routing import APIRouter
from typing import Dict, List
import asyncio
import time
import numpy as np
from datetime import datetime
from neuralwealth.ui.ui_orchestrator import ui_orchestrator

# FastAPI app instance
app = FastAPI()

class WebSocketManager:
    """Manager for WebSocket connections and real-time updates"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_data: Dict[WebSocket, Dict] = {}
        
    async def connect(self, websocket: WebSocket, session_id: str):
        """Accept WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)
        self.connection_data[websocket] = {
            'session_id': session_id,
            'connected_at': datetime.now(),
            'last_heartbeat': time.time()
        }
        print(f"WebSocket connected: {session_id}")
        
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            session_id = self.connection_data.get(websocket, {}).get('session_id')
            del self.connection_data[websocket]
            print(f"WebSocket disconnected: {session_id}")
    
    async def broadcast_portfolio_update(self, session_id: str, portfolio_data: Dict):
        """Broadcast portfolio update to specific session"""
        message = {
            'type': 'portfolio_update',
            'timestamp': datetime.now().isoformat(),
            'data': portfolio_data
        }
        
        for websocket, data in list(self.connection_data.items()):
            if data['session_id'] == session_id:
                try:
                    await websocket.send_json(message)
                except Exception as e:
                    print(f"Error sending portfolio update: {e}")
                    self.disconnect(websocket)
    
    async def broadcast_market_data(self, market_data: Dict):
        """Broadcast market data to all connections"""
        message = {
            'type': 'market_data',
            'timestamp': datetime.now().isoformat(),
            'data': market_data
        }
        
        for websocket in list(self.active_connections):
            try:
                await websocket.send_json(message)
            except Exception as e:
                print(f"Error broadcasting market data: {e}")
                self.disconnect(websocket)
    
    async def send_notification(self, session_id: str, notification: Dict):
        """Send notification to specific session"""
        message = {
            'type': 'notification',
            'timestamp': datetime.now().isoformat(),
            'data': notification
        }
        
        for websocket, data in list(self.connection_data.items()):
            if data['session_id'] == session_id:
                try:
                    await websocket.send_json(message)
                except Exception as e:
                    print(f"Error sending notification: {e}")
                    self.disconnect(websocket)
    
    async def heartbeat(self):
        """Send periodic heartbeats to keep connections alive"""
        while True:
            await asyncio.sleep(30)  # Every 30 seconds
            current_time = time.time()
            message = {
                'type': 'heartbeat',
                'timestamp': datetime.now().isoformat()
            }
            
            # Clean up stale connections
            for websocket in list(self.active_connections):
                data = self.connection_data.get(websocket, {})
                if current_time - data.get('last_heartbeat', 0) > 60:  # 60 second timeout
                    self.disconnect(websocket)
                else:
                    try:
                        await websocket.send_json(message)
                    except Exception as e:
                        print(f"Error sending heartbeat: {e}")
                        self.disconnect(websocket)

# Global WebSocket manager
websocket_manager = WebSocketManager()

# FastAPI WebSocket endpoint
router = APIRouter()

@router.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    """WebSocket endpoint for real-time updates"""
    await websocket_manager.connect(websocket, session_id)
    
    try:
        while True:
            # Wait for messages (heartbeat handling)
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=60.0)
                if data.get('type') == 'heartbeat':
                    websocket_manager.connection_data[websocket]['last_heartbeat'] = time.time()
            except asyncio.TimeoutError:
                # Update heartbeat timestamp
                websocket_manager.connection_data[websocket]['last_heartbeat'] = time.time()
                continue
            
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)

# Background task for market data simulation
async def simulate_market_data():
    """Simulate market data updates (replace with real data source)"""
    while True:
        await asyncio.sleep(5)  # Update every 5 seconds
        market_data = {
            'SPY': {'price': 450 + np.random.normal(0, 0.5), 'change': np.random.normal(0, 0.002)},
            'QQQ': {'price': 380 + np.random.normal(0, 0.6), 'change': np.random.normal(0, 0.003)},
            'AAPL': {'price': 150 + np.random.normal(0, 0.3), 'change': np.random.normal(0, 0.004)},
            'MSFT': {'price': 300 + np.random.normal(0, 0.4), 'change': np.random.normal(0, 0.0035)}
        }
        
        await websocket_manager.broadcast_market_data(market_data)

# Start background tasks on FastAPI startup
@app.on_event("startup")
async def startup_event():
    """Start background tasks when the FastAPI application starts."""
    try:
        asyncio.create_task(websocket_manager.heartbeat())
        asyncio.create_task(simulate_market_data())
        print("Started WebSocket heartbeat and market data simulation tasks")
    except Exception as e:
        print(f"Failed to start background tasks: {e}")

# Include the router in the FastAPI app
app.include_router(router)