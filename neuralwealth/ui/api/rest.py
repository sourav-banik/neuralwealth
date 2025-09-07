from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
from datetime import datetime
from fastapi.responses import JSONResponse
import numpy as np

from neuralwealth.ui.ui_orchestrator import ui_orchestrator

app = FastAPI(title="NeuralWealth UI API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency for session management
def get_session(session_id: str = None):
    """Get or create session"""
    if not session_id:
        session_id = ui_orchestrator.create_session()
    
    session = ui_orchestrator.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    return session_id, session

@app.get("/")
async def root():
    return {"message": "NeuralWealth UI API", "status": "online"}

@app.get("/session/create")
async def create_session():
    """Create a new user session"""
    session_id = ui_orchestrator.create_session()
    return {"session_id": session_id, "message": "Session created"}

@app.get("/portfolio/status")
async def get_portfolio_status(session_id: str = None):
    """Get current portfolio status"""
    _, session = get_session(session_id)
    
    # Placeholder - would connect to actual portfolio manager
    portfolio_data = {
        "total_value": 100000,
        "cash": 25000,
        "positions": {
            "AAPL": {"quantity": 100, "value": 15000},
            "MSFT": {"quantity": 50, "value": 15000},
            "GOOGL": {"quantity": 5, "value": 14000}
        },
        "weights": {"AAPL": 0.15, "MSFT": 0.15, "GOOGL": 0.14, "CASH": 0.25},
        "performance": {"1d": 0.002, "1w": 0.015, "1m": 0.045}
    }
    
    ui_orchestrator.update_session_portfolio(session_id, portfolio_data)
    return portfolio_data

@app.post("/portfolio/rebalance")
async def trigger_rebalancing(strategy_preferences: Dict = None, session_id: str = None):
    """Trigger portfolio rebalancing"""
    _, session = get_session(session_id)
    
    # Placeholder - would connect to actual portfolio manager
    # For now, return a simulated rebalancing result
    result = {
        "status": "success",
        "message": "Rebalancing completed",
        "new_weights": {"AAPL": 0.18, "MSFT": 0.16, "GOOGL": 0.12, "CASH": 0.20},
        "orders_executed": [
            {"asset": "AAPL", "action": "BUY", "quantity": 10},
            {"asset": "MSFT", "action": "BUY", "quantity": 5}
        ],
        "timestamp": datetime.now().isoformat()
    }
    
    return result

@app.get("/strategies")
async def get_available_strategies(session_id: str = None):
    """Get available strategies from knowledge graph"""
    _, session = get_session(session_id)
    
    # Placeholder - would query Neo4j knowledge graph
    strategies = [
        {
            "id": "strat_001",
            "name": "Tech Momentum",
            "description": "Focus on technology stocks with positive momentum",
            "assets": ["AAPL", "MSFT", "GOOGL", "AMZN"],
            "performance": {"sharpe": 1.2, "max_drawdown": -0.15},
            "confidence": 0.85
        },
        {
            "id": "strat_002", 
            "name": "Balanced Growth",
            "description": "Diversified growth portfolio with risk management",
            "assets": ["SPY", "QQQ", "GLD", "TLT"],
            "performance": {"sharpe": 0.9, "max_drawdown": -0.12},
            "confidence": 0.78
        }
    ]
    
    return {"strategies": strategies}

@app.post("/chat/query")
async def chat_query(query: Dict, session_id: str = None):
    """KG-RAG chat interface"""
    session_id, session = get_session(session_id)
    user_message = query.get("message", "")
    
    # Simple pattern-based response (placeholder for actual KG-RAG)
    response = generate_chat_response(user_message)
    
    # Add to chat history
    ui_orchestrator.add_chat_message(session_id, user_message, response)
    
    return {"response": response, "session_id": session_id}

@app.get("/simulation/scenarios")
async def get_crash_scenarios(session_id: str = None):
    """Get available crash scenarios for simulation"""
    _, session = get_session(session_id)
    
    # Placeholder - would list available synthetic scenarios
    scenarios = [
        {
            "id": "crash_2008",
            "name": "2008 Financial Crisis",
            "description": "Global financial crisis scenario",
            "max_drawdown": -0.55,
            "duration_days": 180,
            "recovery_days": 720
        },
        {
            "id": "crash_2020", 
            "name": "2020 COVID Crash",
            "description": "COVID-19 pandemic market crash",
            "max_drawdown": -0.35,
            "duration_days": 33,
            "recovery_days": 180
        },
        {
            "id": "crash_flash",
            "name": "Flash Crash",
            "description": "2010-style flash crash scenario", 
            "max_drawdown": -0.15,
            "duration_days": 1,
            "recovery_days": 7
        }
    ]
    
    return {"scenarios": scenarios}

@app.get("/report/download")
async def download_report(format: str = "html", session_id: str = None):
    """Download portfolio report"""
    _, session = get_session(session_id)
    
    # Generate report based on format
    if format == "json":
        return JSONResponse(content=session.get('portfolio_data', {}))
    elif format == "csv":
        # Convert to CSV
        portfolio_data = session.get('portfolio_data', {})
        csv_data = "Asset,Quantity,Value,Weight\n"
        for asset, pos in portfolio_data.get('positions', {}).items():
            csv_data += f"{asset},{pos['quantity']},{pos['value']},{portfolio_data.get('weights', {}).get(asset, 0)}\n"
        
        return Response(content=csv_data, media_type="text/csv")
    else:
        # HTML report (default)
        from ..web.dash.main import dashboard
        portfolio_data = session.get('portfolio_data', {})
        strategies = []  # Would fetch from knowledge graph
        
        html_content = dashboard.generate_full_dashboard(portfolio_data, strategies, session_id)
        return Response(content=html_content, media_type="text/html")

@app.post("/portfolio/adjust")
async def adjust_portfolio(adjustments: Dict, session_id: str = None):
    """Simulate portfolio adjustments"""
    _, session = get_session(session_id)
    portfolio_data = session.get('portfolio_data', {})
    
    # Simulate impact of adjustments
    result = {
        "original_weights": portfolio_data.get('weights', {}),
        "proposed_weights": adjustments,
        "estimated_impact": simulate_adjustment_impact(portfolio_data, adjustments),
        "risk_assessment": assess_risk_changes(portfolio_data, adjustments)
    }
    
    return result

def simulate_adjustment_impact(portfolio_data: Dict, adjustments: Dict) -> Dict:
    """Simulate impact of portfolio adjustments"""
    # Simplified simulation
    return {
        "expected_return_change": np.random.normal(0, 0.02),
        "volatility_change": np.random.normal(0, 0.01),
        "max_drawdown_change": np.random.normal(0, 0.015)
    }

def assess_risk_changes(portfolio_data: Dict, adjustments: Dict) -> Dict:
    """Assess risk changes from adjustments"""
    return {
        "diversification_score": max(0, min(1, np.random.normal(0.7, 0.1))),
        "liquidity_score": max(0, min(1, np.random.normal(0.8, 0.05))),
        "market_risk": max(0, min(1, np.random.normal(0.6, 0.1)))
    }

def generate_chat_response(message: str) -> str:
    """Simple pattern-based chat response (placeholder for KG-RAG)"""
    message_lower = message.lower()
    
    if "portfolio" in message_lower and "why" in message_lower:
        return "Your portfolio is allocated based on risk-optimized strategies from our AI research. The current weights maximize expected returns while controlling for drawdown risk using CVaR optimization."
    
    elif "crash" in message_lower or "scenario" in message_lower:
        return "I can help you simulate different market crash scenarios. The available scenarios include 2008 Financial Crisis, 2020 COVID Crash, and Flash Crash scenarios."
    
    elif "strategy" in message_lower or "recommend" in message_lower:
        return "Based on your risk profile and market conditions, I recommend a diversified portfolio with technology exposure for growth and bonds for stability."
    
    elif "performance" in message_lower:
        return "Your portfolio has gained 4.5% this month with a Sharpe ratio of 1.1. The maximum drawdown was -8.2% during recent market volatility."
    
    else:
        return "I can help you understand your portfolio allocation, simulate market scenarios, and explain investment strategies. What would you like to know about?"

@app.get("/session/stats")
async def get_session_stats():
    """Get session statistics"""
    return ui_orchestrator.get_session_stats()

if __name__ == "__main__":
    import uvicorn
    from neuralwealth.ui.api.websockets import router as ws_router
    app.include_router(ws_router)
    uvicorn.run(app, host="0.0.0.0", port=8000)