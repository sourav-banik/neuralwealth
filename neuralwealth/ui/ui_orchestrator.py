from typing import Dict, Optional
import logging
from datetime import datetime
import uuid

class UIOrchestrator:
    """Central coordinator for UI components and user sessions"""
    
    def __init__(self):
        self.sessions: Dict[str, Dict] = {}
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup basic logging"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    def create_session(self, user_id: Optional[str] = None) -> str:
        """Create a new user session"""
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            'user_id': user_id or 'anonymous',
            'created_at': datetime.now(),
            'last_activity': datetime.now(),
            'portfolio_data': None,
            'chat_history': []
        }
        self.logger.info(f"Created new session: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session data"""
        session = self.sessions.get(session_id)
        if session:
            session['last_activity'] = datetime.now()
        return session
    
    def update_session_portfolio(self, session_id: str, portfolio_data: Dict):
        """Update portfolio data in session"""
        session = self.get_session(session_id)
        if session:
            session['portfolio_data'] = portfolio_data
            self.logger.info(f"Updated portfolio data for session: {session_id}")
    
    def add_chat_message(self, session_id: str, message: str, response: str):
        """Add message to chat history"""
        session = self.get_session(session_id)
        if session:
            session['chat_history'].append({
                'timestamp': datetime.now(),
                'message': message,
                'response': response
            })
            # Keep only last 50 messages
            session['chat_history'] = session['chat_history'][-50:]
    
    def cleanup_sessions(self, max_age_hours: int = 24):
        """Clean up old sessions"""
        now = datetime.now()
        expired_sessions = []
        
        for session_id, session in self.sessions.items():
            session_age = now - session['created_at']
            if session_age.total_seconds() > max_age_hours * 3600:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self.sessions[session_id]
            self.logger.info(f"Cleaned up expired session: {session_id}")
    
    def get_session_stats(self) -> Dict:
        """Get session statistics"""
        return {
            'total_sessions': len(self.sessions),
            'active_sessions': len([s for s in self.sessions.values() 
                                  if (datetime.now() - s['last_activity']).total_seconds() < 3600])
        }
    
# Create a global instance of UIOrchestrator
ui_orchestrator = UIOrchestrator()