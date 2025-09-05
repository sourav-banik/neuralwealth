from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseAgent(ABC):
    """Base class for all AI agents"""
    
    def __init__(self, agent_id: str, config: Dict[str, Any]):
        self.agent_id = agent_id
        self.config = config
        self.memory = []
        
    @abstractmethod
    def execute(self, input_data: Any) -> Any:
        """Execute agent's primary function"""
        pass
    
    def communicate(self, message: Dict, recipient: 'BaseAgent') -> Dict:
        """Simple inter-agent communication"""
        return recipient.receive(message)
    
    def receive(self, message: Dict) -> Dict:
        """Receive and process messages from other agents"""
        return {"status": "received", "agent": self.agent_id}