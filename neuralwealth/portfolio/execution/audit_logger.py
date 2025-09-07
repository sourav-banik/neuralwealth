import json
from datetime import datetime
from typing import Dict, List
import os

class AuditLogger:
    """Handles audit logging for portfolio operations"""
    
    def __init__(self, log_dir: str = "logs/portfolio"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f"audit_{datetime.now().strftime('%Y%m%d')}.jsonl")
    
    def log_rebalance(self, 
                     current_weights: Dict, 
                     target_weights: Dict, 
                     execution_results: Dict,
                     constraints: Dict) -> str:
        """Log a portfolio rebalance operation"""
        log_entry = {
            'audit_id': f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'timestamp': datetime.now().isoformat(),
            'current_weights': current_weights,
            'target_weights': target_weights,
            'execution_results': execution_results,
            'constraints': constraints,
            'type': 'rebalance'
        }
        
        self._write_log_entry(log_entry)
        return log_entry['audit_id']
    
    def log_trade(self, order: Dict, result: Dict):
        """Log individual trade execution"""
        log_entry = {
            'audit_id': f"trade_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
            'timestamp': datetime.now().isoformat(),
            'order': order,
            'result': result,
            'type': 'trade'
        }
        
        self._write_log_entry(log_entry)
    
    def _write_log_entry(self, log_entry: Dict):
        """Write log entry to JSONL file"""
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"Failed to write audit log: {e}")
    
    def get_logs(self, log_type: str = None, limit: int = 100) -> List[Dict]:
        """Retrieve audit logs with optional filtering"""
        logs = []
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    if len(logs) >= limit:
                        break
                    log_entry = json.loads(line.strip())
                    if log_type is None or log_entry.get('type') == log_type:
                        logs.append(log_entry)
        except FileNotFoundError:
            pass
        return logs