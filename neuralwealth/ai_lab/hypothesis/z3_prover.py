from z3 import *
from typing import Dict, List
import re

class Z3TheoremProver:
    """Simplified Z3 theorem prover for investment hypothesis validation"""
    
    def __init__(self):
        self.solver = Solver()
        self.variables = {}
        
    def _parse_constraints(self, hypothesis: Dict) -> List[str]:
        """Extract logical constraints from hypothesis"""
        constraints = []
        
        # Extract from strategy rules
        if 'strategy' in hypothesis:
            strategy = hypothesis['strategy']
            if 'buy_conditions' in strategy:
                constraints.extend(strategy['buy_conditions'])
            if 'sell_conditions' in strategy:
                constraints.extend(strategy['sell_conditions'])
            if 'conditions' in strategy and isinstance(strategy['conditions'], list):
                constraints.extend(strategy['conditions'])
        
        return constraints
    
    def _create_z3_variables(self, constraints: List[str]) -> None:
        """Create Z3 variables from constraint expressions"""
        variable_pattern = r'data\.([a-zA-Z_][a-zA-Z0-9_]*)\[\-?\d+\]'
        
        for constraint in constraints:
            variables = re.findall(variable_pattern, constraint)
            for var in variables:
                if var not in self.variables:
                    self.variables[var] = Real(var)
    
    def _convert_to_z3_expr(self, constraint: str) -> BoolRef:
        """Convert natural language or structured constraint to Z3 expression"""
        # Replace indexed data references (e.g., data.close_z_score[0]) with variable names
        def replace_indexed_var(match):
            var_name = match.group(1)
            if var_name not in self.variables:
                self.variables[var_name] = Real(var_name)
            return var_name
        
        constraint = re.sub(r'data\.([a-zA-Z_][a-zA-Z0-9_]*)\[\-?\d+\]', replace_indexed_var, constraint)
        
        # Handle indicators (e.g., indicators['rsi'].lines[0])
        constraint = re.sub(r"indicators\['[a-zA-Z_]+'\]\.lines(?:\.[a-zA-Z_]+)?\[\-?\d+\]", replace_indexed_var, constraint)
        
        # Replace logical operators with Z3-compatible syntax
        constraint = constraint.replace(' and ', ' And ')
        constraint = constraint.replace(' or ', ' Or ')
        constraint = constraint.replace('<=', ' <= ')
        constraint = constraint.replace('>=', ' >= ')
        constraint = constraint.replace('<', ' < ')
        constraint = constraint.replace('>', ' > ')
        constraint = constraint.replace('==', ' == ')
        
        # Split into tokens for parsing
        tokens = constraint.split()
        if not tokens:
            return BoolVal(True)
        
        def parse_expression(tokens, pos=0):
            """Recursively parse tokens into Z3 expression"""
            if pos >= len(tokens):
                return None, pos
            
            result = None
            while pos < len(tokens):
                token = tokens[pos]
                
                if token == 'And':
                    left, new_pos = parse_expression(tokens, pos + 1)
                    right, new_pos = parse_expression(tokens, new_pos)
                    if left is not None and right is not None:
                        result = And(left, right)
                    pos = new_pos
                elif token == 'Or':
                    left, new_pos = parse_expression(tokens, pos + 1)
                    right, new_pos = parse_expression(tokens, new_pos)
                    if left is not None and right is not None:
                        result = Or(left, right)
                    pos = new_pos
                elif token in ['<=', '>=', '<', '>', '==']:
                    left_var = tokens[pos - 1]
                    right_val = tokens[pos + 1]
                    try:
                        right_num = float(right_val)
                        left_z3 = self.variables.get(left_var, Real(left_var))
                        if token == '<=':
                            result = left_z3 <= right_num
                        elif token == '>=':
                            result = left_z3 >= right_num
                        elif token == '<':
                            result = left_z3 < right_num
                        elif token == '>':
                            result = left_z3 > right_num
                        elif token == '==':
                            result = left_z3 == right_num
                    except (ValueError, KeyError):
                        return None, pos + 2
                    pos += 2
                else:
                    pos += 1
                    continue
                
                return result, pos
            
            return result, pos
        
        expr, _ = parse_expression(tokens)
        return expr if expr is not None else BoolVal(True)
    
    def check_consistency(self, hypothesis: Dict) -> Dict:
        """Check logical consistency of investment hypothesis"""
        self.solver.reset()
        self.variables = {}
        
        constraints = self._parse_constraints(hypothesis)
        self._create_z3_variables(constraints)
        
        # Add constraints to solver
        for constraint in constraints:
            try:
                z3_expr = self._convert_to_z3_expr(constraint)
                self.solver.add(z3_expr)
            except Exception as e:
                print(f"Warning: Could not parse constraint '{constraint}': {e}")
                continue
        
        # Check consistency
        result = self.solver.check()
        
        return {
            "is_consistent": result == sat,
            "constraints_checked": constraints,
            "variables": list(self.variables.keys()),
            "counterexample": self._get_counterexample() if result == unsat else None
        }
    
    def _get_counterexample(self) -> Dict:
        """Get counterexample for inconsistent constraints"""
        if self.solver.check() == unsat:
            try:
                core = self.solver.unsat_core()
                return {"conflicting_constraints": [str(c) for c in core]}
            except:
                return {"message": "Constraints are inconsistent"}
        return None
    
    def validate_feasibility(self, hypothesis: Dict, market_conditions: Dict) -> bool:
        """Check if hypothesis is feasible given current market conditions"""
        self.solver.reset()
        self.variables = {}
        
        constraints = self._parse_constraints(hypothesis)
        self._create_z3_variables(constraints)
        
        # Add hypothesis constraints
        for constraint in constraints:
            try:
                z3_expr = self._convert_to_z3_expr(constraint)
                self.solver.add(z3_expr)
            except Exception as e:
                print(f"Warning: Could not parse constraint '{constraint}': {e}")
                continue
        
        # Add market condition constraints
        for var, value in market_conditions.items():
            if var in self.variables:
                self.solver.add(self.variables[var] == value)
        
        return self.solver.check() == sat