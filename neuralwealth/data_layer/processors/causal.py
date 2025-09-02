import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from cdt.causality.graph import PC
from causalnex.structure import StructureModel
from scipy.stats import pearsonr

class CausalAnalyzer:
    def __init__(self, alpha: float = 0.05, min_effect: float = 0.3, max_lag: int = 5, p_value_threshold: float = 0.05):
        """Initialize the CausalAnalyzer with parameters for causal discovery.
        
        Args:
            alpha: Significance threshold for PC algorithm's conditional independence tests.
            min_effect: Minimum absolute correlation for lagged relationships.
            max_lag: Maximum time lag for lead-lag relationships.
            p_value_threshold: Significance threshold for lagged correlations.
        """
        self.alpha = alpha
        self.min_effect = min_effect
        self.max_lag = max_lag
        self.p_value_threshold = p_value_threshold

    def _preprocess_data(self, market_data: pd.DataFrame, macro_data: pd.DataFrame) -> pd.DataFrame:
        """Merge and preprocess market and macro data for causal analysis.
        
        Args:
            market_data: DataFrame with market data (e.g., open, high, low).
            macro_data: DataFrame with macroeconomic data (e.g., retail_inventories).
        
        Returns:
            Processed DataFrame with aligned timestamps and handled missing values.
        """
        # Ensure datetime index and remove timezone
        market_data = market_data.copy()
        macro_data = macro_data.copy()
        market_data.index = pd.to_datetime(market_data.index).tz_localize(None).floor('D')
        macro_data.index = pd.to_datetime(macro_data.index).tz_localize(None).floor('D')
        
        # Merge data
        merged = market_data.join(macro_data, how='inner')
        
        # Drop columns with all NaN values
        merged = merged.dropna(axis=1, how='all')
        
        # Forward fill, then mean imputation for remaining NaNs
        merged = merged.fillna(method='ffill').fillna(merged.mean(numeric_only=True))
        
        # Remove columns with zero or near-zero variance
        merged = merged.loc[:, merged.var(numeric_only=True) > 1e-10]
        
        # Remove duplicate rows
        merged = merged.drop_duplicates()
        
        # Remove highly collinear columns (correlation > 0.95)
        corr_matrix = merged.corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
        merged = merged.drop(columns=to_drop)
        
        # Ensure sufficient data
        if merged.empty or len(merged) < 10 or len(merged.columns) < 2:
            raise ValueError(f"Processed data is insufficient: {len(merged)} rows, {len(merged.columns)} columns")
        
        return merged

    def _find_instantaneous_relationships(self, data: pd.DataFrame) -> StructureModel:
        """Discover contemporaneous causal links using the PC algorithm.
        
        Args:
            data: Preprocessed DataFrame.
        
        Returns:
            StructureModel with inferred causal edges.
        """
        try:
            pc = PC(alpha=self.alpha) 
            graph = pc.predict(data)
            sm = StructureModel()
            sm.add_edges_from(graph.edges())
            return sm
        except Exception as e:
            print(f"Error running PC algorithm: {e}")
            return StructureModel()  # Return empty model on failure

    def _find_lagged_relationships(self, data: pd.DataFrame) -> List[Tuple[str, str, int, float]]:
        """Identify lead-lag effects using cross-correlation with significance testing.
        
        Args:
            data: Preprocessed DataFrame.
        
        Returns:
            List of tuples (driver, target, lag, correlation) for significant lagged effects.
        """
        relationships = []
        cols = data.columns
        
        for target in cols:
            for driver in cols:
                if target == driver:
                    continue
                for lag in range(1, self.max_lag + 1):
                    shifted = data[driver].shift(lag)
                    if shifted.isna().all():
                        continue
                    # Compute correlation and p-value
                    valid_idx = data[target].notna() & shifted.notna()
                    if valid_idx.sum() < 10:  # Require at least 10 valid points
                        continue
                    corr, p_value = pearsonr(data[target][valid_idx], shifted[valid_idx])
                    if abs(corr) >= self.min_effect and p_value < self.p_value_threshold:
                        relationships.append((driver, target, lag, corr))
        
        # Sort by absolute correlation and keep only the strongest per target-driver pair
        relationships.sort(key=lambda x: abs(x[3]), reverse=True)
        unique_relationships = {}
        for rel in relationships:
            key = (rel[0], rel[1])  # (driver, target)
            if key not in unique_relationships or abs(rel[3]) > abs(unique_relationships[key][3]):
                unique_relationships[key] = rel
        
        return list(unique_relationships.values())

    def learn_causal_graph(self, market_data: pd.DataFrame, macro_data: pd.DataFrame) -> Dict:
        """Run the causal analysis pipeline.
        
        Args:
            market_data: DataFrame with market data.
            macro_data: DataFrame with macroeconomic data.
        
        Returns:
            Dictionary containing the structure model and lagged effects.
        """
        # Preprocess data
        processed_data = self._preprocess_data(market_data, macro_data)
        
        # Find instantaneous relationships
        sm = self._find_instantaneous_relationships(processed_data)
        
        # Find lagged relationships
        lagged = self._find_lagged_relationships(processed_data)
        
        return {
            "structure_model": sm,
            "lagged_effects": lagged,
            "processed_data": processed_data
        }

    def explain_relationships(self, graph: Dict) -> pd.DataFrame:
        """Generate human-readable explanations of causal relationships.
        
        Args:
            graph: Dictionary containing structure_model and lagged_effects.
        
        Returns:
            DataFrame with relationship details.
        """
        explanations = []
        
        # Instantaneous effects
        for edge in graph["structure_model"].edges:
            from_node, to_node = edge
            explanations.append({
                "relationship_type": "Instantaneous",
                "driver_variable": from_node,
                "target_variable": to_node,
                "effect_size": 0.0,  # PC algorithm doesn't provide effect sizes
                "lag": 0,
                "confidence": float(1 - self.alpha)
            })
        
        # Lagged effects
        for driver, target, lag, corr in graph["lagged_effects"]:
            explanations.append({
                "relationship_type": "Lagged",
                "driver_variable": driver,
                "target_variable": target,
                "effect_size": float(round(corr, 3)),
                "lag": int(lag),
                "confidence": 0.0
            })
        
        return pd.DataFrame(explanations)