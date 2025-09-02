import pandas as pd
import numpy as np
import torch
from typing import List, Tuple
import torch.nn as nn
from neuralwealth.data_layer.generators.findiff_simple import SimpleFinDiff

class SimplifiedFinDiffGenerator:
    """Simplified interface for financial diffusion scenario generation"""
    
    def __init__(
        self, 
        features: List[str] = None,
        crash_intensity: float = 0.7
    ):
        self.features = features or ['close', 'volume', 'rsi', 'macd', 'volatility']
        self.crash_intensity = crash_intensity
        self.model = None
        
    def _prepare_training_data(self, 
                             market_data: pd.DataFrame,
                             crash_periods: List[Tuple[str, str]] = None) -> torch.Tensor:
        """Prepare data for training - focus on crash periods"""
        if crash_periods is None:
            # Default crash periods: 2008, 2020
            crash_periods = [
                ('2007-10-01', '2009-03-31'),  # Global Financial Crisis
                ('2020-02-01', '2020-04-30')   # COVID Crash
            ]
        
        crash_data = []
        for start, end in crash_periods:
            period_data = market_data.loc[start:end].copy()
            if not period_data.empty:
                # Normalize and select features
                data_subset = period_data[self.features].dropna()
                if len(data_subset) > 10:  # Minimum samples
                    normalized = (data_subset - data_subset.mean()) / data_subset.std()
                    crash_data.append(normalized.values)
        
        if not crash_data:
            raise ValueError("No crash period data found for training")
            
        return torch.FloatTensor(np.vstack(crash_data))
    
    def train(
        self, 
        market_data: pd.DataFrame,
        epochs: int = 1000,
        batch_size: int = 32
    ):
        """Train the simplified diffusion model"""
        training_data = self._prepare_training_data(market_data)
        num_samples, input_dim = training_data.shape
        
        self.model = SimpleFinDiff(input_dim=input_dim)
        self.model.to(self.model.device)
        
        optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)
        
        print(f"Training FinDiff on {num_samples} crash examples...")
        for epoch in range(epochs):
            # Random batch
            idx = torch.randint(0, num_samples, (batch_size,))
            x_batch = training_data[idx].to(self.model.device)
            
            # Random timesteps
            t = torch.randint(0, self.model.num_timesteps, (batch_size,)).to(self.model.device)
            
            # Add noise and predict
            x_noisy, noise = self.model.add_noise(x_batch, t)
            predicted_noise = self.model(x_noisy, t)
            
            # Loss and update
            loss = nn.MSELoss()(predicted_noise, noise)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            
            if epoch % 100 == 0:
                print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    def generate_crash_scenario(
        self, 
        current_market_state: pd.DataFrame,
        num_scenarios: int = 5
    ) -> List[pd.DataFrame]:
        """Generate synthetic crash scenarios"""
        if self.model is None:
            raise ValueError("Model must be trained first")
        
        # Get current state statistics for denormalization
        current_stats = {
            'mean': current_market_state[self.features].mean(),
            'std': current_market_state[self.features].std()
        }
        
        # Normalize current state
        current_normalized = (current_market_state[self.features] - current_stats['mean']) / current_stats['std']
        current_tensor = torch.FloatTensor(current_normalized.values[-1:]).to(self.model.device)
        
        # Generate scenarios
        generated_scenarios = []
        for i in range(num_scenarios):
            # Start from current market state + some noise
            x_start = current_tensor + 0.1 * torch.randn_like(current_tensor)
            
            # Diffuse to create crash scenario
            with torch.no_grad():
                crash_scenario = self.model.sample(1, self.model.input_proj.in_features)
            
            # Denormalize
            crash_denorm = crash_scenario.cpu().numpy() * current_stats['std'].values + current_stats['mean'].values
            
            # Create DataFrame with crash pattern
            scenario_df = current_market_state.copy()
            crash_duration = min(30, len(scenario_df))  # 30-day crash
            
            # Apply crash to most recent period
            crash_period = scenario_df.iloc[-crash_duration:].copy()
            for j, feature in enumerate(self.features):
                if feature in crash_period.columns:
                    # Blend generated crash with actual data
                    blend_factor = np.linspace(0, self.crash_intensity, crash_duration)
                    crash_values = crash_denorm[0, j] * np.ones(crash_duration)
                    crash_period[feature] = (1 - blend_factor) * crash_period[feature] + blend_factor * crash_values
            
            scenario_df.iloc[-crash_duration:] = crash_period
            scenario_df['scenario_id'] = f"findiff_crash_{i}"
            scenario_df['is_synthetic'] = True
            
            generated_scenarios.append(scenario_df)
        
        return generated_scenarios