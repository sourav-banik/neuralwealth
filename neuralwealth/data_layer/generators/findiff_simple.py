import numpy as np
from typing import Tuple
import torch
import torch.nn as nn
from tqdm import tqdm

class SimpleFinDiff(nn.Module):
    """Simplified Financial Diffusion Model for crash scenario generation"""
    
    def __init__(
        self, 
        input_dim: int = 10,  # Number of features
        hidden_dim: int = 64,
        num_timesteps: int = 1000,
        device: str = "cuda" if torch.cuda.is_available() else "cpu"
    ):
        super().__init__()
        self.num_timesteps = num_timesteps
        self.device = device
        
        # Noise scheduling (linear)
        self.beta = torch.linspace(1e-4, 0.02, num_timesteps).to(device)
        self.alpha = 1. - self.beta
        self.alpha_bar = torch.cumprod(self.alpha, dim=0)
        
        # Simple UNet for diffusion
        self.time_embed = nn.Linear(1, hidden_dim)
        self.input_proj = nn.Linear(input_dim, hidden_dim)
        
        self.mid_layers = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU()
        )
        
        self.output_layer = nn.Linear(hidden_dim, input_dim)
        
    def forward(
        self, 
        x: torch.Tensor, 
        t: torch.Tensor
    ) -> torch.Tensor:
        """Predict noise added to input x at timestep t"""
        # Time embedding
        t_embed = torch.sin(2 * np.pi * t.float() / self.num_timesteps)
        t_embed = self.time_embed(t_embed.unsqueeze(-1))
        
        # Input projection
        x_embed = self.input_proj(x)
        
        # Combine
        combined = x_embed + t_embed
        hidden = self.mid_layers(combined)
        
        return self.output_layer(hidden)
    
    def add_noise(
        self, 
        x_start: torch.Tensor, 
        t: torch.Tensor
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Add noise to data at timestep t"""
        noise = torch.randn_like(x_start)
        alpha_bar_t = self.alpha_bar[t].view(-1, 1)
        x_noisy = torch.sqrt(alpha_bar_t) * x_start + torch.sqrt(1 - alpha_bar_t) * noise
        return x_noisy, noise
    
    def sample(
        self, 
        num_samples: int, 
        input_dim: int
    ) -> torch.Tensor:
        """Generate samples from noise"""
        x = torch.randn(num_samples, input_dim).to(self.device)
        
        for t in tqdm(range(self.num_timesteps-1, -1, -1), desc="Sampling"):
            t_tensor = torch.full((num_samples,), t, device=self.device)
            predicted_noise = self(x, t_tensor)
            
            alpha_t = self.alpha[t]
            alpha_bar_t = self.alpha_bar[t]
            beta_t = self.beta[t]
            
            if t > 0:
                noise = torch.randn_like(x)
            else:
                noise = torch.zeros_like(x)
                
            x = (1 / torch.sqrt(alpha_t)) * (
                x - (beta_t / torch.sqrt(1 - alpha_bar_t)) * predicted_noise
            ) + torch.sqrt(beta_t) * noise
            
        return x