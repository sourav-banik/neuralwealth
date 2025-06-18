import gym
from gym import spaces
import yfinance as yf
import numpy as np
from transformers import pipeline
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv
import uuid
import matplotlib.pyplot as plt

symbol = "GME"

# Data Layer: Fetch and preprocess data
def fetch_data(ticker, start="2015-01-01", end="2025-01-01"):
    df = yf.download(ticker, start=start, end=end)
    df['Returns'] = df['Close'].pct_change()
    df['Volatility'] = df['Returns'].rolling(window=20).std()
    df['Sentiment'] = np.random.uniform(0, 1, len(df))
    return df.dropna()

# RL Environment for Backtesting
class TradingEnv(gym.Env):
    def __init__(self, data):
        super(TradingEnv, self).__init__()
        self.data = data
        self.current_step = 0
        self.cash = 10000
        self.holdings = 0
        self.max_steps = len(data) - 1

        # Define action space: 0 = Hold, 1 = Buy, 2 = Sell
        self.action_space = spaces.Discrete(3)

        # Define observation space: [Close, Returns, Volatility, Sentiment, Cash, Holdings]
        self.observation_space = spaces.Box(
            low=np.array([0, -np.inf, 0, 0, 0, 0], dtype=np.float32),
            high=np.array([np.inf, np.inf, np.inf, 1, np.inf, np.inf], dtype=np.float32),
            shape=(6,),
            dtype=np.float32
        )

    def reset(self):
        self.current_step = 0
        self.cash = 10000
        self.holdings = 0
        return self._get_state()  # Return observation as NumPy array

    def _get_state(self):
        return np.array([
            self.data['Close'].iloc[self.current_step].item(),  
            self.data['Returns'].iloc[self.current_step].item(),  
            self.data['Volatility'].iloc[self.current_step].item(),  
            self.data['Sentiment'].iloc[self.current_step].item(),  
            float(self.cash),
            float(self.holdings)
        ], dtype=np.float32)

    def step(self, action):
        price = self.data['Close'].iloc[self.current_step].item()  
        if action == 1 and self.cash >= price:  # Buy
            self.holdings += 1
            self.cash -= price
        elif action == 2 and self.holdings > 0:  # Sell
            self.holdings -= 1
            self.cash += price

        self.current_step += 1
        done = self.current_step >= self.max_steps
        portfolio_value = self.cash + self.holdings * price
        returns = (portfolio_value - 10000) / 10000
        volatility = self.data['Returns'].iloc[:self.current_step].std() if self.current_step > 1 else 1
        reward = returns / (volatility + 1e-6)  # Sharpe ratio approximation
        return self._get_state(), reward, done, {}

    def render(self, mode="human"):
        pass  # Optional: Implement if you want to visualize the environment

# Robot Scientist: Hypothesis Generator
def generate_hypothesis(data):
    classifier = pipeline("sentiment-analysis", model="distilbert-base-uncased")
    latest_data = data.iloc[-1]
    price_trend = latest_data['Returns'].item() > 0
    hypothesis = f"Buy {symbol} if sentiment > 0.7 and price trend is {'positive' if price_trend else 'negative'}"
    return {
        "id": str(uuid.uuid4()),
        "text": hypothesis,
        "params": {"sentiment_threshold": 0.7, "trend": "positive" if price_trend else "negative"},
        "confidence": classifier(hypothesis)[0]['score']
    }

# Robot Scientist: Backtesting Engine
def backtest_hypothesis(hypothesis, data):
    env = DummyVecEnv([lambda: TradingEnv(data)])
    model = PPO("MlpPolicy", env, verbose=0)
    model.learn(total_timesteps=1000)
    
    obs = env.reset()
    total_reward = 0
    portfolio_values = []
    cash = 10000
    holdings = 0
    
    for _ in range(len(data) - 1):
        action, _ = model.predict(obs)
        obs, reward, done, _ = env.step(action)
        total_reward += reward[0]
        
        # Calculate portfolio value
        price = data['Close'].iloc[env.envs[0].current_step].item()
        portfolio_value = cash + holdings * price
        portfolio_values.append(portfolio_value)
        
        # Update cash and holdings based on action
        if action == 1 and cash >= price:
            holdings += 1
            cash -= price
        elif action == 2 and holdings > 0:
            holdings -= 1
            cash += price
            
        if done:
            break
    
    sharpe_ratio = total_reward / (data['Returns'].std() + 1e-6)
    
    # Plot portfolio value over time using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.plot(range(len(portfolio_values)), portfolio_values, color='green', label='Portfolio Value ($)')
    plt.xlabel('Time Step')
    plt.ylabel('Portfolio Value ($)')
    plt.title('Portfolio Value Over Time')
    plt.legend()
    plt.grid(True)
    plt.show()
    
    return {
        "hypothesis_id": hypothesis["id"],
        "sharpe_ratio": sharpe_ratio,
        "success": sharpe_ratio > 1.5
    }

# Robot Scientist: Auto-Experiment Designer
def refine_hypothesis(hypothesis, backtest_result):
    if not backtest_result["success"]:
        new_threshold = hypothesis["params"]["sentiment_threshold"] + np.random.uniform(-0.1, 0.1)
        new_threshold = max(0.1, min(0.9, new_threshold))
        new_hypothesis = {
            "id": str(uuid.uuid4()),
            "text": f"Buy {symbol} if sentiment > {new_threshold:.2f} and price trend is {hypothesis['params']['trend']}",
            "params": {"sentiment_threshold": new_threshold, "trend": hypothesis["params"]["trend"]},
            "confidence": hypothesis["confidence"]
        }
        return new_hypothesis
    return None

# Robot Scientist: Knowledge Graph (simplified as dictionary)
knowledge_graph = {}

def store_strategy(backtest_result, hypothesis):
    if backtest_result["success"]:
        knowledge_graph[hypothesis["id"]] = {
            "text": hypothesis["text"],
            "sharpe_ratio": backtest_result["sharpe_ratio"],
            "params": hypothesis["params"]
        }

# User Interface: Command-line interaction
def user_interface(data):
    hypothesis = generate_hypothesis(data)
    print(f"Generated Hypothesis: {hypothesis['text']} (Confidence: {hypothesis['confidence']:.2f})")
    
    backtest_result = backtest_hypothesis(hypothesis, data)
    print(f"Backtest Result: Sharpe Ratio = {backtest_result['sharpe_ratio']:.2f}, Success = {backtest_result['success']}")
    
    if backtest_result["success"]:
        store_strategy(backtest_result, hypothesis)
        print(f"Stored Strategy: {hypothesis['text']}")
    else:
        new_hypothesis = refine_hypothesis(hypothesis, backtest_result)
        if new_hypothesis:
            print(f"Refined Hypothesis: {new_hypothesis['text']}")
    
    if knowledge_graph:
        print("\nValidated Strategies in Knowledge Graph:")
        for strategy_id, strategy in knowledge_graph.items():
            print(f"ID: {strategy_id}, Strategy: {strategy['text']}, Sharpe: {strategy['sharpe_ratio']:.2f}")

# Main execution
if __name__ == "__main__":
    data = fetch_data(symbol)
    user_interface(data)