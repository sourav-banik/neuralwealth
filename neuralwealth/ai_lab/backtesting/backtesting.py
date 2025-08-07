from typing import Dict, List, Any
import pandas as pd
import backtrader as bt
from backtrader.analyzers import (
    SharpeRatio, DrawDown, Returns, TradeAnalyzer, AnnualReturn, TimeReturn,
    PositionsValue, PyFolio, VWR, SQN, Calmar, TimeDrawDown, GrossLeverage
)
from neuralwealth.ai_lab.backtesting.dynamic_datafeed import DynamicDataFeed
from neuralwealth.ai_lab.backtesting.bt_indicators import IndicatorFactory

class BacktestEngine:
    """Backtesting engine that runs separate tests for each asset."""

    def __init__(
        self, 
        cash: float = 100000.0, 
        commission: float = 0.001, 
        risk_free_rate: float = 0.01
    ):
        """
        Initialize the backtesting engine with cash, commission, and risk-free rate.

        Args:
            cash: Initial cash for the broker (default: 100000.0).
            commission: Broker commission per trade (default: 0.001).
            risk_free_rate: Risk-free rate for Sharpe ratio calculations (default: 0.01).
        """
        self.cash = cash
        self.commission = commission
        self.risk_free_rate = risk_free_rate

    def run(self, hypothesis: Dict, data: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
        """
        Run separate backtests for each asset.

        Args:
            hypothesis: Strategy hypothesis containing assets and parameters.
            data: Dictionary of {ticker: DataFrame} with market data.

        Returns:
            Dictionary of {ticker: results_dict} for each asset.
        """
        results = {}
        for asset in hypothesis["assets"]:
            ticker = asset["ticker"]
            if ticker not in data:
                continue

            try:
                cerebro = bt.Cerebro()
                df = data[ticker].copy()
                data_feed = DynamicDataFeed(dataframe=df, name=ticker).create_feed()
                cerebro.adddata(data_feed)
                cerebro.addstrategy(self._create_single_asset_strategy(hypothesis, ticker))
                cerebro.addanalyzer(SharpeRatio, _name="sharpe", riskfreerate=self.risk_free_rate)
                cerebro.addanalyzer(DrawDown, _name="drawdown")
                cerebro.addanalyzer(Returns, _name="returns")
                cerebro.addanalyzer(TradeAnalyzer, _name="trade_analyzer")
                cerebro.addanalyzer(AnnualReturn, _name="annual_return")
                cerebro.addanalyzer(TimeReturn, _name="time_return")
                cerebro.addanalyzer(PositionsValue, _name="positions_value")
                cerebro.addanalyzer(PyFolio, _name="pyfolio")
                cerebro.addanalyzer(VWR, _name="vwr")
                cerebro.addanalyzer(SQN, _name="sqn")
                cerebro.addanalyzer(Calmar, _name="calmar")
                cerebro.addanalyzer(TimeDrawDown, _name="time_drawdown")
                cerebro.addanalyzer(GrossLeverage, _name="gross_leverage")
                cerebro.broker.setcash(self.cash)
                cerebro.broker.setcommission(commission=self.commission)
                strat_results = cerebro.run()[0]
                results[ticker] = {
                    "final_value": cerebro.broker.getvalue(),
                    "sharpe": strat_results.analyzers.sharpe.get_analysis(),
                    "drawdown": strat_results.analyzers.drawdown.get_analysis(),
                    "returns": strat_results.analyzers.returns.get_analysis(),
                    "trade_analyzer": strat_results.analyzers.trade_analyzer.get_analysis(),
                    "annual_return": strat_results.analyzers.annual_return.get_analysis(),
                    "time_return": strat_results.analyzers.time_return.get_analysis(),
                    "positions_value": strat_results.analyzers.positions_value.get_analysis(),
                    "pyfolio": strat_results.analyzers.pyfolio.get_analysis(),
                    "vwr": strat_results.analyzers.vwr.get_analysis(),
                    "sqn": strat_results.analyzers.sqn.get_analysis(),
                    "calmar": strat_results.analyzers.calmar.get_analysis(),
                    "time_drawdown": strat_results.analyzers.time_drawdown.get_analysis(),
                    "gross_leverage": strat_results.analyzers.gross_leverage.get_analysis(),
                    "hypothesis_id": hypothesis.get("id", "unknown")
                }
            except Exception as e:
                results[ticker] = {"error": str(e)}

        return results

    def _create_single_asset_strategy(self, hypothesis: Dict, ticker: str) -> bt.Strategy:
        """Create strategy adjusted for single-asset backtesting."""
        strategy_config = hypothesis.get("strategy", {})

        class SingleAssetStrategy(bt.Strategy):
            params = (
                ("ticker", ticker),
                ("buy_conditions", strategy_config.get("buy_conditions", [])),
                ("sell_conditions", strategy_config.get("sell_conditions", [])),
                ("holding_period", int(strategy_config.get("holding_period", 60))),
                ("size", strategy_config.get("size", 100)),
                ("verbose", strategy_config.get("verbose", True))
            )

            def __init__(self):
                self.trade_duration = 0
                self.indicators = self._init_indicators(strategy_config.get("indicators", []))
                self.trades = []

            def _init_indicators(self, indicators_config: List) -> Dict:
                """Initialize indicators for single data feed."""
                indicators = {}
                for indicator in indicators_config:
                    name = indicator["name"].lower()
                    params = indicator.get("params", {})
                    try:
                        indicators[name] = IndicatorFactory.create_indicator(name, params, self.data)
                    except Exception:
                        continue
                return indicators

            def next(self):
                self.trade_duration += 1
                position = self.getposition()
                if position.size > 0:
                    sell_triggered = any(
                        self._evaluate_condition(cond, self.data)
                        for cond in self.params.sell_conditions
                    )
                    if sell_triggered or self.trade_duration >= self.params.holding_period:
                        self.close()
                        self.trade_duration = 0
                elif position.size == 0:
                    buy_triggered = all(
                        self._evaluate_condition(cond, self.data)
                        for cond in self.params.buy_conditions
                    )
                    if buy_triggered:
                        self.buy(size=self.params.size)
                        self.trade_duration = 0

            def notify_trade(self, trade):
                """Record trade details."""
                self.trades.append({
                    'ticker': self.params.ticker,
                    'dt': self.data.datetime.date(0),
                    'size': trade.size,
                    'price': trade.price,
                    'pnl': trade.pnl,
                    'status': 'open' if trade.isopen else 'closed'
                })

            def _evaluate_condition(self, condition: str, data: bt.DataBase) -> bool:
                """Evaluate condition using a safe context."""
                try:
                    context = {
                        'data': data,
                        'indicators': self.indicators,
                        'len': len
                    }
                    return eval(condition, {'__builtins__': None}, context)
                except Exception:
                    return False

        return SingleAssetStrategy