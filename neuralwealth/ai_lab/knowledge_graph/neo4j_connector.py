from neo4j import GraphDatabase
from typing import Dict, Any
import json

class Neo4jConnector:
    """Manages storage of hypotheses, backtest results, and crash results in Neo4j."""

    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Neo4j connector.

        Args:
            uri: Neo4j database URI (e.g., "neo4j://localhost:7687").
            user: Neo4j username.
            password: Neo4j password.
        """
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            raise Exception(f"Failed to connect to Neo4j: {str(e)}")

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def store_strategy(
        self,          
        hypothesis: Dict, 
        backtest_results: Dict[str, Dict[str, Any]], 
        crash_results: Dict[str, Dict[str, Any]]
    ):
        """
        Store hypothesis, backtest results, and crash results in Neo4j.

        Args:
            hypothesis: Hypothesis dictionary with thesis, assets, strategy, etc.
            backtest_results: Dictionary of {ticker: results_dict} from backtesting.
            crash_results: Dictionary of {crash_event: {ticker: results_dict}} from crash scenarios.
        """
        try:
            with self.driver.session() as session:
                session.execute_write(
                    self._create_knowledge_graph, hypothesis, backtest_results, crash_results
                )
        except Exception as e:
            raise Exception(f"Failed to store strategy: {str(e)}")

    @staticmethod
    def _create_knowledge_graph(
        tx, 
        hypothesis: Dict, 
        backtest_results: Dict[str, Dict[str, Any]], 
        crash_results: Dict[str, Dict[str, Any]]
    ):
        """
        Create the complete knowledge graph with all nodes and relationships.

        Args:
            tx: Neo4j transaction object.
            hypothesis: Hypothesis dictionary.
            backtest_results: Backtest results dictionary.
            crash_results: Crash test results dictionary.
        """
        # Create Hypothesis node
        hypothesis_id = hypothesis.get("id", "unknown")
        tx.run("""
        MERGE (h:Hypothesis {id: $id})
        SET h.thesis = $thesis,
            h.trigger = $trigger,
            h.timeframe = $timeframe,
            h.confidence = $confidence,
            h.risks = $risks,
            h.last_updated = $last_updated,
            h.holding_period = $holding_period
        """, 
        id=hypothesis_id,
        thesis=hypothesis.get("thesis", ""),
        trigger=hypothesis.get("trigger", ""),
        timeframe=hypothesis.get("timeframe", ""),
        confidence=hypothesis.get("confidence", 0.0),
        risks=hypothesis.get("risks", ""),
        last_updated=hypothesis.get("last_updated", ""),
        holding_period=hypothesis.get("strategy", {}).get("holding_period", 0))

        # Process assets
        for asset in hypothesis.get("assets", []):
            ticker = asset.get("ticker")
            asset_class = asset.get("asset_class", "")
            market = asset.get("market", "")

            # Create Asset, AssetClass, and Market nodes with relationships
            tx.run("""
            MERGE (a:Asset {ticker: $ticker})
            MERGE (ac:AssetClass {name: $asset_class})
            MERGE (m:Market {name: $market})
            MERGE (a)-[:HAS_CLASS]->(ac)
            MERGE (a)-[:TRADES_ON]->(m)
            MERGE (h:Hypothesis {id: $hypothesis_id})-[:HAS_ASSET]->(a)
            """, 
            ticker=ticker,
            asset_class=asset_class,
            market=market,
            hypothesis_id=hypothesis_id)

        # Process strategy indicators
        for indicator in hypothesis.get("strategy", {}).get("indicators", []):
            indicator_name = indicator.get("name", "")
            params = indicator.get("params", {})
            
            # Create Indicator node
            tx.run("""
            MERGE (i:Indicator {name: $name})
            MERGE (h:Hypothesis {id: $hypothesis_id})-[:USES_INDICATOR]->(i)
            """, 
            name=indicator_name,
            hypothesis_id=hypothesis_id)
            
            # Create parameter nodes
            for param_key, param_value in params.items():
                tx.run("""
                MATCH (i:Indicator {name: $name})
                MERGE (p:IndicatorParam {key: $key, value: $value})
                MERGE (i)-[:HAS_PARAM]->(p)
                """,
                name=indicator_name,
                key=param_key,
                value=param_value)

        # Process conditions
        for condition in hypothesis.get("strategy", {}).get("buy_conditions", []):
            tx.run("""
            MERGE (c:Condition {type: $type, expression: $expression})
            MERGE (h:Hypothesis {id: $hypothesis_id})-[:HAS_BUY_CONDITION]->(c)
            """,
            type="buy",
            expression=condition,
            hypothesis_id=hypothesis_id)

        for condition in hypothesis.get("strategy", {}).get("sell_conditions", []):
            tx.run("""
            MERGE (c:Condition {type: $type, expression: $expression})
            MERGE (h:Hypothesis {id: $hypothesis_id})-[:HAS_SELL_CONDITION]->(c)
            """,
            type="sell",
            expression=condition,
            hypothesis_id=hypothesis_id)

        # Process data feeds
        for feed_type, fields in hypothesis.get("strategy", {}).get("data_feed", {}).items():
            tx.run("""
            MERGE (df:DataFeed {type: $type, fields: $fields})
            MERGE (h:Hypothesis {id: $hypothesis_id})-[:REQUIRES_DATA]->(df)
            """,
            type=feed_type,
            fields=json.dumps(fields),
            hypothesis_id=hypothesis_id)

        # Process backtest results
        for ticker, results in backtest_results.items():
            if "error" in results:
                continue

            # Create BacktestResult node
            tx.run("""
            MATCH (h:Hypothesis {id: $hypothesis_id})-[:HAS_ASSET]->(a:Asset {ticker: $ticker})
            MERGE (bt:BacktestResult {hypothesis_id: $hypothesis_id, ticker: $ticker})
            SET bt.final_value = $final_value,
                bt.sharpe = $sharpe,
                bt.drawdown = $drawdown,
                bt.returns = $returns,
                bt.vwr = $vwr,
                bt.sqn = $sqn,
                bt.calmar = $calmar
            MERGE (h)-[:HAS_BACKTEST]->(bt)
            MERGE (bt)-[:FOR_ASSET]->(a)
            """,
            hypothesis_id=hypothesis_id,
            ticker=ticker,
            final_value=results.get("final_value", 0.0),
            sharpe=results.get("sharpe", {}).get("sharperatio", 0.0),
            drawdown=results.get("drawdown", {}).get("maxdrawdown", 0.0),
            returns=results.get("returns", {}).get("rtot", 0.0),
            vwr=results.get("vwr", {}).get("vwr", 0.0),
            sqn=results.get("sqn", {}).get("sqn", 0.0),
            calmar=results.get("calmar", {}).get("calmar", 0.0))

            # Add trade analysis
            total_trades = results.get("trade_analyzer", {}).get("total", {}).get("total", 0)
            won_trades = results.get("trade_analyzer", {}).get("won", {}).get("total", 0)
            if total_trades > 0:
                tx.run("""
                MATCH (bt:BacktestResult {hypothesis_id: $hypothesis_id, ticker: $ticker})
                MERGE (ta:TradeAnalysis {total: $total, won: $won})
                MERGE (bt)-[:HAS_TRADE_ANALYSIS]->(ta)
                """,
                hypothesis_id=hypothesis_id,
                ticker=ticker,
                total=total_trades,
                won=won_trades)

            # Add annual returns
            for year, return_val in results.get("annual_return", {}).items():
                tx.run("""
                MATCH (bt:BacktestResult {hypothesis_id: $hypothesis_id, ticker: $ticker})
                MERGE (ar:AnnualReturn {year: $year, return_value: $return_val})
                MERGE (bt)-[:HAS_ANNUAL_RETURN]->(ar)
                """,
                hypothesis_id=hypothesis_id,
                ticker=ticker,
                year=year,
                return_val=return_val)

            # Add time returns
            for date, return_val in results.get("time_return", {}).items():
                tx.run("""
                MATCH (bt:BacktestResult {hypothesis_id: $hypothesis_id, ticker: $ticker})
                MERGE (tr:TimeReturn {date: $date, return_value: $return_val})
                MERGE (bt)-[:HAS_TIME_RETURN]->(tr)
                """,
                hypothesis_id=hypothesis_id,
                ticker=ticker,
                date=date,
                return_val=return_val)

        # Process crash results
        for crash_event, crash_data in crash_results.items():
            for ticker, results in crash_data.items():
                if "error" in results:
                    continue

                # Create CrashTest and CrashResult nodes
                tx.run("""
                MATCH (h:Hypothesis {id: $hypothesis_id})-[:HAS_ASSET]->(a:Asset {ticker: $ticker})
                MERGE (ct:CrashTest {name: $crash_event})
                MERGE (cr:CrashResult {hypothesis_id: $hypothesis_id, ticker: $ticker, crash_event: $crash_event})
                SET cr.final_value = $final_value,
                    cr.sharpe = $sharpe,
                    cr.drawdown = $drawdown,
                    cr.returns = $returns,
                    cr.vwr = $vwr,
                    cr.sqn = $sqn,
                    cr.calmar = $calmar
                MERGE (h)-[:TESTED_IN_CRASH]->(ct)
                MERGE (ct)-[:HAS_CRASH_RESULT]->(cr)
                MERGE (cr)-[:FOR_ASSET]->(a)
                """,
                hypothesis_id=hypothesis_id,
                ticker=ticker,
                crash_event=crash_event,
                final_value=results.get("final_value", 0.0),
                sharpe=results.get("sharpe", {}).get("sharperatio", 0.0),
                drawdown=results.get("drawdown", {}).get("maxdrawdown", 0.0),
                returns=results.get("returns", {}).get("rtot", 0.0),
                vwr=results.get("vwr", {}).get("vwr", 0.0),
                sqn=results.get("sqn", {}).get("sqn", 0.0),
                calmar=results.get("calmar", {}).get("calmar", 0.0))

                # Add crash trade analysis
                total_trades = results.get("trade_analyzer", {}).get("total", {}).get("total", 0)
                won_trades = results.get("trade_analyzer", {}).get("won", {}).get("total", 0)
                if total_trades > 0:
                    tx.run("""
                    MATCH (cr:CrashResult {hypothesis_id: $hypothesis_id, ticker: $ticker, crash_event: $crash_event})
                    MERGE (ta:TradeAnalysis {total: $total, won: $won})
                    MERGE (cr)-[:HAS_TRADE_ANALYSIS]->(ta)
                    """,
                    hypothesis_id=hypothesis_id,
                    ticker=ticker,
                    crash_event=crash_event,
                    total=total_trades,
                    won=won_trades)

                # Add crash annual returns
                for year, return_val in results.get("annual_return", {}).items():
                    tx.run("""
                    MATCH (cr:CrashResult {hypothesis_id: $hypothesis_id, ticker: $ticker, crash_event: $crash_event})
                    MERGE (ar:AnnualReturn {year: $year, return_value: $return_val})
                    MERGE (cr)-[:HAS_ANNUAL_RETURN]->(ar)
                    """,
                    hypothesis_id=hypothesis_id,
                    ticker=ticker,
                    crash_event=crash_event,
                    year=year,
                    return_val=return_val)

                # Add crash time returns
                for date, return_val in results.get("time_return", {}).items():
                    tx.run("""
                    MATCH (cr:CrashResult {hypothesis_id: $hypothesis_id, ticker: $ticker, crash_event: $crash_event})
                    MERGE (tr:TimeReturn {date: $date, return_value: $return_val})
                    MERGE (cr)-[:HAS_TIME_RETURN]->(tr)
                    """,
                    hypothesis_id=hypothesis_id,
                    ticker=ticker,
                    crash_event=crash_event,
                    date=date,
                    return_val=return_val)