LLM_STRATEGY_PROMPTS = {
    "preliminary": """
        Based on {context} and constraints: {constraints}
        Identify 5-10 raw patterns from:
        {data_samples}
        Patterns are combinations of technical, fundamental, or macro signals (e.g., 'RSI below 30 with ROE > 0.15', 'price above SMA with positive CPI inflation').
        Use simple metrics like correlation, frequency, or mean reversion to identify patterns.
        Assign strength based on:
        - HIGH: Strong correlation (>0.7) or frequent occurrence (>50% of samples).
        - MEDIUM: Moderate correlation (0.3-0.7) or occurrence (20-50% of samples).
        - LOW: Weak correlation (<0.3) or rare occurrence (<20% of samples).
        Return JSON:
        [
            {{
                "pattern": "DESCRIPTION",
                "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                "strength": "HIGH|MEDIUM|LOW"
            }}
        ]
    """,
    "validated": """
        Based on {context} and constraints: {constraints}
        Validate these patterns:
        {patterns}
        Use a statistical test (e.g., correlation p-value, historical win rate) to reject patterns with p>0.05 or low returns (<5% annualized).
        Keep the 3-5 strongest patterns based on highest correlation, lowest p-value, or highest historical returns.
        Return JSON:
        [
            {{
                "pattern": "DESCRIPTION",
                "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                "strength": "HIGH|MEDIUM"
            }}
        ]
    """,
    "final": """
        Based on {context} and constraints: {constraints}
        Convert these patterns into investment hypotheses optimized for backtesting with Python Backtrader:
        {patterns}
        The backtesting data schema is:
        {data_schema}
        Return JSON:
        [
            {{{{
                "hypothesis": "ACTIONABLE STATEMENT (e.g., 'Buy when RSI < 30 and ROE > 0.15')",
                "assets": [{{"ticker": "ticker", "asset_class": "asset_class", "market": "market"}}],
                "trigger": "CONCISE CONDITION SUMMARY (e.g., 'Oversold RSI with strong fundamentals')",
                "timeframe": "PERIOD (e.g., '3-6 months')",
                "confidence": 0.0-1.0,
                "risks": "POTENTIAL RISKS (e.g., 'Market volatility, earnings surprises')",
                "strategy": {{{{
                    "indicators": [
                        {{"name": "INDICATOR_NAME", "params": {{"param_name": value}}}}
                    ],
                    "buy_conditions": ["PYTHON_EXPRESSION"],
                    "sell_conditions": ["PYTHON_EXPRESSION"],
                    "holding_period": INTEGER (days),
                    "data_feed": {{{{
                        "measurement_name": ["field1", "field2"]
                    }}}}
                }}}}
            }}}}
        ]

        ### Instructions for Strategy Generation:
        1. **Data Schema Compliance**:
        - Use ONLY fields from the provided schema measurements (e.g., 'market_info.close', 'financial_ratios.roe', 'macro_data.cpi_inflation').
        - Specify exact measurement-field pairs in 'data_feed' (e.g., {{ "market_info": ["close", "rsi"], "quarterly_financial_ratios": ["roe"] }}).
        - For fields appearing in multiple measurements (e.g., 'roe' in 'financial_ratios' and 'quarterly_financial_ratios'), select ONE measurement based on the strategy's timeframe:
            - Prefer 'quarterly_*' measurements for timeframes <= 1 year (365 days) for higher granularity.
            - Prefer annual measurements (e.g., 'financial_ratios') for timeframes > 1 year for stability.
            - If ambiguous, choose 'quarterly_*' for consistency unless annual data is explicitly required.
        - Do NOT list the same field in multiple measurements within the same 'data_feed' to avoid duplication.

        2. **Backtrader-Compatible Indicators**:
        - Use only these Backtrader-supported indicators.
        - The following indicators are supported in Backtrader currently -
            
            Accelerationdecelerationoscillator, Accdeosc, Accum, Cumsum, Cumulativesum, Adaptivemovingaverage, Kama, 
            Movingaverageadaptive, Alln, Anyn, Applyn, Aroondown, Aroonoscillator, Aroonosc, Aroonup, Aroonupdown, Aroonindicator, 
            Aroonupdownoscillator, Aroonupdownosc, Average, Arithmeticmean, Mean, Averagedirectionalmovementindex, Adx, 
            Averagedirectionalmovementindexrating, Adxr, Averagetruerange, Atr, Awesomeoscillator, Awesomeosc, Ao, 
            Baseapplyn, Bollingerbands, Bbands, Bollingerbandspct, Cointn, Commoditychannelindex, Cci, Crossdown, 
            Crossover, Crossup, Dv2, Demarkpivotpoint, Detrendedpriceoscillator, Dpo, Dicksonmovingaverage, Dma, Dicksonma, 
            Directionalindicator, Di, Directionalmovement, Dm, Directionalmovementindex, Dmi, Doubleexponentialmovingaverage, 
            Dema, Movingaveragedoubleexponential, Downday, Downdaybool, Downmove, Envelope, Exponentialmovingaverage, Ema, 
            Movingaverageexponential, Exponentialsmoothing, Expsmoothing, Exponentialsmoothingdynamic, Expsmoothingdynamic, 
            Fibonaccipivotpoint, Findfirstindex, Findfirstindexhighest, Findfirstindexlowest, Findlastindex, Hullmovingaverage,
            Hma, Ichimoku, Minusdi, Momentum, Momentumoscillator, Movingaverage, Movingaveragehull, Movingaveragesimple, Sma, 
            Percentagepriceoscillator, Ppo, Percentagerank, Percentrank, Plusdi, Prettygoodoscillator, Pgo, Rateofchange, Roc, 
            Rateofchange100, Roc100, Rsi, Rsi_safe, Smma, Standarddeviation, Stddev, Stochastic, Stochasticfast, Stochasticfull, 
            Tema, Trix, Trixsignal, Truestrengthindicator, Tsi, Ultimateoscillator, Ultimate, Upday, Updaybool, Upmove, 
            Weightedmovingaverage, Wma, Williamspercentr, Williamsr, Zerolagema, Zerolagexponentialmovingaverage, Zerolagindicator,
            Zerolag, Zlema

        - For pre-calculated fields (e.g., adx_14, atr_14, bb_lower, bb_middle, bb_upper, close_norm, close_z_score, cmo_14, 
            ema_20, high_norm, high_z_score, hma_9, kc_lower, kc_middle, kc_upper, low_norm, low_z_score, macd, obv, open_norm,
            open_z_score, pivot, r1, rsi_14, rsi_z_score,s1,sma_20,stoch_d,stoch_k,vwap) from 'market_info', treat them as data fields (e.g., 'data.rsi_14[0]'), 
            not indicators.
        - Example indicator config:
            - {{ "name": "RSI", "params": {{ "period": 14 }} }}
            - {{ "name": "MACD", "params": {{ "period_me1": 12, "period_me2": 26, "period_signal": 9 }} }}
        - Do NOT include unsupported indicators or custom calculations.

        3. **Buy/Sell Conditions**:
        - Generate conditions as simple, evaluable Python expressions using:
            - Indicator values (e.g., "indicators['rsi'].lines.rsi[0] < 30")
            - Data fields (e.g., "data.close[0] > data.sma_20[0]", "data.roe[0] > 0.15", "data.fed_funds_rate[0]", "data['10_year_treasury_yield'][0]")
            - Temporal references (e.g., "data.close[0] > data.close[-1]")
        - Use lowercase for indicator and field names (e.g., 'rsi', 'close', 'roe').
        - Combine conditions with 'and', 'or', 'not' (e.g., "indicators['rsi'].lines[0] < 30 and data.roe[0] > 0.15").
        - Avoid complex logic, functions, or external calculations.
        - basic_info measurement is only point of time recent data, no time series available for the measurement.
        - The financial ratios, margin like roe, roa, net_profit_margin are in full number (i.e., 15% will be 15 not 0.15, 43% will be 43 not 0.43).  
        - Use necessary data from all measurements (e.g., macro_data, news_sentiment, financial_ratios) and fields according to need.
        - Ensure all referenced fields are included in 'data_feed' from a single measurement per field.
        - WARNING: Do NOT reference indicators (e.g., ATR, RSI, SMA) as data fields (e.g., data.atr[0]). Indicators must use indicators['name'].lines[0] syntax with lowercase 'name' (e.g., indicators['atr'], not indicators['ATR']) and should NOT appear in data_feed.

        4. **Holding Period**:
        - Specify as an integer number of days (e.g., 90 for 3 months).
        - Align with the 'timeframe' description (e.g., '3-6 months' corresponds to 90-180 days).

        5. **Example Output**:
        ```json
        [
            {{
                "hypothesis": "Buy stocks with oversold RSI and high ROE",
                "assets": [{{"ticker": "AAPL", "asset_class": "stock", "market": "NASDAQ"}}],
                "trigger": "RSI below 30 with ROE above 15%",
                "timeframe": "3-6 months",
                "confidence": 0.85,
                "risks": "Market downturn, unexpected earnings miss",
                "strategy": {{
                    "indicators": [
                        {{"name": "RSI", "params": {{"period": 14}}}}
                    ],
                    "buy_conditions": [
                        "indicators['rsi'].lines[0] < 30 and data.roe[0] > 0.15"
                    ],
                    "sell_conditions": [
                        "indicators['rsi'].lines[0] > 70 or data.close[0] < data.close[-5] * 0.95"
                    ],
                    "holding_period": 90,
                    "data_feed": {{
                        "market_info": ["Close", "RSI"],
                        "quarterly_financial_ratios": ["roe"]
                    }}
                }}
            }}
        ]
        ```
        Ensure all fields in 'buy_conditions' and 'sell_conditions' are accessible via the Backtrader data object or indicators dictionary, match the 'data_feed' specification, and originate from a single measurement per field to avoid duplication. The strategy must be directly executable in Backtrader without additional condition parsing or transformations.
    """
}