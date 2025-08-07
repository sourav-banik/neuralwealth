import backtrader as bt
from typing import Dict

class IndicatorFactory:
    """Utility class to initialize Backtrader indicators based on name, params, and data."""
    
    INDS = [
        'accelerationdecelerationoscillator', 'accdeosc', 'accum', 'cumsum', 'cumulativesum',
        'adaptivemovingaverage', 'kama', 'movingaverageadaptive', 'alln', 'anyn', 'applyn', 'aroondown',
        'aroonoscillator', 'aroonosc', 'aroonup', 'aroonupdown', 'aroonindicator',
        'aroonupdownoscillator', 'aroonupdownosc', 'average', 'arithmeticmean', 'mean',
        'averagedirectionalmovementindex', 'adx', 'averagedirectionalmovementindexrating', 'adxr',
        'averagetruerange', 'atr', 'awesomeoscillator', 'awesomeosc', 'ao', 'baseapplyn',
        'bollingerbands', 'bbands', 'bollingerbandspct', 'cointn', 'commoditychannelindex', 'cci',
        'crossdown', 'crossover', 'crossup', 'dv2', 'demarkpivotpoint', 'detrendedpriceoscillator', 'dpo',
        'dicksonmovingaverage', 'dma', 'dicksonma', 'directionalindicator', 'di', 'directionalmovement', 'dm',
        'directionalmovementindex', 'dmi', 'doubleexponentialmovingaverage', 'dema',
        'movingaveragedoubleexponential', 'downday', 'downdaybool', 'downmove', 'envelope',
        'exponentialmovingaverage', 'ema', 'movingaverageexponential', 'exponentialsmoothing', 'expsmoothing',
        'exponentialsmoothingdynamic', 'expsmoothingdynamic', 'fibonaccipivotpoint', 'findfirstindex',
        'findfirstindexhighest', 'findfirstindexlowest', 'findlastindex', 'hullmovingaverage', 'hma',
        'ichimoku', 'minusdi', 'momentum', 'momentumoscillator', 'movingaverage',
        'movingaveragehull', 'movingaveragesimple', 'sma',
        'percentagepriceoscillator', 'ppo', 'percentagerank', 'percentrank', 'plusdi',
        'prettygoodoscillator', 'pgo', 'rateofchange', 'roc', 'rateofchange100', 'roc100',
        'rsi', 'rsi_safe', 'smma', 'standarddeviation', 'stddev',
        'stochastic', 'stochasticfast', 'stochasticfull', 'tema', 'trix', 'trixsignal',
        'truestrengthindicator', 'tsi', 'ultimateoscillator', 'ultimate', 'upday', 'updaybool',
        'upmove', 'weightedmovingaverage', 'wma', 'williamspercentr', 'williamsr',
        'zerolagema', 'zerolagexponentialmovingaverage', 'zerolagindicator', 'zerolag', 'zlema'
    ]

    @staticmethod
    def create_indicator(name: str, params: Dict, data: bt.DataBase) -> bt.Indicator:
        """Create and return a Backtrader indicator based on name, parameters, and data."""
        name = name.lower()
        if name not in IndicatorFactory.INDS:
            raise ValueError(f"Unknown indicator: {name}")

        if name in ['accelerationdecelerationoscillator', 'accdeosc']:
            return bt.indicators.AccelerationDecelerationOscillator(
                data, period=params.get('period', 5), movav=params.get('movav', bt.indicators.SMA)
            )
        
        elif name in ['accum', 'cumsum', 'cumulativesum']:
            return bt.indicators.Accum(data, seed=params.get('seed', 0.0))
        
        elif name in ['adaptivemovingaverage', 'kama', 'movingaverageadaptive']:
            return bt.indicators.AdaptiveMovingAverage(
                data, period=params.get('period', 30), fast=params.get('fast', 2), slow=params.get('slow', 30)
            )
        
        elif name == 'alln':
            return bt.indicators.AllN(data, period=params.get('period', 1))
        
        elif name == 'anyn':
            return bt.indicators.AnyN(data, period=params.get('period', 1))
        
        elif name == 'applyn':
            return bt.indicators.ApplyN(data, period=params.get('period', 1), func=params.get('func', None))
        
        elif name == 'aroondown':
            return bt.indicators.AroonDown(
                data, period=params.get('period', 14), upperband=params.get('upperband', 70), lowerband=params.get('lowerband', 30)
            )
        
        elif name in ['aroonoscillator', 'aroonosc']:
            return bt.indicators.AroonOscillator(
                data, period=params.get('period', 14), upperband=params.get('upperband', 70), lowerband=params.get('lowerband', 30)
            )
        
        elif name == 'aroonup':
            return bt.indicators.AroonUp(
                data, period=params.get('period', 14), upperband=params.get('upperband', 70), lowerband=params.get('lowerband', 30)
            )
        
        elif name in ['aroonupdown', 'aroonindicator']:
            return bt.indicators.AroonUpDown(
                data, period=params.get('period', 14), upperband=params.get('upperband', 70), lowerband=params.get('lowerband', 30)
            )
        
        elif name in ['aroonupdownoscillator', 'aroonupdownosc']:
            return bt.indicators.AroonUpDownOscillator(
                data, period=params.get('period', 14), upperband=params.get('upperband', 70), lowerband=params.get('lowerband', 30)
            )
        
        elif name in ['average', 'arithmeticmean', 'mean']:
            return bt.indicators.Average(data, period=params.get('period', 1))
        
        elif name in ['averagedirectionalmovementindex', 'adx']:
            return bt.indicators.AverageDirectionalMovementIndex(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['averagedirectionalmovementindexrating', 'adxr']:
            return bt.indicators.AverageDirectionalMovementIndexRating(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['averagetruerange', 'atr']:
            return bt.indicators.AverageTrueRange(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['awesomeoscillator', 'awesomeosc', 'ao']:
            return bt.indicators.AwesomeOscillator(
                data, fast=params.get('fast', 5), slow=params.get('slow', 34), movav=params.get('movav', bt.indicators.SMA)
            )
        
        elif name in ['bollingerbands', 'bbands']:
            return bt.indicators.BollingerBands(
                data, 
                period=params.get('period', 20), 
                devfactor=params.get('devfactor', 2.0), 
                movav=params.get('movav', bt.indicators.MovingAverageSimple)
            )
        
        elif name == 'bollingerbandspct':
            return bt.indicators.BollingerBandsPct(
                data, 
                period=params.get('period', 20), 
                devfactor=params.get('devfactor', 2.0), 
                movav=params.get('movav', bt.indicators.MovingAverageSimple)
            )
        
        elif name == 'cointn':
            return bt.indicators.CointN(data, period=params.get('period', 10), regression=params.get('regression', 'c'))
        
        elif name in ['commoditychannelindex', 'cci']:
            return bt.indicators.CommodityChannelIndex(
                data, 
                period=params.get('period', 20), 
                factor=params.get('factor', 0.015), 
                movav=params.get('movav', bt.indicators.MovingAverageSimple), 
                upperband=params.get('upperband', 100.0), 
                lowerband=params.get('lowerband', -100.0)
            )
        
        elif name == 'crossdown':
            return bt.indicators.CrossDown(data, data1=params.get('data1', data), subplot=params.get('subplot', True))
        
        elif name == 'crossover':
            return bt.indicators.CrossOver(data, data1=params.get('data1', data), subplot=params.get('subplot', True))
        
        elif name == 'crossup':
            return bt.indicators.CrossUp(data, data1=params.get('data1', data), subplot=params.get('subplot', True))
        
        elif name == 'dv2':
            return bt.indicators.DV2(
                data, 
                period=params.get('period', 252), 
                maperiod=params.get('maperiod', 2), 
                _movav=params.get('_movav', bt.indicators.SMA)
            )
        
        elif name == 'demarkpivotpoint':
            return bt.indicators.DemarkPivotPoint(
                data, 
                open=params.get('open', False), 
                close=params.get('close', False), 
                _autoplot=params.get('_autoplot', True), 
                level1=params.get('level1', 0.382), 
                level2=params.get('level2', 0.618), 
                level3=params.get('level3', 1.0)
            )
        
        elif name in ['detrendedpriceoscillator', 'dpo']:
            return bt.indicators.DetrendedPriceOscillator(
                data, period=params.get('period', 20), movav=params.get('movav', bt.indicators.MovingAverageSimple)
            )
        
        elif name in ['dicksonmovingaverage', 'dma', 'dicksonma']:
            return bt.indicators.DicksonMovingAverage(
                data, 
                period=params.get('period', 30), 
                gainlimit=params.get('gainlimit', 50), 
                hperiod=params.get('hperiod', 7), 
                _movav=params.get('_movav', bt.indicators.EMA), 
                _hma=params.get('_hma', bt.indicators.HMA)
            )
        
        elif name in ['directionalindicator', 'di']:
            return bt.indicators.DirectionalIndicator(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['directionalmovement', 'dm']:
            return bt.indicators.DirectionalMovement(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['directionalmovementindex', 'dmi']:
            return bt.indicators.DirectionalMovementIndex(
                data, period=params.get('period', 14), movav=params.get('movav', bt.indicators.SmoothedMovingAverage)
            )
        
        elif name in ['doubleexponentialmovingaverage', 'dema', 'movingaveragedoubleexponential']:
            return bt.indicators.DoubleExponentialMovingAverage(
                data, period=params.get('period', 30), _movav=params.get('_movav', bt.indicators.EMA)
            )
        
        elif name == 'downday':
            return bt.indicators.DownDay(data, period=params.get('period', 1))
        
        elif name == 'downdaybool':
            return bt.indicators.DownDayBool(data, period=params.get('period', 1))
        
        elif name == 'downmove':
            return bt.indicators.DownMove(data)
        
        elif name == 'envelope':
            return bt.indicators.Envelope(data, perc=params.get('perc', 2.5))
        
        elif name in ['exponentialmovingaverage', 'ema', 'movingaverageexponential']:
            return bt.indicators.ExponentialMovingAverage(data, period=params.get('period', 30))
        
        elif name in ['exponentialsmoothing', 'expsmoothing']:
            return bt.indicators.ExponentialSmoothing(data, period=params.get('period', 1), alpha=params.get('alpha', None))
        
        elif name in ['exponentialsmoothingdynamic', 'expsmoothingdynamic']:
            return bt.indicators.ExponentialSmoothingDynamic(data, period=params.get('period', 1), alpha=params.get('alpha', None))
        
        elif name == 'fibonaccipivotpoint':
            return bt.indicators.FibonacciPivotPoint(
                data, 
                open=params.get('open', False), 
                close=params.get('close', False), 
                _autoplot=params.get('_autoplot', True), 
                level1=params.get('level1', 0.382), 
                level2=params.get('level2', 0.618), 
                level3=params.get('level3', 1.0)
            )
        
        elif name == 'findfirstindex':
            return bt.indicators.FindFirstIndex(data, period=params.get('period', 1), _evalfunc=params.get('_evalfunc', None))
        
        elif name == 'findfirstindexhighest':
            return bt.indicators.FindFirstIndexHighest(data, period=params.get('period', 1))
        
        elif name == 'findfirstindexlowest':
            return bt.indicators.FindFirstIndexLowest(data, period=params.get('period', 1))
        
        elif name == 'findlastindex':
            return bt.indicators.FindLastIndex(data, period=params.get('period', 1), _evalfunc=params.get('_evalfunc', None))
        
        elif name in ['hullmovingaverage', 'hma']:
            return bt.indicators.HullMovingAverage(data, period=params.get('period', 9))
        
        elif name == 'ichimoku':
            return bt.indicators.Ichimoku(
                data, tenkan=params.get('tenkan', 9), 
                kijun=params.get('kijun', 26), 
                senkou=params.get('senkou', 52), 
                senkou_lead=params.get('senkou_lead', 26), 
                chikou=params.get('chikou', 26)
            )
        
        elif name == 'minusdi':
            return bt.indicators.MinusDirectionalIndicator(data, period=params.get('period', 14))
        
        elif name in ['momentum', 'momentumoscillator']:
            return bt.indicators.Momentum(data, period=params.get('period', 14))
        
        elif name == 'movingaverage':
            return bt.indicators.MovingAverage(data, period=params.get('period', 30))
        
        elif name in ['movingaveragehull', 'hma']:
            return bt.indicators.HullMovingAverage(data, period=params.get('period', 9))
        
        elif name in ['movingaveragesimple', 'sma']:
            return bt.indicators.MovingAverageSimple(data, period=params.get('period', 20))
        
        elif name in ['percentagepriceoscillator', 'ppo']:
            return bt.indicators.PercentagePriceOscillator(
                data, period_fast=params.get('period_fast', 12), period_slow=params.get('period_slow', 26)
            )
        
        elif name in ['percentagerank', 'percentrank']:
            return bt.indicators.PercentRank(data, period=params.get('period', 20))
        
        elif name == 'plusdi':
            return bt.indicators.PlusDirectionalIndicator(data, period=params.get('period', 14))
        
        elif name in ['prettygoodoscillator', 'pgo']:
            return bt.indicators.PrettyGoodOscillator(data, period=params.get('period', 14))
        
        elif name in ['rateofchange', 'roc']:
            return bt.indicators.RateOfChange(data, period=params.get('period', 12))
        
        elif name in ['rateofchange100', 'roc100']:
            return bt.indicators.RateOfChange100(data, period=params.get('period', 12))
        
        elif name == 'rsi':
            return bt.indicators.RSI(data, period=params.get('period', 14))
        
        elif name == 'rsi_safe':
            return bt.indicators.RSI_Safe(data, period=params.get('period', 14))
        
        elif name == 'smma':
            return bt.indicators.SmoothedMovingAverage(data, period=params.get('period', 20))
        
        elif name in ['standarddeviation', 'stddev']:
            return bt.indicators.StandardDeviation(
                data, 
                period=params.get('period', 20), 
                movav=params.get('movav', bt.indicators.MovingAverageSimple)
            )
        
        elif name in ['stochastic', 'stochasticfast', 'stochasticfull']:
            return bt.indicators.Stochastic(
                data, period=params.get('period', 14), 
                period_dfast=params.get('period_dfast', 3), 
                period_dslow=params.get('period_dslow', 3)
            )
        
        elif name == 'tema':
            return bt.indicators.TripleExponentialMovingAverage(data, period=params.get('period', 20))
        
        elif name == 'trix':
            return bt.indicators.Trix(data, period=params.get('period', 15))
        
        elif name == 'trixsignal':
            return bt.indicators.TrixSignal(data, period=params.get('period', 15), period_signal=params.get('period_signal', 9))
        
        elif name in ['truestrengthindicator', 'tsi']:
            return bt.indicators.TrueStrengthIndicator(data, period1=params.get('period1', 25), period2=params.get('period2', 13))
        
        elif name in ['ultimateoscillator', 'ultimate']:
            return bt.indicators.UltimateOscillator(
                data, period1=params.get('period1', 7), period2=params.get('period2', 14), period3=params.get('period3', 28)
            )
        
        elif name == 'upday':
            return bt.indicators.UpDay(data, period=params.get('period', 1))
        
        elif name == 'updaybool':
            return bt.indicators.UpDayBool(data, period=params.get('period', 1))
        
        elif name == 'upmove':
            return bt.indicators.UpMove(data)
        
        elif name == 'weightedmovingaverage':
            return bt.indicators.WeightedMovingAverage(data, period=params.get('period', 20))
        
        elif name in ['williamspercentr', 'williamsr']:
            return bt.indicators.WilliamsR(data, period=params.get('period', 14))
        
        elif name in ['zerolagema', 'zerolagexponentialmovingaverage', 'zerolagindicator', 'zerolag', 'zlema']:
            return bt.indicators.ZeroLagExponentialMovingAverage(data, period=params.get('period', 20))
        
        else:
            raise ValueError(f"Indicator {name} not implemented")