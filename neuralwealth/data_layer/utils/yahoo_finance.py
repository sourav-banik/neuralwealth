def get_yahoo_symbol(trading_symbol, asset_class, market=None, **kwargs):
    """
    Convert trading symbol to Yahoo Finance symbol with extended international market support
    
    Args:
        trading_symbol (str): The base trading symbol (e.g., 'AAPL')
        asset_class (str): One of ['stock', 'etf', 'option', 'future', 'forex', 'crypto', 'index', 'private', 'treasury']
        market (str): Market identifier (e.g., 'L' for London, 'BO' for Bombay, 'SI' for Singapore)
        kwargs: Additional parameters for options (expiry, strike, option_type)
    
    Returns:
        str: Yahoo Finance compatible symbol
    """
    # Market suffix mapping
    MARKET_SUFFIXES = {
        # North America
        'US': '', 
        'NYSE': '', 
        'NASDAQ': '',
        'TSX': 'TO',  # Toronto
        'TSXV': 'V',  # TSX Venture
        'CSE': 'CN',  # Canadian Securities Exchange
        
        # Europe
        'LSE': 'L',   # London
        'LSEIOB': 'IL',  # London International Order Book
        'FRA': 'F',   # Frankfurt
        'ETR': 'DE',  # Xetra
        'AMS': 'AS',  # Amsterdam
        'EPA': 'PA',  # Paris
        'EBR': 'BR',  # Brussels
        'MIL': 'MI',  # Milan
        'MCE': 'MC',  # Madrid
        'SWX': 'SW',  # Swiss Exchange
        'OSL': 'OL',  # Oslo
        'CPH': 'CO',  # Copenhagen
        'STO': 'ST',  # Stockholm
        'HEL': 'HE',  # Helsinki
        
        # Asia
        'NSE': 'NS',  # National Stock Exchange India
        'BSE': 'BO',  # Bombay Stock Exchange
        'SGX': 'SI',  # Singapore
        'HKG': 'HK',  # Hong Kong
        'SHA': 'SS',  # Shanghai
        'SHE': 'SZ',  # Shenzhen
        'TYO': 'T',   # Tokyo
        'OSE': 'OS',  # Osaka
        'KRX': 'KS',  # Korea Exchange
        'TWSE': 'TW', # Taiwan
        'SET': 'BK',  # Thailand
        
        # Oceania
        'ASX': 'AX',  # Australia
        'NZX': 'NZ',  # New Zealand
        
        # Other
        'JSE': 'JO',  # Johannesburg
        'TASE': 'TA', # Tel Aviv
    }
    
    symbol = trading_symbol.upper()
    market = market.upper() if market else None
    
    if asset_class.lower() == 'stock':
        if market and market in MARKET_SUFFIXES:
            suffix = MARKET_SUFFIXES[market]
            return f"{symbol}.{suffix}" if suffix else symbol
        return symbol
    
    elif asset_class.lower() == 'etf':
        return symbol
    
    elif asset_class.lower() == 'option':
        expiry = kwargs.get('expiry')  # '20240621'
        strike = kwargs.get('strike')  # 150.0
        option_type = kwargs.get('option_type', 'C').upper()  # 'C' or 'P'
        
        if not all([expiry, strike, option_type]):
            raise ValueError("Options require expiry, strike, and option_type parameters")
            
        strike_formatted = f"{float(strike):08.0f}" if strike.is_integer() else f"{float(strike)*1000:08.0f}"
        return f"{symbol}{expiry}{option_type}{strike_formatted}"
    
    elif asset_class.lower() == 'future':
        return f"{symbol}=F"
    
    elif asset_class.lower() == 'forex':
        return f"{symbol}=X"
    
    elif asset_class.lower() == 'crypto':
        return f"{symbol}-USD"
    
    elif asset_class.lower() == 'index':
        return f"^{symbol}"
    
    elif asset_class.lower() == 'treasury':
        return f"^{symbol}"
    
    elif asset_class.lower() == 'private':
        return f"{symbol}.PVT"
    
    else:
        raise ValueError(f"Unsupported asset class: {asset_class}")
    

def parse_yahoo_symbol(yahoo_symbol):
    """
    Convert Yahoo Finance symbol back to its components (trading symbol, asset class, market)
    
    Args:
        yahoo_symbol (str): Yahoo Finance symbol (e.g., "HSBA.L", "EURUSD=X")
    
    Returns:
        dict: {
            'trading_symbol': str,
            'asset_class': str,
            'market': str or None,
            'option_details': dict or None  # Only for options
        }
    """
    # Market suffix to exchange mapping (reverse of previous)
    SUFFIX_MARKETS = {
        # North America
        'TO': 'TSX', 'V': 'TSXV', 'CN': 'CSE',
        # Europe
        'L': 'LSE', 'IL': 'LSEIOB', 'F': 'FRA', 'DE': 'ETR',
        'AS': 'AMS', 'PA': 'EPA', 'BR': 'EBR', 'MI': 'MIL',
        'MC': 'MCE', 'SW': 'SWX', 'OL': 'OSL', 'CO': 'CPH',
        'ST': 'STO', 'HE': 'HEL',
        # Asia
        'NS': 'NSE', 'BO': 'BSE', 'SI': 'SGX', 'HK': 'HKG',
        'SS': 'SHA', 'SZ': 'SHE', 'T': 'TYO', 'OS': 'OSE',
        'KS': 'KRX', 'TW': 'TWSE', 'BK': 'SET',
        # Oceania
        'AX': 'ASX', 'NZ': 'NZX',
        # Other
        'JO': 'JSE', 'TA': 'TASE'
    }
    
    symbol = yahoo_symbol.upper()
    result = {
        'trading_symbol': None,
        'asset_class': None,
        'market': None,
        'option_details': None
    }
    
    # Check for special symbol types
    if symbol.startswith('^'):
        if symbol in ['^IRX', '^FVX', '^TNX', '^TYX']:
            result['asset_class'] = 'treasury'
        else:    
            result['asset_class'] = 'index'
        result['trading_symbol'] = symbol[1:]
    elif symbol.endswith('PVT'):
        result['asset_class'] = 'private'
        result['trading_symbol'] = symbol.split('.')[0]
    elif symbol.endswith('=X'):
        result['asset_class'] = 'forex'
        result['trading_symbol'] = symbol[:-2]
    elif symbol.endswith('=F'):
        result['asset_class'] = 'future'
        result['trading_symbol'] = symbol[:-2]
    elif '-' in symbol and (symbol.endswith('-USD') or len(symbol.split('-')) == 2):
        result['asset_class'] = 'crypto'
        result['trading_symbol'] = symbol.split('-')[0]
    elif '.' in symbol:
        # International stock
        base, suffix = symbol.split('.')
        result['asset_class'] = 'stock'
        result['trading_symbol'] = base
        result['market'] = SUFFIX_MARKETS.get(suffix, suffix)
    elif len(symbol) > 12 and any(c in symbol for c in ['C', 'P']):
        # Option symbol (e.g., AAPL240621C00150000)
        try:
            # Find the position of C or P
            for i, char in enumerate(symbol):
                if char in ['C', 'P']:
                    strike_start = i + 1
                    break
            
            underlying = symbol[:6].rstrip('0123456789')
            expiry = symbol[len(underlying):len(underlying)+6]
            option_type = symbol[len(underlying)+6]
            strike = float(symbol[strike_start:strike_start+8]) / 1000
            
            result['asset_class'] = 'option'
            result['trading_symbol'] = underlying
            result['option_details'] = {
                'expiry': expiry,
                'option_type': 'call' if option_type == 'C' else 'put',
                'strike': strike
            }
        except:
            # Fallback to regular stock
            result['asset_class'] = 'stock'
            result['trading_symbol'] = symbol
    else:
        # Default to US stock/ETF
        result['asset_class'] = 'stock' if len(symbol) <= 5 else 'etf'
        result['trading_symbol'] = symbol
    
    return result