import ccxt
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

class BinanceClient:
    def __init__(self):
        load_dotenv(override=True)
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.secret = os.getenv('BINANCE_SECRET')
        
        if not self.api_key or not self.secret:
            raise ValueError("Binance API credentials not found in .env")
            
        self.exchange = ccxt.binance({
            'apiKey': self.api_key,
            'secret': self.secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        })
        
        # Testnet check
        if os.getenv('USE_TESTNET') == 'true':
            self.exchange.set_sandbox_mode(True)

    def get_top_gainers(self, limit=50):
        """
        Fetch top gainers from Binance Futures.
        Returns list of (symbol, ticker_data) tuples sorted by percentage change.
        """
        try:
            tickers = self.exchange.fetch_tickers()
            # Filter for USDT pairs and valid volume
            valid_tickers = []
            for symbol, data in tickers.items():
                if symbol.endswith('/USDT:USDT') or (symbol.endswith('USDT') and 'info' in data):
                    # Calculate 24h change percentage
                    if 'percentage' in data and data['percentage'] is not None:
                        valid_tickers.append((symbol, data))
            
            # Sort by percentage change desc
            valid_tickers.sort(key=lambda x: float(x[1]['percentage']), reverse=True)
            return valid_tickers[:limit]
        except Exception as e:
            print(f"Error fetching top gainers: {e}")
            return []

    def get_historical_klines(self, symbol, timeframe='15m', limit=100):
        """
        Fetch OHLCV data.
        Returns DataFrame with columns: timestamp, open, high, low, close, volume
        """
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            print(f"Error fetching klines for {symbol}: {e}")
            return pd.DataFrame()

    def get_balance(self):
        """
        Get USDT balance.
        """
        try:
            balance = self.exchange.fetch_balance()
            return float(balance['USDT']['total'])
        except Exception as e:
            print(f"Error fetching balance: {e}")
            return 0.0

    def get_max_leverage(self, symbol):
        """
        Get max leverage for a symbol.
        """
        try:
            # This often requires a specific endpoint or checking market info
            market = self.exchange.market(symbol)
            if 'limits' in market and 'leverage' in market['limits']:
                return market['limits']['leverage']['max']
            return 20 # Default fallback
        except Exception as e:
            # print(f"Error fetching max leverage for {symbol}: {e}")
            return 20
