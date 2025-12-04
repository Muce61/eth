import pandas as pd
import pandas_ta as ta
from config.settings import Config

class MomentumStrategy:
    def __init__(self):
        self.config = Config()

    def filter_top_gainers(self, tickers_data):
        """
        Filter tickers that meet the top gainer criteria.
        :param tickers_data: List of ticker dictionaries from BinanceClient
        :return: List of symbols
        """
        # Already sorted by BinanceClient, just filter by threshold
        qualified = [
            t[0] for t in tickers_data 
            if self.config.CHANGE_THRESHOLD_MIN <= float(t[1]['percentage']) <= self.config.CHANGE_THRESHOLD_MAX
        ]
        return qualified[:self.config.TOP_GAINER_COUNT]

    def analyze_pattern(self, df):
        """
        Check for continuous bullish candles pattern.
        :param df: DataFrame with OHLCV data
        :return: Boolean
        """
        if len(df) < self.config.BULLISH_CANDLES_COUNT:
            return False
            
        # Get last N candles
        last_n = df.tail(self.config.BULLISH_CANDLES_COUNT)
        
        # Check if all are bullish (Close > Open)
        is_bullish = all(row['close'] > row['open'] for _, row in last_n.iterrows())
        
        # Check if each close is higher than previous close (Uptrend)
        closes = last_n['close'].values
        is_uptrend = all(closes[i] > closes[i-1] for i in range(1, len(closes)))
        
        return is_bullish and is_uptrend

    def analyze_volume(self, df):
        """
        Check if volume is supporting the move.
        Current volume > Moving Average Volume
        :param df: DataFrame with OHLCV data
        :return: Boolean
        """
        if len(df) < 20:
            return False
            
        # Calculate SMA of Volume
        df['vol_ma'] = ta.sma(df['volume'], length=20)
        
        current_vol = df['volume'].iloc[-1]
        ma_vol = df['vol_ma'].iloc[-1]
        
        # Volume should be higher than average
        return current_vol > ma_vol

    def analyze_rsi(self, df):
        """
        Check RSI.
        RSI should be > 50 (momentum).
        Removed upper limit 75 because winning trades often have high RSI.
        """
        if len(df) < 14:
            return False
        
        rsi = ta.rsi(df['close'], length=14)
        if rsi is None:
            return False
            
        current_rsi = rsi.iloc[-1]
        
        # Condition: RSI > 50 (Momentum)
        return current_rsi > 50

    def analyze_trend(self, df):
        """
        Check if price is above EMA 200 (Long term trend).
        """
        if len(df) < 200:
            return False
            
        ema200 = ta.ema(df['close'], length=200)
        if ema200 is None:
            return False
            
        current_price = df['close'].iloc[-1]
        current_ema = ema200.iloc[-1]
        
        return current_price > current_ema

    def check_signal(self, symbol, df):
        """
        Strict Breakout Signal with Loss Pattern Optimization.
        """
        if df.empty or len(df) < 21:
            return None
            
        # DEBUG: Trace BTC
        if 'BTC' in symbol.upper():
            # print(f"DEBUG: Checking {symbol} | Len: {len(df)}")
            pass

        current = df.iloc[-1]
        
        # Calculate metrics
        avg_vol = df['volume'].iloc[-21:-1].mean()
        vol_ratio = current['volume'] / avg_vol if avg_vol > 0 else 0
        rsi = ta.rsi(df['close'], length=14).iloc[-1]
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)['ADX_14'].iloc[-1]
        
        # Trend Check (EMA 200)
        ema200 = ta.ema(df['close'], length=200)
        if ema200 is None:
            # if 'BTC' in symbol.upper():
            #     print(f"DEBUG: {symbol} EMA200 is None (Len: {len(df)})")
            return None
            
        current_ema = ema200.iloc[-1]
        if pd.isna(current_ema):
            # if 'BTC' in symbol.upper():
            #     print(f"DEBUG: {symbol} EMA200 is NaN (Len: {len(df)})")
            return None
            
        # DEBUG: Print metrics for BTC
        if 'BTC' in symbol.upper() and (vol_ratio > 2.0 or rsi > 50 or rsi < 50):
             # print(f"DEBUG: {symbol} Price: {current['close']:.2f} | EMA: {current_ema:.2f} | Vol: {vol_ratio:.2f} | RSI: {rsi:.2f}")
             pass
        
        # Get timestamp from the last candle
        timestamp = df.index[-1]

        # ----------------------------------------
        # LONG SIGNAL
        # ----------------------------------------
        if current['close'] > current_ema:
            # 1. EMA Deviation Cap (15%)
            if (current['close'] - current_ema) / current_ema > 0.15:
                # print(f"DEBUG: {symbol} Long Rejected - EMA Deviation {((current['close'] - current_ema) / current_ema):.2f} > 0.15")
                return None
                
            # 2. Time Filter (Avoid 05:00-07:00 UTC)
            if timestamp.hour in [5, 6, 7]:
                return None
            
            # 3. Volume Filter (Data-optimized > 3.2)
            if vol_ratio <= 3.2:
                # print(f"DEBUG: {symbol} Long Rejected - Vol {vol_ratio:.2f} <= 3.5")
                return None
                
            # 4. RSI Filter (Data-optimized > 59)
            if rsi <= 59:
                # print(f"DEBUG: {symbol} Long Rejected - RSI {rsi:.2f} <= 60")
                return None
                
            # 5. ADX Filter (Data-optimized 33-60 for best win rate)
            if not (33 <= adx <= 60):
                # print(f"DEBUG: {symbol} Long Rejected - ADX {adx:.2f} not in 25-60")
                return None
            
            print(f"✅ SIGNAL FOUND: {symbol} LONG | Vol: {vol_ratio:.2f} | RSI: {rsi:.2f}")    
            return {
                'symbol': symbol,
                'side': 'LONG',
                'entry_price': current['close'],
                'timestamp': timestamp,
                'metrics': {'rsi': rsi, 'adx': adx, 'vol': vol_ratio}
            }
            
        # ----------------------------------------
        # SHORT SIGNAL
        # ----------------------------------------
        elif current['close'] < current_ema:
            # 1. EMA Deviation Cap (15% downside)
            if (current_ema - current['close']) / current_ema > 0.15:
                return None
                
            # 2. Time Filter
            if timestamp.hour in [5, 6, 7]:
                return None
                
            # 3. Volume Filter (Panic Selling)
            if vol_ratio <= 3.0:
                return None
                
            # 4. RSI Filter (Weakness < 45)
            if rsi >= 45:
                return None
                
            # 5. ADX Filter
            if not (25 <= adx <= 60):
                return None
                
            # 6. Candle Shape (Must be Red)
            if current['close'] >= current['open']:
                return None
            
            print(f"✅ SIGNAL FOUND: {symbol} SHORT | Vol: {vol_ratio:.2f} | RSI: {rsi:.2f}")
            return {
                'symbol': symbol,
                'side': 'SHORT',
                'entry_price': current['close'],
                'timestamp': timestamp,
                'metrics': {'rsi': rsi, 'adx': adx, 'vol': vol_ratio}
            }
            
        return None

    def calculate_signal_score(self, df):
        """
        Return detailed metrics and Chinese status for logging.
        """
        if df.empty or len(df) < 20:
            return {'status': '数据不足'}
            
        current = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. Trend Check
        ema20 = ta.ema(df['close'], length=20).iloc[-1]
        if current['close'] < ema20:
            diff_pct = (ema20 - current['close']) / current['close']
            return {'status': f'趋势未确认 (需涨 {diff_pct:.1%})'}
            
        # 2. Breakout Check
        if current['close'] <= prev['high']:
            diff_pct = (prev['high'] - current['close']) / current['close']
            return {'status': f'等待突破 (距前高 {diff_pct:.1%})'}
            
        # 3. Volume Check
        avg_vol = df['volume'].iloc[-21:-1].mean()
        vol_ratio = current['volume'] / avg_vol if avg_vol > 0 else 0
        if vol_ratio < 3.5:
            return {'status': f'成交量不足 (当前{vol_ratio:.1f}x, 需3.5x)'}
            
        # 4. Candle Shape Check
        if current['close'] <= current['open']:
            return {'status': '形态不佳 (阴线)'}
            
        body_size = current['close'] - current['open']
        upper_wick = current['high'] - current['close']
        wick_ratio = upper_wick / body_size if body_size > 0 else 0
        
        if wick_ratio > 0.3:
            return {'status': f'上影线过长 ({wick_ratio:.0%}, 需<30%)'}
            
        # If all pass
        # Calculate Score (0-100)
        # Base: 60
        # Volume Bonus: up to 20 (Ratio 3.5 -> 5.0)
        # Trend Bonus: up to 10 (Distance from EMA)
        # Shape Bonus: up to 10 (Wick ratio)
        
        score = 60
        
        # Volume Bonus
        vol_bonus = min(20, (vol_ratio - 3.5) * 10)
        score += vol_bonus
        
        # Shape Bonus (Lower wick ratio is better)
        shape_bonus = (0.3 - wick_ratio) * 33  # 0.3->0, 0.0->10
        score += max(0, shape_bonus)
        
        return {'status': '🔥 信号触发 (等待收盘确认) 🔥', 'score': int(score)}
