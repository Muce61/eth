
import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from backtest.real_engine import RealBacktestEngine
from bot.stream import StreamManager  # Reuse live stream
from bot.executor import OrderExecutor # Needed? No, we mock execution.
from config.settings import Config

# Configure simulated logger
logger = logging.getLogger("ShadowBot")
import sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("logs/shadow.log"),
    logging.StreamHandler(sys.stdout)
])

class ShadowBacktestEngine(RealBacktestEngine):
    """
    Adapter to run RealBacktestEngine with Live Streaming Data.
    Mimics consistency verification.
    """
    def __init__(self):
        super().__init__(initial_balance=100) # Match user request
        self.config = Config() # Reload config
        self.trades_file = "trades/shadow_history.csv"
        
        # Override data feed to be dynamic
        self.data_feed = {} # {symbol: 15m_resampled_df}
        self.raw_1m_buffer = {} # {symbol: list_of_1m_candles} for resampling
        self.buffer_1s = {} # {symbol: list_of_1s_candles} for risk check
        
        # Initialize output
        self._init_trade_log()
        
    def _init_trade_log(self):
        import os
        if not os.path.exists("trades"):
            os.makedirs("trades")
        trade_log = pd.DataFrame(columns=["Symbol", "EntryTime", "ExitTime", "EntryPrice", "ExitPrice", "PnL", "Reason"])
        # We don't overwrite, we append? Or overwrite for new session?
        # Overwrite to be clean "Current Session Shadow"
        # trade_log.to_csv(self.trades_file, index=False)

    def log_shadow_trade(self, trade_dict):
        """
        Write trade to CSV
        """
        import csv, os
        file_exists = os.path.isfile(self.trades_file)
        with open(self.trades_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Symbol", "EntryTime", "ExitTime", "EntryPrice", "ExitPrice", "PnL", "Reason"])
            if not file_exists:
                writer.writeheader()
            writer.writerow(trade_dict)
            
    # OVERRIDE _open_position to log only
    def _open_position(self, symbol, price, timestamp, history_slice, metrics=None, is_paper_trade=False):
        # Call parent logic to calculate size & SL
        # But we capture the side effects (self.positions update)
        
        # We must mimic parent but redirect 'print' to 'logger' and save to CSV?
        # Actually RealBacktestEngine updates self.positions.
        super()._open_position(symbol, price, timestamp, history_slice, metrics, is_paper_trade)
        
        # Log the open event
        if symbol in self.positions:
             pos = self.positions[symbol]
             # Check if it was JUST added (timestamp match)
             if pos['entry_time'] == timestamp:
                 logger.info(f"👻 [影子开仓] {symbol} @ {price} | SL: {pos['stop_loss']}")

    # OVERRIDE _close_position to log to CSV
    def _close_position(self, symbol, exit_price, timestamp, reason):
        if symbol in self.positions:
            pos = self.positions[symbol]
            entry_price = pos['entry_price']
            qty = pos['quantity']
            pnl = (exit_price - entry_price) * qty
            
            logger.info(f"👻 [影子平仓] {symbol} @ {exit_price} | PnL: {pnl:.2f} | {reason}")
            
            self.log_shadow_trade({
                "Symbol": symbol,
                "EntryTime": pos['entry_time'],
                "ExitTime": timestamp,
                "EntryPrice": entry_price,
                "ExitPrice": exit_price,
                "PnL": pnl,
                "Reason": reason
            })
            
        # Call parent to cleanup dict
        super()._close_position(symbol, exit_price, timestamp, reason)

    async def warmup_data(self):
        """
        Fetch recent 15m data from Binance to populate self.data_feed
        """
        logger.info("🔥 正在预热影子数据 (获取最近 1000 根 15m K线)...")
        from binance import AsyncClient
        # Use config keys
        client = await AsyncClient.create(api_key=self.config.API_KEY, api_secret=self.config.SECRET)
        try:
            # Get Exchange Info to find valid USDT futures
            exchange_info = await client.futures_exchange_info()
            symbols_info = exchange_info['symbols']
            
            # Filter: USDT Futures, Trading Status
            valid_symbols = [
                s['symbol'] for s in symbols_info 
                if s['symbol'].endswith('USDT') and s['status'] == 'TRADING'
            ]
            
            # We want high volume ones. We can get 24hr ticker for volume.
            # But futures_ticker() returns all.
            tickers = await client.futures_ticker()
            # Map ticker to volume
            vol_map = {t['symbol']: float(t['quoteVolume']) for t in tickers if t['symbol'] in valid_symbols}
            
            # Sort by Volume
            top_symbols = sorted(vol_map.keys(), key=lambda s: vol_map[s], reverse=True)
            
            # Use ALL valid symbols to ensure full coverage of Live Bot's Top 200 universe
            # (Watching ~300 symbols is fine for one socket/process)
            logger.info(f"全量预热: 目标覆盖 {len(top_symbols)} 个 USDT 币种...")
            
            for i, sym in enumerate(top_symbols):
                try:
                    klines = await client.futures_klines(symbol=sym, interval='15m', limit=200)
                    if not klines: continue
                    
                    # Convert to DF
                    data = []
                    for k in klines:
                        data.append([
                            pd.to_datetime(k[0], unit='ms', utc=True),
                            float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
                        ])
                    
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df.set_index('timestamp', inplace=True)
                    
                    self.data_feed[sym] = df
                except Exception as e:
                    logger.warning(f"⚠️ 无法加载 {sym}: {e}")
                    
                if i % 10 == 0:
                   logger.info(f"已加载 {i}/{len(top_symbols)}...")
                   
            logger.info("✅ 数据预热完成")
            
        except Exception as e:
            logger.error(f"❌ 预热失败: {e}")
        finally:
            await client.close_connection()

    async def on_candle_closed(self, symbol, candle_data):
        """
        Called when a 1m candle closes.
        We aggregate it into 15m candles.
        """
        # Dictionary mode: {t, o, h, l, c, v, q, V, Q, B}
        # candle_data is dict from StreamManager
        
        # 1. Parse Data
        try:
            ts = pd.to_datetime(candle_data['t'], unit='ms', utc=True)
            # Check if this 1m candle IS the close of a 15m candle?
            # 1m Close Time = t (Open Time) + 60s ?
            # Binance stream 't' is K-line start time.
            # A 15m candle starts at 00, 15, 30, 45.
            # It ends at 14:59, 29:59...
            # The 1m candle at 14:00 (Starts 14:00) is the LAST candle of 00-15 block?
            # 00,01...14 (15 candles). Yes.
            
            # So if (minute + 1) % 15 == 0, this 1m candle is the last piece.
            
            # Maintain Raw Buffer if strict resampling needed?
            # For simplicity, we can just APPEND to the 15m DF if it completes the block?
            # Or rely on RealBacktestEngine's re-sampling?
            # RealBacktestEngine expects 15m data in data_feed.
            
            # Strategy:
            # 1. Buffer 1m candles.
            # 2. When 15m completes, merge into a Row.
            # 3. Append to self.data_feed[symbol].
            # 4. Trigger self._scan_market(timestamp_of_15m_close)
            
            if symbol not in self.raw_1m_buffer:
                self.raw_1m_buffer[symbol] = []
                
            self.raw_1m_buffer[symbol].append({
                'timestamp': ts,
                'open': float(candle_data['o']),
                'high': float(candle_data['h']),
                'low': float(candle_data['l']),
                'close': float(candle_data['c']),
                'volume': float(candle_data['v'])
            })
            
            # Check for 15m boundary
            if (ts.minute + 1) % 15 == 0:
                # This was the last minute (e.g. 14, 29...)
                # Aggregate buffer
                buffer = self.raw_1m_buffer[symbol]
                if not buffer: return
                
                df_1m = pd.DataFrame(buffer)
                
                # Create 15m Row
                # Open of first, High max, Low min, Close of last
                agg_row = {
                    'open': df_1m.iloc[0]['open'],
                    'high': df_1m['high'].max(),
                    'low': df_1m['low'].min(),
                    'close': df_1m.iloc[-1]['close'],
                    'volume': df_1m['volume'].sum()
                }
                # Timestamp is the start of the 15m block (timestamp of first 1m)
                ts_15m = df_1m.iloc[0]['timestamp']
                
                # Append to DataFeed
                row_df = pd.DataFrame([agg_row], index=[ts_15m])
                
                if symbol in self.data_feed:
                    self.data_feed[symbol] = pd.concat([self.data_feed[symbol], row_df])
                    # Keep size manageable (last 300)
                    if len(self.data_feed[symbol]) > 300:
                        self.data_feed[symbol] = self.data_feed[symbol].iloc[-300:]
                else:
                    self.data_feed[symbol] = row_df
                
                # Clear Buffer
                self.raw_1m_buffer[symbol] = []
                
                # TRIGGER ENGINE IF IT'S "MAIN" SYMBOL OR JUST PERIODICALLY?
                # RealBacktestEngine scans ALL symbols at current_time.
                # In live streaming, symbols arrive slightly async.
                # We should trigger scan ONCE per 15m block, after a short delay to ensure all data arrived?
                # Or trigger per symbol?
                # RealBacktestEngine._scan_market updates ALL.
                # Let's use an Event from ShadowBot main loop.
                pass 

        except Exception as e:
            logger.error(f"Data error {symbol}: {e}")

async def main():
    logger.info("🚀 启动影子验证进程 (Shadow Bot)...")
    from binance import AsyncClient, BinanceSocketManager
    
    # 1. Initialize Engine
    engine = ShadowBacktestEngine()
    await engine.warmup_data()
    
    # 2. Init Client & BSM for Stream
    # Use valid config keys
    client = await AsyncClient.create(api_key=engine.config.API_KEY, api_secret=engine.config.SECRET)
    bsm = BinanceSocketManager(client)
    
    # 3. Subscribe to Stream
    stream = StreamManager(client, bsm)
    
    # Get symbols from Data Feed
    symbols = list(engine.data_feed.keys()) 
    if not symbols:
        symbols = ['ETHUSDT', 'BTCUSDT'] # Fallback
        
    # Start Stream Loop
    asyncio.create_task(stream.start())
    
    # Update Subscriptions (Universe = symbols, Active = empty for now)
    await stream.update_subscriptions(set(symbols), set())
    
    logger.info("✅ 影子进程已连接数据流，正在通过回测引擎运行...")

    # 3. Main Loop
    last_scan_time = None
    
    while True:
        # Read from Stream's internal queue
        try:
            msg_wrapper = await stream.event_queue.get()
            
            # Multiplex socket returns {'stream': '...', 'data': {...}}
            if 'data' in msg_wrapper:
                event = msg_wrapper['data']
            else:
                event = msg_wrapper
                
            # Safely check for event type
            if isinstance(event, dict) and event.get('e') == 'kline':
                k = event['k']
                symbol = event['s']
                is_closed = k['x']
                stream_name = msg_wrapper['stream'] # Extract stream name
                ts = pd.to_datetime(k['t'], unit='ms', utc=True) # Define ts earlier for 1s buffer
                
                # Check if this is a 1s stream for active positions
                if 'kline_1s' in stream_name:
                    # Buffer 1s data for risk check
                    if symbol not in engine.buffer_1s: # Use engine.buffer_1s
                        engine.buffer_1s[symbol] = []
                    # Store full kline for HWM/SL checks
                    engine.buffer_1s[symbol].append({
                        'timestamp': ts,
                        'high': float(k['h']),
                        'low': float(k['l']),
                        'close': float(k['c'])
                    })
                    # Keep buffer reasonable (last 15m = 900 items)
                    if len(engine.buffer_1s[symbol]) > 1000:
                        engine.buffer_1s[symbol] = engine.buffer_1s[symbol][-1000:]
                    continue # Skip further processing for 1s candles

                if is_closed and 'kline_1m' in stream_name:
                    # Only process 1m closes
                    await engine.on_candle_closed(symbol, k)
                    
                    # Check time for 15m trigger
                    # ts is already defined above
                    
                    # Trigger Scan at XX:00, XX:15... (When we receive the close of XX:14, XX:29...)
                    # k['t'] is Open Time.
                    # If current candle is 14:00 (Open), it closes at 14:59.
                    # We receive this event at ~15:00.
                    # (14 + 1) % 15 == 0. Yes.
                    
                    if (ts.minute + 1) % 15 == 0:
                        current_15m_start = ts.replace(second=0, microsecond=0) - timedelta(minutes=ts.minute % 15)
                        engine_time = current_15m_start + timedelta(minutes=15) # We are "at" the close
                        
                        if last_scan_time != engine_time:
                             logger.info(f"⏰ 触发影子回测逻辑 @ {engine_time}")
                             
                             # CALL ENGINE LOGIC
                             engine._manage_positions(engine_time)
                             engine._scan_market(engine_time)
                             
                             # DYNAMIC SUBSCRIPTION UPDATE
                             # Active positions need 1s data.
                             active_symbols = set(engine.positions.keys())
                             # Only update if changed? 
                             # StreamManager.update_subscriptions handles diff.
                             # We pass ALL universe symbols + Active symbols.
                             # Note: universe symbols are keys of data_feed (Top 300)
                             universe = set(engine.data_feed.keys())
                             
                             # Run in background to not block loop
                             asyncio.create_task(stream.update_subscriptions(universe, active_symbols))
                             
                             last_scan_time = engine_time
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(1)

# Overwrite _manage_single_position logic in Shadow Engine Class
def _manage_single_position_override(self, symbol, current_time, is_paper=False):
    if is_paper:
        pos = self.paper_positions[symbol]
    else:
        pos = self.positions[symbol]
        
    if symbol not in self.data_feed:
        return
    
    # 1. Fallback to 1m check first (standard)
    df = self.data_feed[symbol]
    if current_time not in df.index:
        # This can happen if the 15m candle for current_time hasn't been aggregated yet
        # or if current_time is not a 15m boundary.
        # For live, current_time is always a 15m boundary.
        # If the 15m candle is not in df, it means on_candle_closed hasn't processed it yet.
        # We should not proceed without the 15m candle.
        return
        
    current_candle = df.loc[current_time]
    current_close = current_candle['close']
    
    sl_triggered = False
    
    # 2. CHECK 1S BUFFER (High Fidelity)
    if symbol in self.buffer_1s:
        buffer = self.buffer_1s[symbol]
        # Filter for the relevant 15m window? 
        # current_time is the END of the 15m window (e.g. 12:15:00).
        # We want data from 12:00:00 to 12:14:59.
        start_monitor = current_time - timedelta(minutes=15)
        
        for k1s in buffer:
            ts = k1s['timestamp']
            if ts <= start_monitor or ts > current_time:
                continue
                
            # HWM
            if k1s['high'] > pos['highest_price']:
                pos['highest_price'] = k1s['high']
            
            # SL
            if k1s['low'] <= pos['stop_loss']:
                self._close_position(symbol, pos['stop_loss'], ts, 'Stop Loss (1s Shadow)')
                sl_triggered = True
                break
    
    if sl_triggered:
        return

    # If 1s data didn't trigger, use 1m Logic (inherited check trailing)
    # But wait, we shouldn't duplicate checks.
    # The parent method does 1. File Load (fails) -> 2. 1m Fallback.
    # We replaced File Load with Buffer Check.
    # So we can just process Smart Exit now.
    
    is_15m_boundary = (current_time.minute % 15 == 0)
    
    if is_15m_boundary:
        should_exit, reason, exit_price = self.smart_exit.check_exit(
            pos, 
            current_close, 
            current_time
        )
        
        if should_exit:
            self._close_position(symbol, exit_price, current_time, reason)

# Monkey Patch the method
ShadowBacktestEngine._manage_single_position = _manage_single_position_override

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shadow Bot Stopped.")
