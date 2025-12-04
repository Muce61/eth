import pandas as pd
import numpy as np
from pathlib import Path
from datetime import timedelta
from strategy.momentum import MomentumStrategy
from risk.manager import RiskManager
from config.settings import Config

from strategy.smart_exit import SmartExitModule
from strategy.quality_filter import QualityFilterModule

class RealBacktestEngine:
    def __init__(self, initial_balance=100): # User requested 100u
        self.balance = initial_balance
        self.initial_balance = initial_balance
        self.positions = {} # {symbol: {entry_price, quantity, stop_loss, ...}}
        self.trades = []
        self.pending_signal = None  # For realistic entry price simulation
        self.strategy = MomentumStrategy()
        self.risk_manager = RiskManager()
        self.config = Config()
        
        # Initialize New Modules
        self.smart_exit = SmartExitModule()
        self.quality_filter = QualityFilterModule()
        
        # Override config for backtest if needed
        self.config.MAX_OPEN_POSITIONS = 10 # Match live config
        self.config.MAX_LONG_POSITIONS = 7  # Reserve slots for longs
        self.config.MAX_SHORT_POSITIONS = 3 # Reserve 3 slots for shorts (hedging)
        self.config.LEVERAGE = 20 # Reduced from 50x for stability
        self.risk_manager.config.LEVERAGE = 20
        self.config.TRADE_MARGIN_PERCENT = 0.1 # Match live config
        self.risk_manager.config.TRADE_MARGIN_PERCENT = 0.1
        
        # Quality Filters (NEW)
        self.MIN_24H_VOLUME_USD = 10_000_000  # $10M minimum 24h volume
        self.TOP_N_COINS = 200  # Only trade top 200 coins by volume
        self.coin_volume_ranking = {}  # Will be populated in run()
        
    def load_data(self):
        """
        Load data from processed 15m directory if available, otherwise fallback to 1m -> 15m resampling.
        """
        processed_dir = Path("/Users/muce/1m_data/processed_15m_data")
        raw_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed = {}
        
        # Try loading from processed 15m directory first
        if processed_dir.exists() and any(processed_dir.iterdir()):
            print(f"Loading pre-processed 15m data from {processed_dir}...")
            data_dir = processed_dir
            is_resampled = True
        else:
            print(f"Loading raw 1m data from {raw_dir}...")
            print("Applying 1m -> 15m resampling (This may take a while)...")
            data_dir = raw_dir
            is_resampled = False
            
        # Check if directory exists
        if not data_dir.exists():
            print(f"WARNING: Data directory {data_dir} does not exist!")
            return

        for file_path in data_dir.glob("*.csv"):
            symbol = file_path.stem # e.g. BTCUSDT
            try:
                df = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
                
                if is_resampled:
                    # Already 15m, just load
                    df_15m = df
                else:
                    # Resample 1m to 15m
                    agg_dict = {
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }
                    
                    # Handle potential missing columns or different case
                    available_cols = df.columns.tolist()
                    final_agg = {}
                    for col, func in agg_dict.items():
                        if col in available_cols:
                            final_agg[col] = func
                        elif col.capitalize() in available_cols: # Try Title case
                            final_agg[col.capitalize()] = func
                            
                    if not final_agg:
                        print(f"Skipping {symbol}: No OHLCV columns found")
                        continue
                        
                    df_15m = df.resample('15min').agg(final_agg).dropna()
                
                # Normalize column names to lowercase
                df_15m.columns = [c.lower() for c in df_15m.columns]
                
                self.data_feed[symbol] = df_15m
                # if 'BTC' in symbol.upper():
                #     print(f"DEBUG: Loaded {symbol} with {len(df_15m)} rows")
            except Exception as e:
                print(f"Error loading {symbol}: {e}")
            
        print(f"Loaded {len(self.data_feed)} symbols.")
        
    def run(self, days=None, start_date=None, end_date=None):
        if not hasattr(self, 'data_feed') or not self.data_feed:
            self.load_data()
            
        # PRE-COMPUTE: Rank coins by average 24h volume (proxy for market cap)
        print("Ranking coins by average 24h volume...")
        self._rank_coins_by_volume()
        
        # 1. Align Timestamps
        all_timestamps = set()
        for df in self.data_feed.values():
            all_timestamps.update(df.index)
            
        sorted_timestamps = sorted(list(all_timestamps))
        
        # Filter by Date Range
        if start_date:
            start_ts = pd.Timestamp(start_date)
            sorted_timestamps = [t for t in sorted_timestamps if t >= start_ts]
            
        if end_date:
            end_ts = pd.Timestamp(end_date)
            sorted_timestamps = [t for t in sorted_timestamps if t <= end_ts]
            
        # Filter for last N days if requested (overrides start_date if both present, or used in conjunction?)
        # Let's make 'days' mutually exclusive or secondary to explicit dates.
        # If start_date is NOT provided but days IS provided:
        if days and not start_date:
            end_time = sorted_timestamps[-1]
            start_time = end_time - timedelta(days=days)
            sorted_timestamps = [t for t in sorted_timestamps if t >= start_time]
            print(f"Filtered to last {days} days: {len(sorted_timestamps)} candles")
        
        if not sorted_timestamps:
            print("No data found for the specified date range.")
            return

        print(f"Backtesting over {len(sorted_timestamps)} timestamps...")
        print(f"Start: {sorted_timestamps[0]}, End: {sorted_timestamps[-1]}")
        
        # 2. Iterate through time
        # Need a lookback window for 24h change calculation (24h = 96 * 15m candles)
        lookback_24h = 96
        min_history = 50 # For strategy pattern
        
        # If we filtered by days, we might need to load more history for indicators
        # But for simplicity, we just start from the filtered start index
        # In a real engine, we'd keep previous data for lookback. 
        # Here, we assume data_feed has full history, so we can just look back.
        
        # Find index of first timestamp in our filtered list
        # Actually, we are iterating by index of sorted_timestamps. 
        # If we sliced sorted_timestamps, we need to be careful about lookups in data_feed.
        # data_feed has full history, so looking up by timestamp is fine.
        
        start_index = 0 
        # But we need to ensure we have enough history BEFORE the first timestamp for indicators
        # The strategy checks history using df.loc[:current_time]. 
        # So as long as data_feed has data before sorted_timestamps[0], we are good.
        
        for i in range(start_index, len(sorted_timestamps)):
            current_time = sorted_timestamps[i]
            
            # 1. Execute pending signal (if entry time matches)
            if self.pending_signal and self.pending_signal['entry_time'] == current_time:
                symbol = self.pending_signal['symbol']
                side = self.pending_signal['side']
                entry_price = self.pending_signal['entry_price']
                metrics = self.pending_signal['metrics']
                
                # Debug: Print when executing SHORT
                if side == 'SHORT':
                    print(f"🔴 EXECUTING SHORT: {symbol} @ {entry_price} (side={side})")
                
                # Execute the entry at next open price
                self._open_position(symbol, entry_price, current_time, None, metrics, side)
                self.pending_signal = None  # Clear after execution
            
            # 2. Manage Existing Positions
            self._manage_positions(current_time)
            
            # 3. Check for New Entries (check slots by side)
            # Count current longs and shorts
            current_longs = sum(1 for pos in self.positions.values() if pos.get('side', 'LONG') == 'LONG')
            current_shorts = sum(1 for pos in self.positions.values() if pos.get('side') == 'SHORT')
            
            # Scan if any slots available
            if current_longs < self.config.MAX_LONG_POSITIONS or current_shorts < self.config.MAX_SHORT_POSITIONS:
                self._scan_market(current_time, current_longs, current_shorts)
                
            if i % 100 == 0:
                print(f"Processing {current_time}... Balance: ${self.balance:.2f}")
                
        # Debug: Check for unclosed positions
        if self.positions:
            print(f"\n⚠️ WARNING: {len(self.positions)} unclosed positions at end of backtest!")
            for sym, pos in self.positions.items():
                print(f"  - {sym} ({pos.get('side', 'LONG')}): Entry @ {pos['entry_price']:.4f}")
                
        self._generate_report(days)
        
    def _manage_positions(self, current_time):
        # Create a list of keys to iterate safely while modifying dict
        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            
            # Get current price
            if current_time not in self.data_feed[symbol].index:
                continue # No data for this timestamp
                
            current_candle = self.data_feed[symbol].loc[current_time]
            current_price = current_candle['close']
            high_price = current_candle['high']
            low_price = current_candle['low']
            
            side = pos.get('side', 'LONG')
            
            # === STRICT EXECUTION LOGIC (FIXED FOR BOTH LONG AND SHORT) ===
            if side == 'LONG':
                # 1. Check Stop Loss FIRST (Low Price for LONG)
                if low_price <= pos['stop_loss']:
                     self._close_position(symbol, pos['stop_loss'], current_time, 'Stop Loss')
                     continue

                # 2. Check Liquidation (LONG)
                liq_price = pos['entry_price'] * (1 - 0.9 / pos['leverage'])
                if low_price <= liq_price:
                    self._close_position(symbol, liq_price, current_time, 'LIQUIDATION')
                    continue

                # 3. Update Highest Price (Only if we survived the Low)
                if high_price > pos['highest_price']:
                    pos['highest_price'] = high_price
                    
            else:  # SHORT
                # 1. Check Stop Loss FIRST (High Price for SHORT - price rising)
                if high_price >= pos['stop_loss']:
                     self._close_position(symbol, pos['stop_loss'], current_time, 'Stop Loss')
                     continue

                # 2. Check Liquidation (SHORT)
                liq_price = pos['entry_price'] * (1 + 0.9 / pos['leverage'])
                if high_price >= liq_price:
                    self._close_position(symbol, liq_price, current_time, 'LIQUIDATION')
                    continue

                # 3. Update Lowest Price (for SHORT trailing)
                if low_price < pos.get('lowest_price', pos['entry_price']):
                    pos['lowest_price'] = low_price
                
            # 4. Check Smart Exit (Trailing Stop / Break-even)
            # Now it's safe to check this because we know we didn't stop out
            should_exit, exit_reason, exit_price = self.smart_exit.check_exit(pos, current_price, current_time)
            if should_exit:
                self._close_position(symbol, exit_price, current_time, exit_reason)
                continue
    
    def _rank_coins_by_volume(self):
        """
        Rank all coins by their average 24h USD volume.
        This serves as a proxy for market cap (top 200 filter).
        """
        volume_stats = {}
        
        for symbol, df in self.data_feed.items():
            if len(df) < 96:
                continue
                
            # Calculate average volume over last 500 candles (approx 5 days)
            # Volume in USD = volume * close
            avg_vol_usd = (df['volume'] * df['close']).tail(500).mean()
            volume_stats[symbol] = avg_vol_usd
            
        # Sort desc
        sorted_coins = sorted(volume_stats.items(), key=lambda x: x[1], reverse=True)
        
        # Create rank map
        self.coin_volume_ranking = {coin: rank+1 for rank, (coin, vol) in enumerate(sorted_coins)}
        print(f"Ranked {len(self.coin_volume_ranking)} coins by volume.")
        
    def _scan_market(self, current_time, current_longs, current_shorts):
        long_candidates = []
        short_candidates = []
        
        for symbol, df in self.data_feed.items():
            # Need at least 96 candles for 24h change
            # And need to be at current_time
            if current_time not in df.index:
                # if 'BTC' in symbol.upper():
                #     print(f"DEBUG: {symbol} not in index at {current_time}")
                continue
                
            # Get index location of current_time
            # Note: get_loc might be slow if index is not unique, but here it should be
            try:
                row_loc = df.index.get_loc(current_time)
                
                # Need at least 200 candles for EMA200
                if row_loc < 200:
                    # if 'BTC' in symbol.upper():
                    #     print(f"DEBUG: {symbol} row_loc {row_loc} < 200 at {current_time}")
                    continue
                    
                # Slice data up to current_time (inclusive)
                # We need enough history for indicators (EMA200, RSI, etc.)
                # Taking last 201 candles to be safe
                start_loc = row_loc - 200
                if start_loc < 0:
                    start_loc = 0
                    
                # Use iloc for slicing by position
                # We need up to current_time (row_loc is the index of current_time)
                # So we want [start_loc : row_loc + 1]
                historical_data = df.iloc[start_loc : row_loc + 1].copy()
                # Calculate 24h change
                # 24h = 96 * 15m
                current_close = df.iloc[row_loc]['close']
                prev_close = df.iloc[row_loc - 96]['close']
                
                if prev_close == 0:
                    continue
                    
                change_pct = (current_close - prev_close) / prev_close * 100
                
                # QUALITY FILTER 1: Volume Check
                # Get last 24h volume sum
                volume_24h_slice = df.iloc[row_loc - 96 : row_loc + 1]
                volume_24h_usd = (volume_24h_slice['close'] * volume_24h_slice['volume']).sum()
                
                # === QUALITY FILTER MODULE CHECK ===
                is_good, reason = self.quality_filter.check_quality(symbol, volume_24h_slice, volume_24h_usd)
                if not is_good:
                    # if 'BTC' in symbol.upper():
                    #     print(f"DEBUG: {symbol} Quality Filter Failed: {reason}")
                    continue
                
                # QUALITY FILTER 2: Top 200 Ranking Check
                if symbol not in self.coin_volume_ranking:
                    # if 'BTCUSDTUSDT' == symbol:
                    #     print(f"DEBUG: {symbol} Failed Ranking - Not in ranking list")
                    continue  # Not in top 200, skip
                
                if self.coin_volume_ranking[symbol] > self.TOP_N_COINS:
                    # if 'BTCUSDTUSDT' == symbol:
                    #     print(f"DEBUG: {symbol} Failed Ranking - Rank {self.coin_volume_ranking[symbol]} > {self.TOP_N_COINS}")
                    continue  # Ranked below 200, skip
                
                # Filter by 24h change threshold (Absolute value to allow Shorts)
                # Relaxed to capture more opportunities
                if abs(change_pct) >= self.config.CHANGE_THRESHOLD_MIN:
                    # Separate into long/short candidates based on change direction
                    candidate_data = {
                        'symbol': symbol,
                        'change': change_pct,
                        'df': df,
                        'row_loc': row_loc,
                        'volume_24h': volume_24h_usd
                    }
                    
                    # Positive change -> potential long, Negative -> potential short
                    if change_pct > 0:
                        long_candidates.append(candidate_data)
                    else:
                        short_candidates.append(candidate_data)
                else:
                    # if 'BTCUSDTUSDT' == symbol:
                    #     print(f"DEBUG: {symbol} Failed Change Threshold: {change_pct:.2f}% not in [{self.config.CHANGE_THRESHOLD_MIN}, {self.config.CHANGE_THRESHOLD_MAX}]")
                    pass
                    
            except Exception as e:
                # if 'BTC' in symbol.upper():
                #     print(f"DEBUG: {symbol} Exception in _scan_market: {e}")
                continue
                
        # Sort by Change % (Top Gainers for longs, Top Losers for shorts)
        long_candidates.sort(key=lambda x: x['change'], reverse=True)
        short_candidates.sort(key=lambda x: x['change'], reverse=False)  # Most negative first
        
        # Process based on available slots
        signals_to_process = []
        
        # Process shorts first (priority for hedging)
        if current_shorts < self.config.MAX_SHORT_POSITIONS:
            slots_available = self.config.MAX_SHORT_POSITIONS - current_shorts
            top_shorts = short_candidates[:min(slots_available, self.config.TOP_GAINER_COUNT)]
            signals_to_process.extend(top_shorts)
        
        # Process longs
        if current_longs < self.config.MAX_LONG_POSITIONS:
            slots_available = self.config.MAX_LONG_POSITIONS - current_longs
            top_longs = long_candidates[:min(slots_available, self.config.TOP_GAINER_COUNT)]
            signals_to_process.extend(top_longs)
        
        # Check Strategy for these candidates
        for cand in signals_to_process:
            symbol = cand['symbol']
            df = cand['df']
            row_loc = cand['row_loc']
            
            # Get recent history for strategy (Need > 200 for EMA200)
            # Was 50, now increasing to 205 to be safe
            start_slice = row_loc - 205
            if start_slice < 0:
                start_slice = 0
                
            history_slice = df.iloc[start_slice : row_loc + 1]
            
            signal = self.strategy.check_signal(symbol, history_slice)
            
            if signal:
                # Check if we have a slot available for this side
                if signal['side'] == 'LONG':
                    if current_longs >= self.config.MAX_LONG_POSITIONS:
                        continue  # No long slots available
                elif signal['side'] == 'SHORT':
                    if current_shorts >= self.config.MAX_SHORT_POSITIONS:
                        continue  # No short slots available
                
                # FIX P0: Entry price should be NEXT K-line's open, not current close
                # In real trading, we see the signal after current K-line closes,
                # so we can only enter at next K-line's open price
                
                # Check if next K-line exists
                if row_loc + 1 >= len(df):
                    continue  # No next K-line, skip this signal
                
                next_open_price = df.iloc[row_loc + 1]['open']
                next_time = df.iloc[row_loc + 1].name  # Timestamp of next K-line
                
                # Store signal for execution at next time step
                # We'll open the position at the BEGINNING of the next iteration
                # For now, mark this as a pending signal
                self.pending_signal = {
                    'symbol': symbol,
                    'side': signal['side'],  # Store side (LONG or SHORT)
                    'entry_price': next_open_price,
                    'entry_time': next_time,
                    'metrics': signal.get('metrics', {})
                }
                
                # Debug: Print when storing SHORT
                if signal['side'] == 'SHORT':
                    print(f"🔴 STORING SHORT SIGNAL: {symbol} for execution at {next_time}")
                
                break # Only open 1 position per scan

    def _open_position(self, symbol, price, timestamp, history_slice, metrics=None, side='LONG'):
        # DYNAMIC LEVERAGE: Tier-based leverage assignment
        # Top 50 coins: 50x
        # Top 51-200 coins: 20x
        # Others (shouldn't reach here due to filter, but fallback): 10x
        
        coin_rank = self.coin_volume_ranking.get(symbol, 999)
        
        if coin_rank <= 50:
            leverage = 50  # Mainstream coins
        elif coin_rank <= 200:
            leverage = 20  # Mid-tier coins
        else:
            leverage = 10  # Small coins (fallback)
        
        # Update risk manager leverage for this trade
        self.risk_manager.config.LEVERAGE = leverage
        
        # OPT 2: ATR-based dynamic stop-loss
        # Calculate ATR if we have history_slice, otherwise use fixed
        if history_slice is not None and len(history_slice) >= 14:
            import pandas_ta as ta
            atr = ta.atr(history_slice['high'], history_slice['low'], history_slice['close'], length=14).iloc[-1]
            
            # Calculate ATR distance
            sl_distance = atr * 2.5 # Match live config
            
            # CRITICAL: Cap stop loss at 1.4% for 50x leverage (liquidation at ~1.5%)
            max_stop_distance = price * 0.014
            sl_distance = min(sl_distance, max_stop_distance)
            
            stop_loss_pct = sl_distance / price
        else:
            # Fallback to safe 1.4%
            stop_loss_pct = 0.014
        
        # P1 FIX: Add slippage (0.05% - more realistic for small orders on liquid pairs)
        slippage = 0.0005  # 0.05% (reduced from 0.1%)
        
        if side == 'LONG':
            entry_price_with_slippage = price * (1 + slippage)
            stop_loss = entry_price_with_slippage * (1 - stop_loss_pct)
        else: # SHORT
            entry_price_with_slippage = price * (1 - slippage)
            stop_loss = entry_price_with_slippage * (1 + stop_loss_pct)
        
        # Use risk manager for size
        # Note: RiskManager calculates size based on risk amount / distance to SL
        quantity = self.risk_manager.calculate_position_size(self.balance, entry_price_with_slippage, stop_loss)
        
        # Check margin usage
        margin_used = (entry_price_with_slippage * quantity) / leverage
        
        if quantity <= 0:
            return

        # Deduct Fee (0.05% Taker)
        notional = price * quantity
        fee = notional * 0.0005
        self.balance -= fee
        
        self.positions[symbol] = {
            'entry_price': entry_price_with_slippage,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'highest_price': entry_price_with_slippage, # For trailing stop (LONG)
            'lowest_price': entry_price_with_slippage,  # For trailing stop (SHORT)
            'entry_time': timestamp,
            'leverage': leverage,
            'side': side,
            'metrics': metrics or {}
        }
        print(f"[{timestamp}] OPEN {side} {symbol} @ {entry_price_with_slippage:.4f} | SL: {stop_loss:.4f} | Size: {quantity:.2f}")

    def _close_position(self, symbol, exit_price, timestamp, reason):
        pos = self.positions[symbol]
        side = pos.get('side', 'LONG')
        
        # P1 FIX: Add exit slippage (0.05% worse price on exit)
        slippage = 0.0005  # 0.05% (reduced from 0.1%)
        
        if side == 'LONG':
            exit_price_with_slippage = exit_price * (1 - slippage)
            # PnL = (Exit - Entry) * Qty
            pnl = (exit_price_with_slippage - pos['entry_price']) * pos['quantity']
        else: # SHORT
            exit_price_with_slippage = exit_price * (1 + slippage)
            # PnL = (Entry - Exit) * Qty
            pnl = (pos['entry_price'] - exit_price_with_slippage) * pos['quantity']
        
        # Fee
        notional = exit_price_with_slippage * pos['quantity']
        fee = notional * 0.0005
        
        net_pnl = pnl - fee
        self.balance += net_pnl
        
        trade_record = {
            'symbol': symbol,
            'side': side,
            'entry_price': pos['entry_price'],
            'exit_price': exit_price_with_slippage,
            'entry_time': pos['entry_time'],
            'exit_time': timestamp,
            'pnl': net_pnl,
            'reason': reason,
            'duration': timestamp - pos['entry_time'],
            'balance_after': self.balance  # 记录交易后的账户余额（资金曲线）
        }
        # Add metrics to trade record
        if 'metrics' in pos:
            trade_record.update(pos['metrics'])
            
        self.trades.append(trade_record)
        del self.positions[symbol]
        
        print(f"[{timestamp}] CLOSE {symbol} @ {exit_price:.4f} | PnL: ${net_pnl:.2f} | Reason: {reason}")

    def _generate_report(self, days=None):
        print("\n" + "=" * 40)
        title = f"REALISTIC BACKTEST RESULTS ({days} Days)" if days else "REALISTIC BACKTEST RESULTS (Full)"
        print(title)
        print("=" * 40)
        print(f"Final Balance: ${self.balance:.2f}")
        total_return = ((self.balance - self.initial_balance)/self.initial_balance)*100
        print(f"Total Return: {total_return:.2f}%")
        print(f"Total Trades: {len(self.trades)}")
        
        if self.trades:
            wins = [t for t in self.trades if t['pnl'] > 0]
            losses = [t for t in self.trades if t['pnl'] <= 0]
            winning_trades = len(wins)
            losing_trades = len(losses)
            win_rate = winning_trades / len(self.trades) * 100
            
            print(f"Winning Trades: {winning_trades}")
            print(f"Losing Trades: {losing_trades}")
            print(f"Win Rate: {win_rate:.2f}%")
            
            avg_win = np.mean([t['pnl'] for t in wins]) if wins else 0
            avg_loss = np.mean([t['pnl'] for t in losses]) if losses else 0
            profit_factor = abs(sum([t['pnl'] for t in wins]) / sum([t['pnl'] for t in losses])) if losses else float('inf')
            
            print(f"Avg Win: ${avg_win:.2f}")
            print(f"Avg Loss: ${avg_loss:.2f}")
            print(f"Profit Factor: {profit_factor:.2f}")

            # New Metrics
            max_profit_trade = max(self.trades, key=lambda x: x['pnl'])
            max_loss_trade = min(self.trades, key=lambda x: x['pnl'])
            
            print(f"Max Profit Trade: ${max_profit_trade['pnl']:.2f} ({max_profit_trade['symbol']})")
            print(f"Max Loss Trade: ${max_loss_trade['pnl']:.2f} ({max_loss_trade['symbol']})")
            
            max_duration = max(self.trades, key=lambda x: x['duration'])['duration']
            min_duration = min(self.trades, key=lambda x: x['duration'])['duration']
            
            print(f"Max Holding Time: {max_duration}")
            print(f"Min Holding Time: {min_duration}")
            
            # Save trades to CSV
            filename = f"backtest_trades_{days}d.csv" if days else "backtest_trades.csv"
            pd.DataFrame(self.trades).to_csv(filename)
            print(f"Trades saved to {filename}")
