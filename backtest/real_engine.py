import pandas as pd
import numpy as np
from pathlib import Path
from datetime import timedelta
from strategy.momentum import MomentumStrategy
from risk.manager import RiskManager
from config.settings import Config

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
        
        # Override config for backtest if needed
        self.config.MAX_OPEN_POSITIONS = 10 # Match live config
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
        Load all CSVs from /Users/muce/1m_data/backtest_data_legacy/ into a dictionary of DataFrames.
        """
        data_dir = Path("/Users/muce/1m_data/backtest_data_legacy")
        self.data_feed = {}
        
        print("Loading data...")
        for file_path in data_dir.glob("*.csv"):
            symbol = file_path.stem # e.g. BTCUSDT
            # Convert back to standard format if needed, but internal logic uses whatever
            # Let's keep it simple: symbol string
            df = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
            self.data_feed[symbol] = df
            
        print(f"Loaded {len(self.data_feed)} symbols.")
        
    def run(self, days=None):
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
        
        # Filter for last N days if requested
        if days:
            end_time = sorted_timestamps[-1]
            start_time = end_time - timedelta(days=days)
            sorted_timestamps = [t for t in sorted_timestamps if t >= start_time]
            print(f"Filtered to last {days} days: {len(sorted_timestamps)} candles")
        
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
                entry_price = self.pending_signal['entry_price']
                metrics = self.pending_signal['metrics']
                
                # Execute the entry at next open price
                self._open_position(symbol, entry_price, current_time, None, metrics)
                self.pending_signal = None  # Clear after execution
            
            # 2. Manage Existing Positions
            self._manage_positions(current_time)
            
            # 3. Check for New Entries (only if slots available)
            if len(self.positions) < self.config.MAX_OPEN_POSITIONS:
                self._scan_market(current_time)
                
            if i % 100 == 0:
                print(f"Processing {current_time}... Balance: ${self.balance:.2f}")
                
        self._generate_report(days)
        
    def _manage_positions(self, current_time):
        # Create a list of keys to iterate safely while modifying dict
        active_symbols = list(self.positions.keys())
        
        for symbol in active_symbols:
            pos = self.positions[symbol]
            df = self.data_feed.get(symbol)
            
            if df is None or current_time not in df.index:
                continue
                
            current_candle = df.loc[current_time]
            current_price = current_candle['close']
            high_price = current_candle['high']
            low_price = current_candle['low']
            
            # Update Highest Price (for Trailing Stop)
            pos['highest_price'] = max(pos['highest_price'], high_price)
            
            # Calculate liquidation price first (needed for comparison)
            # Liquidation Price approx Entry * (1 - 1/Leverage) for Long
            # With 50x, 1/50 = 0.02
            liq_price = pos['entry_price'] * (1 - (1 / pos['leverage']) + 0.005)  # 0.5% buffer for maintenance margin
            
            # CRITICAL FIX: Check Stop Loss BEFORE Liquidation
            # In real trading, stop-loss orders execute at the stop price (if no price gap)
            # So if low_price hits stop_loss but is still above liq_price, it's a Stop Loss execution
            # If low_price hits both, we assume stop-loss triggered first (more realistic)
            
            # Check Stop Loss first
            if low_price <= pos['stop_loss']:
                # Determine if this was truly a liquidation or just stop-loss
                # If stop_loss price is very close to liq_price (within 0.3%), likely got liquidated
                # Otherwise, stop-loss order would have executed
                if low_price <= liq_price:
                    # Price fell below liquidation line
                    # But check if stop-loss is reasonably above liq (indicating stop would trigger first)
                    stop_to_liq_gap = (pos['stop_loss'] - liq_price) / pos['entry_price']
                    if stop_to_liq_gap >= 0.002:  # 0.2% gap or more = stop-loss executes first
                        self._close_position(symbol, pos['stop_loss'], current_time, 'Stop Loss')
                    else:
                        # Stop-loss too close to liq, assume liquidation happened
                        self._close_position(symbol, liq_price, current_time, 'LIQUIDATION')
                else:
                    # Normal stop-loss execution
                    self._close_position(symbol, pos['stop_loss'], current_time, 'Stop Loss')
                continue
                
            # Check Stepped Trailing Profit
            # Logic: Reach +20% ROE -> SL = +15% ROE
            #        Reach +40% ROE -> SL = +35% ROE
            #        Step = 20%, Buffer = 5%
            
            # Calculate current max ROE based on highest price since entry
            # Note: We use highest_price to determine if a threshold was reached
            max_price_gain_pct = (pos['highest_price'] - pos['entry_price']) / pos['entry_price']
            max_roe = max_price_gain_pct * pos['leverage']
            
            # OPT 3: Fast profit-taking mechanism
            # Lock in profits early to avoid turning winners into losers
            current_price_gain_pct = (current_price - pos['entry_price']) / pos['entry_price']
            current_roe = current_price_gain_pct * pos['leverage']
            
            # === PARTIAL PROFIT TAKING (NEW) ===
            # Goal: Distribute profits across multiple exits for steady returns
            
            # Level 1: ROE 15% - Take 40% off the table
            if current_roe >= 0.15 and not pos.get('partial_1_closed', False):
                partial_qty = pos['quantity'] * 0.40
                partial_pnl = partial_qty * (current_price - pos['entry_price'])
                self.balance += partial_pnl
                
                # Record partial close
                self.trades.append({
                    'symbol': symbol,
                    'entry_price': pos['entry_price'],
                    'exit_price': current_price,
                    'entry_time': pos['entry_time'],
                    'exit_time': current_time,
                    'pnl': partial_pnl,
                    'reason': 'Partial TP 15%',
                    'duration': current_time - pos['entry_time'],
                    'rsi': pos.get('rsi', 0),
                    'adx': pos.get('adx', 0),
                    'volume_ratio': pos.get('volume_ratio', 0),
                    'upper_wick_ratio': pos.get('upper_wick_ratio', 0)
                })
                
                pos['quantity'] -= partial_qty
                pos['partial_1_closed'] = True
                # Move SL to breakeven
                pos['stop_loss'] = max(pos['stop_loss'], pos['entry_price'] * 1.002)
                continue
            
            # Level 2: ROE 25% - Take another 30% (50% of remaining)
            if current_roe >= 0.25 and pos.get('partial_1_closed') and not pos.get('partial_2_closed', False):
                partial_qty = pos['quantity'] * 0.50  # 50% of remaining = 30% of original
                partial_pnl = partial_qty * (current_price - pos['entry_price'])
                self.balance += partial_pnl
                
                self.trades.append({
                    'symbol': symbol,
                    'entry_price': pos['entry_price'],
                    'exit_price': current_price,
                    'entry_time': pos['entry_time'],
                    'exit_time': current_time,
                    'pnl': partial_pnl,
                    'reason': 'Partial TP 25%',
                    'duration': current_time - pos['entry_time'],
                    'rsi': pos.get('rsi', 0),
                    'adx': pos.get('adx', 0),
                    'volume_ratio': pos.get('volume_ratio', 0),
                    'upper_wick_ratio': pos.get('upper_wick_ratio', 0)
                })
                
                pos['quantity'] -= partial_qty
                pos['partial_2_closed'] = True
                # Lock in 20% ROE
                new_sl_price = pos['entry_price'] * (1 + (0.20 / pos['leverage']))
                pos['stop_loss'] = max(pos['stop_loss'], new_sl_price)
                continue
            
            # Level 3: ROE 40% - Close all remaining
            if current_roe >= 0.40:
                self._close_position(symbol, current_price, current_time, 'Final TP 40%')
                continue
            
            # At 15% ROE, move SL to breakeven (if not already done)
            if current_roe >= 0.15 and pos['stop_loss'] < pos['entry_price']:
                new_sl_price = pos['entry_price'] * 1.002  # Breakeven + 0.2%
                pos['stop_loss'] = max(pos['stop_loss'], new_sl_price)
                # print(f"[{current_time}] {symbol} Fast Profit: ROE {current_roe*100:.1f}% -> SL to Breakeven")
            
            # At 25% ROE, lock in 12% ROE (Match Live)
            elif current_roe >= 0.25:
                target_sl_roe = 0.12
                new_sl_price = pos['entry_price'] * (1 + (target_sl_roe / pos['leverage']))
                pos['stop_loss'] = max(pos['stop_loss'], new_sl_price)
                # print(f"[{current_time}] {symbol} Fast Profit: ROE {current_roe*100:.1f}% -> SL to 12% ROE")
                
            # At 40% ROE, lock in 25% ROE (Match Live)
            elif current_roe >= 0.40:
                target_sl_roe = 0.25
                new_sl_price = pos['entry_price'] * (1 + (target_sl_roe / pos['leverage']))
                pos['stop_loss'] = max(pos['stop_loss'], new_sl_price)
                # print(f"[{current_time}] {symbol} Fast Profit: ROE {current_roe*100:.1f}% -> SL to 25% ROE")
            
            # Determine the highest threshold reached (20%, 40%, 60%...)
            # e.g. 0.25 -> 0.20. 0.45 -> 0.40.
            if max_roe >= 0.20:
                # Current bracket floor (0.2, 0.4, etc.)
                bracket_floor = int(max_roe / 0.20) * 0.20
                
                # Target SL ROE (Bracket - 5%)
                target_sl_roe = bracket_floor - 0.05
                
                # Convert ROE to Price
                # Price = Entry * (1 + ROE / Leverage)
                new_sl_price = pos['entry_price'] * (1 + (target_sl_roe / pos['leverage']))
                
                # Update SL if higher
                if new_sl_price > pos['stop_loss']:
                    pos['stop_loss'] = new_sl_price
                    # print(f"[{current_time}] {symbol} Trailing Step: Max ROE {max_roe*100:.1f}% -> SL {target_sl_roe*100:.1f}%")

            # Note: The actual closing happens in the "Check Stop Loss" block in the NEXT iteration 
            # or we can check it right here against low_price if we want instant trigger.
            # Let's check immediately to be precise.
            if low_price <= pos['stop_loss']:
                 self._close_position(symbol, pos['stop_loss'], current_time, 'Trailing Stop (Stepped)')
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
            
            # Calculate average 24h volume over the entire dataset
            df['volume_usd'] = df['close'] * df['volume']
            avg_volume_24h = df['volume_usd'].rolling(96).sum().mean()
            volume_stats[symbol] = avg_volume_24h
        
        # Sort by volume (descending)
        ranked = sorted(volume_stats.items(), key=lambda x: x[1], reverse=True)
        
        # Assign ranking (1 = highest volume)
        for rank, (symbol, vol) in enumerate(ranked, start=1):
            self.coin_volume_ranking[symbol] = rank
        
        top_200 = [s for s, r in self.coin_volume_ranking.items() if r <= 200]
        print(f"Top 200 coins identified: {len(top_200)} coins qualify")

    def _scan_market(self, current_time):
        """
        Simulate 'get_top_gainers' and strategy check.
        """
        candidates = []
        
        # 1. Calculate 24h Change for all symbols
        time_24h_ago = current_time - timedelta(hours=24)
        
        for symbol, df in self.data_feed.items():
            if current_time not in df.index:
                continue
                
            # Get price 24h ago (approximate or exact)
            try:
                # FIX P0: Use PREVIOUS K-line close to avoid look-ahead bias
                # In real trading, we can only know the 24h change AFTER the current K-line closes
                # So we should use the previous K-line's close price for calculation
                row_loc = df.index.get_loc(current_time)
                
                # Check if we have enough history (96 candles = 24h)
                if row_loc < 96:
                    continue
                
                # Use PREVIOUS close (not current close)
                previous_close = df.iloc[row_loc - 1]['close']
                price_24h_ago = df.iloc[row_loc - 96]['close']
                
                change_pct = ((previous_close - price_24h_ago) / price_24h_ago) * 100
                
                # QUALITY FILTER 1: Volume Filter (24h > $10M)
                # Calculate 24h volume in USD (last 96 candles)
                volume_24h_slice = df.iloc[row_loc - 96 : row_loc]
                volume_24h_usd = (volume_24h_slice['close'] * volume_24h_slice['volume']).sum()
                
                if volume_24h_usd < self.MIN_24H_VOLUME_USD:
                    continue  # Skip low-volume coins
                
                # QUALITY FILTER 2: Top 200 Ranking Check
                if symbol not in self.coin_volume_ranking:
                    continue  # Not in top 200, skip
                
                if self.coin_volume_ranking[symbol] > self.TOP_N_COINS:
                    continue  # Ranked below 200, skip
                
                # Filter by 24h change threshold
                if self.config.CHANGE_THRESHOLD_MIN <= change_pct <= self.config.CHANGE_THRESHOLD_MAX:
                    candidates.append({
                        'symbol': symbol,
                        'change': change_pct,
                        'df': df,
                        'row_loc': row_loc,
                        'volume_24h': volume_24h_usd
                    })
                    
            except Exception:
                continue
                
        # Sort by Change % (Top Gainers)
        candidates.sort(key=lambda x: x['change'], reverse=True)
        
        # Take top N
        top_candidates = candidates[:self.config.TOP_GAINER_COUNT]
        
        # Check Strategy for these candidates
        for cand in top_candidates:
            symbol = cand['symbol']
            df = cand['df']
            row_loc = cand['row_loc']
            
            # Get recent history for strategy (50 candles)
            history_slice = df.iloc[row_loc - 50 : row_loc + 1]
            
            signal = self.strategy.check_signal(symbol, history_slice)
            
            if signal and signal['side'] == 'LONG':
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
                    'entry_price': next_open_price,
                    'entry_time': next_time,
                    'metrics': signal.get('metrics', {})
                }
                break # Only open 1 position per scan

    def _open_position(self, symbol, price, timestamp, history_slice, metrics=None):
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
        entry_price_with_slippage = price * (1 + slippage)
        stop_loss = entry_price_with_slippage * (1 - stop_loss_pct)
        
        # Use risk manager for size
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
            'highest_price': entry_price_with_slippage,
            'entry_time': timestamp,
            'leverage': leverage,
            'metrics': metrics or {}
        }
        print(f"[{timestamp}] OPEN LONG {symbol} @ {entry_price_with_slippage:.4f} | SL: {stop_loss:.4f} (-0.75%/-15%ROE) | Size: {quantity:.2f}")

    def _close_position(self, symbol, exit_price, timestamp, reason):
        pos = self.positions[symbol]
        
        # P1 FIX: Add exit slippage (0.05% worse price on exit)
        slippage = 0.0005  # 0.05% (reduced from 0.1%)
        exit_price_with_slippage = exit_price * (1 - slippage)
        
        # PnL
        pnl = (exit_price_with_slippage - pos['entry_price']) * pos['quantity']
        
        # Fee
        notional = exit_price_with_slippage * pos['quantity']
        fee = notional * 0.0005
        
        net_pnl = pnl - fee
        self.balance += net_pnl
        
        trade_record = {
            'symbol': symbol,
            'entry_price': pos['entry_price'],
            'exit_price': exit_price_with_slippage,
            'entry_time': pos['entry_time'],
            'exit_time': timestamp,
            'pnl': net_pnl,
            'reason': reason,
            'duration': timestamp - pos['entry_time']
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
