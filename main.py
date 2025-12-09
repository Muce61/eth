import time
import threading
from datetime import datetime
from config.settings import Config
from data.binance_client import BinanceClient
from data.websocket_monitor import MarketMonitor
from strategy.momentum import MomentumStrategy
from strategy.quality_filter import QualityFilterModule as QualityFilter
from risk.manager import RiskManager
from risk.trend_reversal_detector import TrendReversalDetector
from strategy.smart_exit import SmartExitModule
from execution.executor import Executor
from monitor.logger import setup_logger
import logging
import pandas as pd

# 动态杠杆策略模块
from leverage_strategies.signal_confidence import SignalConfidenceModule

class TradingBot:
    def __init__(self):
        self.logger = setup_logger()
        self.config = Config()
        
        self.client = BinanceClient()
        self.executor = Executor()
        self.strategy = MomentumStrategy()
        
        # 初始化信号置信度模块 (动态杠杆)
        self.leverage_strategy = SignalConfidenceModule()
        self.logger.info("📊 已启用: 信号置信度驱动动态杠杆策略")
        self.risk_manager = RiskManager()
        
        # 初始化趋势反转检测器 (熔断机制)
        self.trend_detector = TrendReversalDetector()
        self.logger.info("🛡️ 已启用: 趋势反转熔断机制 (三层防御)")
        
        # 初始化智能离场模块 (对齐回测逻辑)
        self.smart_exit = SmartExitModule()
        self.logger.info("🧠 已启用: 智能离场模块 (动态追踪 + 保本止损)")
        
        self.quality_filter = QualityFilter() # New: Backtest Alignment
        
        self.active_symbols = set() # Symbols we are monitoring/trading
        self.positions = {} # {symbol: position_data}
        self.paper_positions = {} # {symbol: position_data} (虚拟持仓)
        
        self.monitor = MarketMonitor(callbacks={
            'ticker': self.on_ticker_update,
            'kline': self.on_kline_update
        })
        
        self.lock = threading.Lock() # Thread safety for shared resources
        # self.trade_logger = setup_logger('trade_logger', 'trades.log') # Disabled per user request
        self.trade_logger = logging.getLogger('trade_logger_null')
        self.trade_logger.addHandler(logging.NullHandler())
        
        from monitor.trade_recorder import TradeRecorder
        self.recorder = TradeRecorder()

        # Cache for volume ranking
        self.coin_volume_ranking = {}
        self.TOP_N_COINS = 200
        
    def start(self):
        self.logger.info("正在启动交易机器人...")
        self.trade_logger.info("交易机器人启动 - 交易日志")
        
        # 1. Initial Top Gainers Scan
        self.scan_top_gainers()
        
        # 2. Initial Historical Check (New Feature)
        self.check_historical_signals()
        
        # 3. Start WebSocket
        self.monitor.symbols = self.active_symbols
        self.monitor.start()
        
        # 4. Main Loop (Periodic Scan & Position Management)
        try:
            while True:
                time.sleep(60) # Scan every minute
                self.scan_top_gainers()
                
                # Polling Strategy Trigger (Since WS is dummy)
                # Iterate over a copy to avoid modification issues
                current_symbols = list(self.active_symbols)
                for symbol in current_symbols:
                    self.process_strategy_safe(symbol)
                    
                self.log_market_status() # New logging function
                self.manage_positions()
        except KeyboardInterrupt:
            self.stop()

    def process_strategy_safe(self, symbol):
        """
        Safely fetch data and process strategy for a symbol.
        """
        try:
            # Fetch recent klines (enough for strategy)
            # Strategy needs ~50 candles
            df = self.client.get_historical_klines(symbol, timeframe=self.config.TIMEFRAME, limit=60)
            
            # FIX: Drop the last candle if it's incomplete (active)
            # Binance API usually returns the incomplete candle as the last row
            if not df.empty:
                # Ensure timestamps are localized to UTC for comparison
                if df['timestamp'].dt.tz is None:
                     df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                
                # USER REQUEST (2025-12-09): Enable minute-level opening (Intra-bar execution)
                # Do NOT drop the last candle. Use the developing candle for signals.
                pass
            
            if not df.empty:
                self.process_strategy(symbol, df)
        except Exception as e:
            self.logger.error(f"策略处理失败 {symbol}: {e}")

    def check_historical_signals(self):
        """
        Check for signals immediately upon startup using recent history.
        """
        self.logger.info("正在执行启动时历史数据检查...")
        for symbol in self.active_symbols:
            try:
                df = self.client.get_historical_klines(symbol, timeframe=self.config.TIMEFRAME, limit=50)
                if not df.empty:
                    self.process_strategy(symbol, df)
            except Exception as e:
                self.logger.error(f"历史检查 {symbol} 失败: {e}")
        self.logger.info("启动时检查完成")

    def log_market_status(self):
        """
        Log detailed status of top 20 coins to CSV (Snapshot).
        """
        # self.logger.info("正在更新市场状态CSV...")
        csv_file = "logs/market_status.csv"
        
        data_rows = []
        for symbol in self.active_symbols:
            try:
                # Fetch latest data (small limit for check)
                df = self.client.get_historical_klines(symbol, timeframe=self.config.TIMEFRAME, limit=25)
                if df.empty:
                    continue
                    
                metrics = self.strategy.calculate_signal_score(df)
                
                price = df['close'].iloc[-1]
                
                change = 0.0
                if hasattr(self, 'top_gainers_data'):
                    for t in self.top_gainers_data:
                        if t[0] == symbol:
                            change = float(t[1]['percentage'])
                            break
                            
                volume = df['volume'].iloc[-1]
                pattern_str = "看涨" if metrics.get('pattern') else "震荡/跌"
                status_str = metrics.get('status', '未知')
                
                data_rows.append({
                    "币种": symbol,
                    "价格": price,
                    "涨幅%": change,
                    "成交量": volume,
                    "K线形态": pattern_str,
                    "状态": status_str,
                    "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
            except Exception as e:
                self.logger.error(f"获取 {symbol} 状态失败: {e}")
        
        # Write to CSV (Overwrite)
        if data_rows:
            df_status = pd.DataFrame(data_rows)
            df_status.to_csv(csv_file, index=False, encoding='utf-8-sig')

    def stop(self):
        self.logger.info("正在停止交易机器人...")
        self.monitor.stop()

    def scan_top_gainers(self):
        """
        Fetch top gainers and update active symbols.
        Now includes Quality Filter and Volume Ranking (Backtest Alignment).
        """
        try:
            # 1. Fetch ALL tickers for Global Ranking
            tickers = self.client.get_usdt_tickers()
            
            # 2. Update Volume Ranking (Global) - The "Rolling Universe"
            # We need 24h volume for all coins to rank them properly
            all_volumes = []
            for t in tickers:
                symbol = t[0]
                vol = float(t[1].get('quoteVolume', 0))
                all_volumes.append((symbol, vol))
            
            all_volumes.sort(key=lambda x: x[1], reverse=True)
            self.coin_volume_ranking = {sym: rank+1 for rank, (sym, _) in enumerate(all_volumes)}
            self.logger.info(f"市场全量扫描: 已更新 {len(self.coin_volume_ranking)} 个币种的成交量排名")
            
            # 3. Filter Candidates (Must be in Top 200 Universe)
            # Filter by Rank first
            universe_candidates = [t for t in tickers if self.coin_volume_ranking.get(t[0], 999) <= self.TOP_N_COINS]
            
            # Then Filter by Change % (Top Gainers Logic)
            gainer_candidates = [t for t in universe_candidates if self.config.CHANGE_THRESHOLD_MIN <= float(t[1].get('percentage', 0)) <= self.config.CHANGE_THRESHOLD_MAX]
            
            # Sort by Change % Descending
            gainer_candidates.sort(key=lambda x: float(x[1].get('percentage', 0)), reverse=True)
            
            # Cache for logging (Top 50)
            self.top_gainers_data = gainer_candidates[:50]
            
            new_symbols = set()
            
            # Limit to checking top 30 candidates to avoid rate limits
            # These are already: In Top 200 Volume AND have good gains
            for t in gainer_candidates[:30]:
                symbol = t[0]
                
                # Rank Check
                rank = self.coin_volume_ranking.get(symbol, 999)
                if rank > self.TOP_N_COINS:
                    # self.logger.debug(f"Skipping {symbol}: Rank {rank} > {self.TOP_N_COINS}")
                    continue
                    
                # Quality Check (Needs History)
                try:
                    # Fetch enough data for EMA96 (24h) and Volume MA
                    df = self.client.get_historical_klines(symbol, timeframe=self.config.TIMEFRAME, limit=120)
                    if df.empty: continue
                    
                    # Calculate 24h Volume
                    # Approximate by summing last 96 candles (15m * 96 = 24h)
                    volume_24h_slice = df.iloc[-96:]
                    volume_24h_usd = (volume_24h_slice['close'] * volume_24h_slice['volume']).sum()
                    
                    is_good, reason = self.quality_filter.check_quality(symbol, volume_24h_slice, volume_24h_usd)
                    
                    if is_good:
                        new_symbols.add(symbol)
                    else:
                        pass
                        # self.logger.debug(f"Skipping {symbol}: {reason}")
                        
                except Exception as e:
                    self.logger.error(f"Error checking quality for {symbol}: {e}")
            
            # Update Active Symbols
            with self.lock:
                # Add new symbols
                for s in new_symbols:
                    if s not in self.active_symbols:
                        self.active_symbols.add(s)
                        # self.monitor.subscribe(s) # Dummy monitor has no subscribe
                        self.logger.info(f"新增监控: {s}")
                
                # Remove old symbols (if not in position)
                # Keep symbols if we have a position or if they are still in top gainers
                to_remove = []
                for s in self.active_symbols:
                    if s not in new_symbols and s not in self.positions and s not in self.paper_positions:
                        to_remove.append(s)
                        
                for s in to_remove:
                    self.active_symbols.remove(s)
                    # self.monitor.unsubscribe(s) # Dummy monitor has no unsubscribe
                    self.logger.info(f"移除监控: {s}")
                    
            self.logger.info(f"当前监控列表 ({len(self.active_symbols)}): {list(self.active_symbols)}")
            
        except Exception as e:
            self.logger.error(f"扫描涨幅榜失败: {e}")

    def on_ticker_update(self, data):
        # Handle ticker updates if needed (e.g. for real-time top gainer check)
        pass

    def on_kline_update(self, data):
        """
        Callback for K-line updates.
        """
        symbol = data['s']
        kline = data['k']
        is_closed = kline['x']
        
        with self.lock:
            if symbol not in self.active_symbols:
                return

        if is_closed:
            # Fetch recent history for this symbol to run strategy
            # Optimization: Maintain local buffer instead of fetching every time
            # For now, fetch last 50 candles
            df = self.client.get_historical_klines(symbol, timeframe=self.config.TIMEFRAME, limit=50)
            
            if not df.empty:
                self.process_strategy(symbol, df)

    def process_strategy(self, symbol, df):
        # 1. Check Circuit Breaker (Global)
        current_balance = float(self.client.get_balance())
        
        # Let's use a simple peak balance tracker
        if not hasattr(self, 'peak_balance'):
            self.peak_balance = current_balance
        self.peak_balance = max(self.peak_balance, current_balance)
        
        should_pause = self.trend_detector.should_pause_trading(current_balance, self.peak_balance)
        
        # 2. Check BTC Trend (Market Regime)
        # DISABLED PER USER REQUEST (2025-12-07) - Backtest showed removing this yields 7x profit
        is_btc_downtrend = False
        '''
        try:
            btc_df = self.client.get_historical_klines('BTCUSDT', timeframe=self.config.TIMEFRAME, limit=210)
            if not btc_df.empty:
                import pandas_ta as ta
                ema200 = ta.ema(btc_df['close'], length=200).iloc[-1]
                btc_price = btc_df['close'].iloc[-1]
                if btc_price < ema200:
                    is_btc_downtrend = True
        except Exception as e:
            self.logger.warning(f"无法获取BTC趋势: {e}")
        '''
        
        # Determine if this should be a paper trade
        is_paper_trade = False
        if should_pause:
            is_paper_trade = True
        # elif is_btc_downtrend: # Disabled
        #     is_paper_trade = True

        # Check for Entry
        # LOCKING: Protect access to positions
        with self.lock:
            # If paper trade, we check paper positions
            if is_paper_trade:
                if symbol in self.paper_positions:
                    return 
            else:
                if symbol in self.positions:
                    return

            # DEBUG: Log that we're checking for a signal
            # self.logger.debug(f"检查 {symbol} 信号...")
            
            signal = self.strategy.check_signal(symbol, df)
            
            # DEBUG: Log signal check result
            if signal:
                if signal.get('status') == 'REJECTED':
                    # Log rejection details (Optional: reduce log noise by using debug)
                    self.logger.info(f"✗ 信号拒绝: {symbol} | {signal['reason']}")
                elif signal.get('side') == 'LONG':
                    self.logger.info(f"✓ 信号检测: {symbol} 满足条件")
            
            if signal and signal.get('side') == 'LONG':
                
                # Check Max Positions (Real only)
                if not is_paper_trade:
                    if len(self.positions) >= self.config.MAX_OPEN_POSITIONS:
                        active_symbol = list(self.positions.keys())[0]
                        self.logger.info(f"跳过信号 {symbol}: 已有持仓 {active_symbol}")
                        self.trade_logger.info(f"跳过信号 {symbol}: 已有持仓 {active_symbol} (上限 {self.config.MAX_OPEN_POSITIONS})")
                        return
                    
                self.logger.info(f"发现信号: {symbol} 做多 (虚拟交易: {is_paper_trade})")
                self.trade_logger.info(f"触发信号: {symbol} 做多 | 价格: {df['close'].iloc[-1]} | 虚拟: {is_paper_trade}")
                self.execute_entry(symbol, df, signal=signal, is_paper_trade=is_paper_trade)  # 传递signal对象用于动态杠杆计算
    
    # Check for Exit (Trailing Stop handled in manage_positions or here?)
    # Better here with real-time price, but we need current price.
    # df.iloc[-1]['close'] is the close of the JUST closed candle.
    # For trailing stop, we might want real-time price from ticker.
    pass

    def execute_entry(self, symbol, df, signal=None, is_paper_trade=False):
        """执行开仓 (已集成信号置信度动态杠杆)"""
        mode_str = "虚拟交易" if is_paper_trade else "实盘交易"
        self.logger.info(f"[开仓流程] 开始执行 {symbol} 开仓 ({mode_str})...")
        
        try:
            price = df['close'].iloc[-1]
            self.logger.info(f"[开仓流程] {symbol} 入场价格: {price}")
            
            balance = float(self.client.get_balance())
            self.logger.info(f"[开仓流程] 账户余额: {balance} USDT")
            
            # PAPER TRADING EXECUTION
            if is_paper_trade:
                # For paper trade, use real balance to simulate realistic size
                # Calculate Stop Loss (ATR-based)
                if len(df) >= 14:
                    import pandas_ta as ta
                    atr = ta.atr(df['high'], df['low'], df['close'], length=14).iloc[-1]
                    sl_distance = atr * 2.5
                    stop_loss_price = price - sl_distance
                else:
                    # Fallback to fixed 1.4% for safety
                    stop_loss_price = price * (1 - 0.014)

                # For paper trade, use a fixed leverage for size calculation
                paper_leverage = self.config.LEVERAGE # Or a default like 20x
                quantity = self.risk_manager.calculate_position_size(balance, price, stop_loss_price, leverage=paper_leverage)
                
                if quantity <= 0:
                    self.logger.warning(f"计算仓位为0，跳过虚拟开仓")
                    return

                position_data = {
                    'symbol': symbol,
                    'entry_price': price,
                    'quantity': quantity,
                    'stop_loss': stop_loss_price,
                    'highest_price': price,
                    'entry_time': datetime.now(),
                    'is_paper': True
                }
                self.paper_positions[symbol] = position_data
                self.logger.info(f"📝 [虚拟交易] 开仓成功: {symbol} @ {price}")
                return

            # 1. Set Margin Mode to ISOLATED (Safety First)
            self.executor.set_margin_mode(symbol, 'ISOLATED')
            
            # 2. DYNAMIC LEVERAGE (Tier-based - Aligned with Backtest)
            # Calculate leverage FIRST so we can use it for stop loss cap
            coin_rank = self.coin_volume_ranking.get(symbol, 999)
            
            if coin_rank <= 50:
                target_leverage = 50  # Top 50 coins
            elif coin_rank <= 200:
                target_leverage = 20  # Mid-tier coins
            else:
                target_leverage = 10  # Fallback
            
            # Check Max Leverage supported by Binance
            max_leverage = self.client.get_max_leverage(symbol)
            if max_leverage is None:
                max_leverage = 20 # Fallback safety
                
            leverage = min(target_leverage, max_leverage)
            
            if leverage < target_leverage:
                self.logger.info(f"⚠️ {symbol} 杠杆被限制: 目标 {target_leverage}x -> 实际 {leverage}x (最大支持)")
            
            # Log the final leverage used
            if coin_rank <= 50:
                self.logger.info(f"[开仓流程] {symbol} 使用 {leverage}x 杠杆 (Top 50 主流币, 排名: {coin_rank})")
            elif coin_rank <= 200:
                self.logger.info(f"[开仓流程] {symbol} 使用 {leverage}x 杠杆 (Top 51-200 中型币, 排名: {coin_rank})")
            else:
                self.logger.info(f"[开仓流程] {symbol} 使用 {leverage}x 杠杆 (非Top200币种, 排名: {coin_rank})")
            
            # 3. Calculate Stop Loss with leverage-based cap
            # Use ATR for dynamic stop, but cap it at 1.4% (same as backtest)
            if len(df) >= 14:
                import pandas_ta as ta
                atr = ta.atr(df['high'], df['low'], df['close'], length=14).iloc[-1]
                sl_distance = atr * 2.5
                
                # CRITICAL: Cap stop loss at 1.4% to match backtest (all leverages)
                # For 50x leverage: liquidation at ~1.5%, so 1.4% is safe
                # For 20x/10x leverage: 1.4% is conservative but aligns with backtest
                max_stop_distance = price * 0.014
                sl_distance = min(sl_distance, max_stop_distance)
                stop_loss_price = price - sl_distance
                self.logger.info(f"[开仓流程] 动态止损: ATR={atr:.6f}, SL距离={sl_distance:.6f}, 上限=1.4%")
            else:
                # Fallback to fixed 1.4% for safety
                stop_loss_price = price * (1 - 0.014)
                self.logger.info(f"[开仓流程] 固定止损: 1.4%")
            
            # 4. Calculate Quantity using Risk Manager (pass leverage)
            quantity = self.risk_manager.calculate_position_size(balance, price, stop_loss_price, leverage=leverage)
            
            if quantity <= 0:
                self.logger.warning(f"计算仓位为0，跳过开仓")
                return
            
            # 5. Set leverage on exchange
            self.executor.set_leverage(symbol, leverage)
            
            # 6. Place Market Order
            self.executor.place_order(symbol, 'BUY', quantity, 'MARKET')
            
            # 4. Place Stop Loss
            self.executor.place_stop_loss(symbol, 'BUY', quantity, stop_loss_price)
            
            # 5. Record Position
            position_data = {
                'symbol': symbol,
                'entry_price': price,
                'quantity': quantity,
                'stop_loss': stop_loss_price,
                'highest_price': price,
                'entry_time': datetime.now(),
                'leverage': leverage,
                'is_paper': False
            }
            self.positions[symbol] = position_data
            
            self.logger.info(f"✅ [实盘交易] 开仓成功: {symbol} @ {price}")
            
        except Exception as e:
            self.logger.error(f"开仓失败 {symbol}: {e}")

    def manage_positions(self):
        """
        Manage open positions (Real and Paper).
        """
        # LOCKING: Protect access to positions
        with self.lock:
            # 1. Manage Real Positions
            for symbol in list(self.positions.keys()):
                try:
                    self.manage_single_position(symbol, is_paper=False)
                except Exception as e:
                    self.logger.error(f"管理实盘仓位 {symbol} 出错: {e}")
                    
            # 2. Manage Paper Positions
            for symbol in list(self.paper_positions.keys()):
                try:
                    self.manage_single_position(symbol, is_paper=True)
                except Exception as e:
                    self.logger.error(f"管理虚拟仓位 {symbol} 出错: {e}")

    def manage_single_position(self, symbol, is_paper=False):
        if is_paper:
            pos = self.paper_positions[symbol]
        else:
            pos = self.positions[symbol]
            
        # Get current price
        # Optimization: Use local ticker cache if available, else fetch
        # For now, fetch ticker
        try:
            ticker = self.client.get_ticker(symbol)
            current_price = float(ticker['last'])
        except Exception as e:
            self.logger.error(f"获取价格失败 {symbol}: {e}")
            return
        
        # Check Exit Conditions
        
        # 1. Hard Stop Loss (Safety Net)
        # For real positions, this is also on the exchange, but we check here to sync state.
        if current_price <= pos['stop_loss']:
            pnl = (pos['stop_loss'] - pos['entry_price']) * pos['quantity']
            reason = "Stop Loss"
            self.close_position(symbol, pos['stop_loss'], reason, is_paper)
            return

        # 2. Smart Exit (Dynamic Trailing / Break-even / Time Stop)
        # Using the same logic as backtest
        should_exit, exit_reason, exit_price = self.smart_exit.check_exit(
            position=pos,
            current_price=current_price,
            current_time=datetime.now()
            # current_atr=None # We don't have ATR here yet, optional
        )
        
        if should_exit:
            # Use current_price for execution if exit_price is not specified or different
            # check_exit returns the trigger price, but we execute at market (current_price)
            self.close_position(symbol, current_price, exit_reason, is_paper)
            return

    def close_position(self, symbol, price, reason, is_paper=False):
        if is_paper:
            if symbol in self.paper_positions:
                pos = self.paper_positions[symbol]
                pnl = (price - pos['entry_price']) * pos['quantity']
                del self.paper_positions[symbol]
                self.logger.info(f"📝 [虚拟交易] 平仓 {symbol} @ {price} | PnL: ${pnl:.2f} | 原因: {reason}")
                
                # Feed result to detector
                self.trend_detector.add_trade_result(symbol, pnl, datetime.now())
                
                # Check recovery
                if self.trend_detector.check_recovery():
                    self.logger.info("✅ 虚拟交易表现良好，准备恢复实盘交易")
                
        else:
            if symbol in self.positions:
                pos = self.positions[symbol]
                quantity = pos['quantity']
                
                self.logger.info(f"正在平仓 {symbol} ({reason})...")
                
                # 1. Cancel Stop Loss
                self.executor.cancel_all_orders(symbol)
                
                # 2. Place Market Sell
                self.executor.place_order(symbol, 'SELL', quantity, 'MARKET')
                
                pnl = (price - pos['entry_price']) * quantity
                self.logger.info(f"✅ [实盘交易] 平仓完成 {symbol} @ {price} | PnL: ${pnl:.2f}")
                self.trade_logger.info(f"平仓: {symbol} | 价格: {price} | PnL: {pnl} | 原因: {reason}")
                
                del self.positions[symbol]
                
                # Feed result to detector (Real trades also count for stats)
                self.trend_detector.add_trade_result(symbol, pnl, datetime.now())
                
                # Log Trade
                self.recorder.log_trade({
                    'symbol': symbol,
                    'entry_price': pos['entry_price'],
                    'exit_price': price,
                    'quantity': quantity,
                    'pnl': pnl,
                    'reason': reason,
                    'entry_time': pos['entry_time'],
                    'exit_time': datetime.now(),
                    'leverage': pos.get('leverage', 1)
                })



if __name__ == "__main__":
    bot = TradingBot()
    bot.start()
