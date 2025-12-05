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
            # 1. Fetch all tickers
            tickers = self.client.get_top_gainers()
            self.top_gainers_data = tickers # Cache for logging
            
            # 2. Update Volume Ranking (Global)
            # We need 24h volume for all coins to rank them
            # Tickers data usually contains quoteVolume
            all_volumes = []
            for t in tickers:
                symbol = t[0]
                vol = float(t[1].get('quoteVolume', 0))
                all_volumes.append((symbol, vol))
            
            all_volumes.sort(key=lambda x: x[1], reverse=True)
            self.coin_volume_ranking = {sym: rank+1 for rank, (sym, _) in enumerate(all_volumes)}
            
            # 3. Filter Candidates
            new_symbols = set()
            
            # Pre-filter by change % to reduce API calls
            candidates = [t for t in tickers if self.config.CHANGE_THRESHOLD_MIN <= float(t[1]['percentage']) <= self.config.CHANGE_THRESHOLD_MAX]
            
            # Limit to checking top 30 candidates to avoid rate limits
            for t in candidates[:30]:
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
        is_btc_downtrend = False
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

        # Determine if this should be a paper trade
        is_paper_trade = False
        if should_pause:
            is_paper_trade = True
        elif is_btc_downtrend:
            is_paper_trade = True

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
                self.logger.info(f"✓ 信号检测: {symbol} 满足条件")
            
            if signal and signal['side'] == 'LONG':
                
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
            
            # Calculate Stop Loss
            # ... (Reuse existing logic or call strategy)
            # For simplicity, let's assume fixed 1.4% or similar to backtest
            # Backtest uses ATR or fixed. Let's use fixed 1.4% for safety as per backtest fallback
            stop_loss_pct = 0.014
            stop_loss_price = price * (1 - stop_loss_pct)
            
            # Calculate Quantity using Risk Manager
            # For paper trade, use real balance to simulate realistic size
            quantity = self.risk_manager.calculate_position_size(balance, price, stop_loss_price)
            
            if quantity <= 0:
                self.logger.warning(f"计算仓位为0，跳过开仓")
                return

            # PAPER TRADING EXECUTION
            if is_paper_trade:
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

            # REAL TRADING EXECUTION
            # 1. Set Margin Mode to ISOLATED (Safety First)
            self.executor.set_margin_mode(symbol, 'ISOLATED')
            
            # 2. 动态杠杆计算 (信号置信度驱动)
            if signal and 'metrics' in signal:
                # 使用信号置信度模块计算最优杠杆
                calculated_leverage = self.leverage_strategy.calculate(
                    symbol=symbol,
                    signal_metrics=signal['metrics'],
                    market_volatility=signal.get('volatility', 0) # 需要策略返回波动率
                )
                leverage = calculated_leverage
            else:
                leverage = 10 # Default fallback
                
            self.executor.set_leverage(symbol, leverage)
            
            # 3. Place Market Order
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

    def execute_entry(self, symbol, df, signal, is_paper_trade=False):
        """
        Execute entry logic: Calculate leverage, quantity, and place orders.
        """
        try:
            # 1. Calculate Dynamic Leverage
            if not is_paper_trade:
                calculated_leverage, metrics = self.leverage_strategy.calculate_dynamic_leverage(
                    symbol=symbol, 
                    signal=signal,
                    current_price=df['close'].iloc[-1],
                    df=df
                )
                confidence_score = self.leverage_strategy.get_confidence_score(signal)
                self.logger.info(f"🎯 [动态杠杆] {symbol} 置信度评分: {confidence_score}/100 → 杠杆: {calculated_leverage}x")
            else:
                # Fallback: 使用固定杠杆
                calculated_leverage = self.config.LEVERAGE
                confidence_score = 0
                # self.logger.warning(f"⚠️  [动态杠杆] {symbol} 无有效信号metrics,使用固定杠杆 {calculated_leverage}x")
            
            # 3. 安全限制: 最大杠杆25x (留安全边际)
            max_safe_leverage = 25
            max_allowed_lev = self.client.get_max_leverage(symbol)
            target_leverage = int(min(calculated_leverage, max_safe_leverage, max_allowed_lev))
            
            self.logger.info(f"[开仓流程] {symbol} 计算杠杆={calculated_leverage}x, 最大允许={max_allowed_lev}x, 最终使用={target_leverage}x")
            
            # 4. Set Leverage
            try:
                self.executor.set_leverage(symbol, target_leverage)
            except Exception as e:
                self.logger.warning(f"[开仓流程] 设置杠杆 {target_leverage}x 失败: {e}, 尝试降级到 10x...")
                target_leverage = 10
                self.executor.set_leverage(symbol, target_leverage)

            # 5. Calculate Quantity & Stop Loss (Risk-Based)
            current_price = df['close'].iloc[-1]
            balance = float(self.client.get_balance())
            
            # 5.1 Calculate ATR Stop Loss
            stop_loss_price = self.risk_manager.calculate_stop_loss(df, current_price, side='LONG')
            
            # 5.2 Calculate Position Size (Risk Manager)
            # Note: RiskManager uses account balance to determine risk amount
            quantity = self.risk_manager.calculate_position_size(balance, current_price, stop_loss_price, leverage=target_leverage)
            
            # 5.3 Sanity Check
            if quantity <= 0:
                self.logger.warning(f"❌ [开仓流程] {symbol} 计算数量为0 (可能止损太近或余额不足)")
                return

            # 5.4 Ensure Minimum Notional (Binance usually $5)
            notional = quantity * current_price
            if notional < 6:
                self.logger.warning(f"❌ [开仓流程] {symbol} 名义价值 ${notional:.2f} < $6, 放弃开仓")
                return
            
            self.logger.info(f"[开仓流程] {symbol} 最终计算: 数量={quantity:.4f}, 杠杆={target_leverage}x, 止损={stop_loss_price:.4f}")
            self.logger.info(f"📊 [置信度详情] RSI={signal['metrics'].get('rsi', 0):.1f}, Vol比={signal['metrics'].get('volume_ratio', 0):.2f}x, ADX={signal['metrics'].get('adx', 0):.1f}")
            
            if quantity <= 0:
                self.logger.warning(f"{symbol} 计算仓位为 0，跳过")
                return

            # 5. Place Market Order
            self.logger.info(f"[开仓流程] {symbol} 提交市价买单...")
            order = self.executor.place_order(symbol, 'BUY', quantity)
            
            if order:
                self.logger.info(f"✅ 开仓订单已提交: {symbol} {quantity} @ {price}")
                self.trade_logger.info(f"开仓成功: {symbol} | 数量: {quantity} | 价格: {price} | 止损: {stop_loss}")
                
                # Log Order (Entry) - 包含置信度信息
                self.recorder.log_order({
                    'order_id': order.get('id', ''),
                    'symbol': symbol,
                    'type': 'MARKET',
                    'side': 'BUY',
                    'price': price,
                    'quantity': quantity,
                    'status': 'FILLED',
                    'leverage': target_leverage,  # 记录实际使用的杠杆
                    'confidence_score': confidence_score,  # 置信度评分
                    'signal_metrics': signal['metrics'] if signal else {}  # 信号指标
                })
                
                # 6. Place Stop Loss IMMEDIATELY
                self.logger.info(f"[开仓流程] 正在立即设置止损单 @ {stop_loss}...")
                try:
                    self.executor.place_stop_loss(symbol, 'BUY', quantity, stop_loss)
                    self.logger.info(f"✅ 止损订单已提交: {symbol} @ {stop_loss}")
                except Exception as e:
                    self.logger.critical(f"❌ 止损订单提交失败 {symbol}: {e} - 请手动设置止损！")
                    self.trade_logger.critical(f"{symbol} 止损订单提交失败: {e}")
                
                # 7. (Optional) Place Take Profit if needed?
                # Strategy uses dynamic trailing, so we don't place a hard TP to avoid capping gains.
                # But we log it.
                self.logger.info(f"[开仓流程] 止盈策略: 动态ROE追踪 (15%保本, 25%锁定, 40%锁定)")
                
                self.positions[symbol] = {
                    'entry_price': price,
                    'quantity': quantity,
                    'stop_loss': stop_loss,
                    'highest_price': price,
                    'entry_time': datetime.now(),
                    'leverage': target_leverage,  # 记录杠杆用于后续监控
                    'confidence_score': confidence_score,  # 记录置信度
                    'metrics': signal['metrics'] if signal else {}  # 保存完整metrics
                }
            else:
                self.logger.error(f"[开仓流程] {symbol} 订单提交失败（exchange返回None）")
                self.trade_logger.error(f"{symbol} 订单提交失败")
                
        except Exception as e:
            self.logger.error(f"执行开仓出错 {symbol}: {e}")
            self.trade_logger.error(f"开仓失败 {symbol}: {e}")

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
            current_price = float(ticker['lastPrice'])
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
