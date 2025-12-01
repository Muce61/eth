import time
import threading
from datetime import datetime
from config.settings import Config
from data.binance_client import BinanceClient
from data.websocket_monitor import MarketMonitor
from strategy.momentum import MomentumStrategy
from risk.manager import RiskManager
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
        
        self.active_symbols = [] # Symbols we are monitoring/trading
        self.positions = {} # {symbol: position_data}
        
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
                self.log_market_status() # New logging function
                self.manage_positions()
        except KeyboardInterrupt:
            self.stop()

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
        """
        try:
            # Fetch more than 50 to allow for filtering
            top_gainers = self.client.get_top_gainers(limit=100)
            
            # Filter using strategy logic (5% <= change <= 20%)
            qualified_symbols = self.strategy.filter_top_gainers(top_gainers)
            
            self.top_gainers_data = top_gainers # Cache full list for lookup if needed
            new_symbols = qualified_symbols
            
            # Update monitor if symbols changed
            with self.lock:
                if set(new_symbols) != set(self.active_symbols):
                    self.logger.info("监控列表发生变化，正在更新 WebSocket 订阅...")
                    self.active_symbols = new_symbols
                    
                    # Restart Monitor to subscribe to new symbols
                    # Note: Monitor restart might take time, do it outside lock? 
                    # No, active_symbols needs protection. But stopping monitor might block.
                    # Better: Update symbols, then restart monitor outside lock if possible?
                    # Monitor reads self.symbols. Let's keep it simple for now.
                    
                    if self.monitor.keep_running:
                        self.monitor.stop()
                        # Wait a bit for thread to close
                        time.sleep(1)
                        
                    self.monitor.symbols = self.active_symbols
                    self.monitor.start()
                else:
                    self.active_symbols = new_symbols
                
            self.logger.info(f"涨幅榜已更新: {len(self.active_symbols)} 个币种")
            
        except Exception as e:
            self.logger.error(f"扫描涨幅榜出错: {e}")

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
        # Check for Entry
        if symbol not in self.positions:
            # DEBUG: Log that we're checking for a signal
            # self.logger.debug(f"检查 {symbol} 信号...")
            
            signal = self.strategy.check_signal(symbol, df)
            
            # DEBUG: Log signal check result
            if signal:
                self.logger.info(f"✓ 信号检测: {symbol} 满足条件")
            
            if signal and signal['side'] == 'LONG':
                
                # Check Max Positions
                if len(self.positions) >= self.config.MAX_OPEN_POSITIONS:
                    active_symbol = list(self.positions.keys())[0]
                    self.logger.info(f"跳过信号 {symbol}: 已有持仓 {active_symbol}")
                    self.trade_logger.info(f"跳过信号 {symbol}: 已有持仓 {active_symbol} (上限 {self.config.MAX_OPEN_POSITIONS})")
                    return
                    
                self.logger.info(f"发现信号: {symbol} 做多")
                self.trade_logger.info(f"触发信号: {symbol} 做多 | 价格: {df['close'].iloc[-1]}")
                self.execute_entry(symbol, df, signal=signal)  # 传递signal对象用于动态杠杆计算
        
        # Check for Exit (Trailing Stop handled in manage_positions or here?)
        # Better here with real-time price, but we need current price.
        # df.iloc[-1]['close'] is the close of the JUST closed candle.
        # For trailing stop, we might want real-time price from ticker.
        pass

    def execute_entry(self, symbol, df, signal=None):
        """执行开仓 (已集成信号置信度动态杠杆)"""
        self.logger.info(f"[开仓流程] 开始执行 {symbol} 开仓...")
        try:
            price = df['close'].iloc[-1]
            self.logger.info(f"[开仓流程] {symbol} 入场价格: {price}")
            
            balance = self.client.get_balance()
            self.logger.info(f"[开仓流程] 账户余额: {balance} USDT")
            
            # 1. Set Margin Mode to ISOLATED (Safety First)
            self.executor.set_margin_mode(symbol, 'ISOLATED')
            
            # 2. 动态杠杆计算 (信号置信度驱动)
            if signal and 'metrics' in signal:
                # 使用信号置信度模块计算最优杠杆
                calculated_leverage = self.leverage_strategy.calculate(
                    symbol=symbol,
                    signal=signal,
                    current_price=price,
                    df=df
                )
                confidence_score = self.leverage_strategy.get_confidence_score(signal)
                self.logger.info(f"🎯 [动态杠杆] {symbol} 置信度评分: {confidence_score}/100 → 杠杆: {calculated_leverage}x")
            else:
                # Fallback: 使用固定杠杆
                calculated_leverage = self.config.LEVERAGE
                confidence_score = 0
                self.logger.warning(f"⚠️  [动态杠杆] {symbol} 无有效信号metrics,使用固定杠杆 {calculated_leverage}x")
            
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

            # 5. Calculate Quantity
            # Risk Amount = Balance * Margin% (e.g. 10%)
            risk_amount = balance * self.config.TRADE_MARGIN_PERCENT
            # Quantity = (Risk Amount * Leverage) / Price
            quantity = (risk_amount * target_leverage) / price
            
            # Calculate Stop Loss (根据杠杆调整)
            if target_leverage >= 30:
                stop_loss_pct = 0.025  # 30x: 2.5%止损
            elif target_leverage >= 20:
                stop_loss_pct = 0.035  # 20x: 3.5%止损
            else:
                stop_loss_pct = 0.045  # 10x: 4.5%止损
            
            stop_loss = price * (1 - stop_loss_pct)
            
            self.logger.info(f"[开仓流程] {symbol} 最终计算: 数量={quantity:.4f}, 杠杆={target_leverage}x, 止损={stop_loss:.4f} ({stop_loss_pct*100:.1f}%)")
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
        Comprehensive position management matching backtest logic:
        1. Base stop loss check (CRITICAL)
        2. Liquidation protection (CRITICAL)
        3. ROE-based profit taking
        4. Trailing stop
        5. Stagnation exit
        """
        if not self.positions:
            return
            
        self.logger.info(f"正在管理 {len(self.positions)} 个持仓...")
        
        from datetime import datetime, timedelta
        
        # Iterate over a copy of keys to avoid RuntimeError during modification
        for symbol in list(self.positions.keys()):
            try:
                position = self.positions[symbol]
                
                # Fetch latest price
                ticker = self.client.exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                
                # Get position details
                entry_price = position['entry_price']
                stop_loss = position['stop_loss']
                highest_price = position.get('highest_price', entry_price)
                entry_time = position.get('entry_time', datetime.now())
                leverage = self.config.LEVERAGE
                
                # Calculate ROE
                current_roe = ((current_price - entry_price) / entry_price) * leverage
                
                # ===== 1. BASE STOP LOSS CHECK (CRITICAL) =====
                if current_price <= stop_loss:
                    self.logger.critical(f"🛑 {symbol} 止损触发! 当前价: {current_price:.6f}, 止损价: {stop_loss:.6f}")
                    self.trade_logger.critical(f"{symbol} 止损平仓 @ {current_price}, ROE: {current_roe:.2%}")
                    
                    # Close position
                    self._close_position_and_log(symbol, 'Stop Loss', current_price, position)
                    continue
                
                # ===== 2. LIQUIDATION PROTECTION (CRITICAL) =====
                liq_threshold = 1 / leverage - 0.005  # 50x: -1.5%
                liq_price = entry_price * (1 - liq_threshold)
                
                if current_price <= liq_price:
                    self.logger.critical(f"⚠️ {symbol} 接近爆仓! 当前价: {current_price:.6f}, 爆仓价: {liq_price:.6f}")
                    self.trade_logger.critical(f"{symbol} 爆仓保护平仓 @ {current_price}, ROE: {current_roe:.2%}")
                    
                    # Emergency close
                    self._close_position_and_log(symbol, 'LIQUIDATION PROTECT', current_price, position)
                    continue
                
                # Update highest price
                if current_price > highest_price:
                    position['highest_price'] = current_price
                    highest_price = current_price
                    self.logger.info(f"✨ {symbol} 新高: {current_price:.6f}, ROE: {current_roe:.2%}")
                
                # ===== 3. ROE-BASED PROFIT TAKING =====
                # Move to breakeven at 15% ROE (matched to backtest)
                if current_roe >= 0.15 and stop_loss < entry_price:
                    new_sl = entry_price * 1.002  # Breakeven + 0.2%
                    self.logger.info(f"📈 {symbol} 15% ROE达成，移动止损至保本: {new_sl:.6f}")
                    position['stop_loss'] = new_sl
                    
                    # Update exchange stop loss order
                    try:
                        self.executor.cancel_all_orders(symbol)
                        self.executor.place_stop_loss(symbol, 'BUY', position['quantity'], new_sl)
                    except Exception as e:
                        self.logger.warning(f"更新止损单失败: {e}")
                
                # Lock in 12% profit at 25% ROE
                elif current_roe >= 0.25:
                    target_roe = 0.12
                    new_sl = entry_price * (1 + target_roe / leverage)
                    
                    if new_sl > stop_loss:
                        self.logger.info(f"💰 {symbol} 25% ROE达成，锁定12%利润: {new_sl:.6f}")
                        
                        # CRITICAL FIX: If price dropped below new_sl, sell immediately
                        if new_sl > current_price:
                            self.logger.warning(f"⚠️ {symbol} 价格回调过快 ({current_price} < {new_sl})，立即市价止盈!")
                            self._close_position_and_log(symbol, 'Panic Take Profit (25%)', current_price, position)
                            continue
                        
                        position['stop_loss'] = new_sl
                        try:
                            self.executor.cancel_all_orders(symbol)
                            self.executor.place_stop_loss(symbol, 'BUY', position['quantity'], new_sl)
                        except Exception as e:
                            self.logger.warning(f"更新止损单失败: {e}")
                
                # Lock in 25% profit at 40% ROE
                elif current_roe >= 0.40:
                    target_roe = 0.25
                    new_sl = entry_price * (1 + target_roe / leverage)
                    
                    if new_sl > stop_loss:
                        self.logger.info(f"🎯 {symbol} 40% ROE达成，锁定25%利润: {new_sl:.6f}")
                        
                        # CRITICAL FIX: If price dropped below new_sl, sell immediately
                        if new_sl > current_price:
                            self.logger.warning(f"⚠️ {symbol} 价格回调过快 ({current_price} < {new_sl})，立即市价止盈!")
                            self._close_position_and_log(symbol, 'Panic Take Profit (40%)', current_price, position)
                            continue
                            
                        position['stop_loss'] = new_sl
                        try:
                            self.executor.cancel_all_orders(symbol)
                            self.executor.place_stop_loss(symbol, 'BUY', position['quantity'], new_sl)
                        except Exception as e:
                            self.logger.warning(f"更新止损单失败: {e}")
                
                # ===== 4. STAGNATION EXIT =====
                time_held = datetime.now() - entry_time
                if time_held > timedelta(hours=24) and current_roe < 0.05:
                    self.logger.info(f"⏰ {symbol} 滞涨离场: 持仓{time_held}, ROE={current_roe:.2%}")
                    self.trade_logger.info(f"{symbol} 滞涨平仓 @ {current_price}, 持仓时长: {time_held}")
                    
                    # Close position
                    self._close_position_and_log(symbol, 'Stagnation', current_price, position)
                    continue
                
                # ===== 5. TRADITIONAL TRAILING STOP =====
                max_roe = ((highest_price - entry_price) / entry_price) * leverage
                
                # Stepped trailing for high ROE (>20%)
                if max_roe >= 0.20:
                    bracket_floor = int(max_roe / 0.20) * 0.20
                    target_sl_roe = bracket_floor - 0.05
                    trail_sl = entry_price * (1 + target_sl_roe / leverage)
                    
                    if trail_sl > stop_loss:
                        self.logger.info(f"🔝 {symbol} 阶梯止盈触发: max_roe={max_roe:.2%}, 新止损={trail_sl:.6f}")
                        
                        # CRITICAL FIX: If price dropped below trail_sl, sell immediately
                        if trail_sl > current_price:
                            self.logger.warning(f"⚠️ {symbol} 价格回调过快 ({current_price} < {trail_sl})，立即市价止盈!")
                            self._close_position_and_log(symbol, 'Trailing Stop (Panic)', current_price, position)
                            continue
                        
                        position['stop_loss'] = trail_sl
                        
                        try:
                            self.executor.cancel_all_orders(symbol)
                            self.executor.place_stop_loss(symbol, 'BUY', position['quantity'], trail_sl)
                        except Exception as e:
                            self.logger.warning(f"更新止损单失败: {e}")
                
                # Log position status
                self.logger.info(f"📊 {symbol}: 价格={current_price:.6f}, ROE={current_roe:.2%}, 止损={stop_loss:.6f}, 持仓={time_held}")
                    
            except Exception as e:
                self.logger.error(f"管理持仓 {symbol} 出错: {e}")

    def _close_position_and_log(self, symbol, reason, current_price, position):
        """
        Helper to close position, cancel orders, and log trade.
        """
        try:
            # 1. Close Position
            self.executor.place_order(symbol, 'SELL', position['quantity'])
            self.executor.cancel_all_orders(symbol)
            
            # 2. Calculate Metrics
            entry_price = position['entry_price']
            entry_time = position.get('entry_time', datetime.now())
            leverage = position.get('leverage', self.config.LEVERAGE)  # 使用实际杠杆
            pnl = (current_price - entry_price) * position['quantity']
            pnl_pct = (current_price - entry_price) / entry_price
            roe = pnl_pct * leverage
            holding_time_min = (datetime.now() - entry_time).total_seconds() / 60
            
            # 3. Log Trade
            self.recorder.log_trade_close({
                'symbol': symbol,
                'side': 'LONG',
                'entry_time': entry_time,
                'entry_price': entry_price,
                'quantity': position['quantity'],
                'leverage': leverage,
                'signal_score': position.get('signal_score', 0),
                'confidence_score': position.get('confidence_score', 0),  # 新增
                'rsi': position.get('metrics', {}).get('rsi', 0),  # 新增 (需从signal保存)
                'adx': position.get('metrics', {}).get('adx', 0),  # 新增
                'volume_ratio': position.get('metrics', {}).get('volume_ratio', 0),  # 新增
                'exit_time': datetime.now(),
                'exit_price': current_price,
                'exit_reason': reason,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
                'roe': roe,
                'holding_time_min': holding_time_min
            })
            
            # 4. Cleanup
            if symbol in self.positions:
                del self.positions[symbol]
                
            self.logger.info(f"✅ {symbol} 平仓完成 ({reason}): PnL=${pnl:.2f}, ROE={roe:.2%}")
            
        except Exception as e:
            self.logger.error(f"平仓日志记录失败 {symbol}: {e}")

if __name__ == "__main__":
    bot = TradingBot()
    bot.start()
