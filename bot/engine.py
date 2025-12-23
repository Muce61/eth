
import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone
from strategy.momentum import MomentumStrategy
from strategy.quality_filter import QualityFilterModule # NEW
from bot.live_logger import LiveTradeLogger
from bot.signal_logger import SignalLogger

logger = logging.getLogger("BotEngine")

class BotEngine:
    def __init__(self, client, scanner, stream_manager, executor, risk_manager, config):
        self.client = client
        self.scanner = scanner
        self.stream = stream_manager
        self.executor = executor
        self.risk = risk_manager
        self.config = config
        
        self.strategy = MomentumStrategy()
        self.quality_filter = QualityFilterModule() # NEW
        self.live_logger = LiveTradeLogger()
        self.signal_logger = SignalLogger(log_dir="logs", filename="live_signals.csv")
        # self.leverage_strategy = SignalConfidenceModule() # Disabled to match backtest
        self.active_universe = set()
        
        # Metrics Tracking
        self.scan_tracker = {
            'total': 0, 
            'quality_reject': 0, 
            'strategy_reject': 0, 
            'signal': 0, 
            'error': 0
        }

    async def start(self):
        """
        Main Engine Loop (Consumer)
        """
        logger.info("🧠 机器人引擎已启动")
        
        # Start Universe Sync Task
        asyncio.create_task(self.sync_universe_loop())
        # Start Periodic Reporting Task (Global 15m Summary)
        asyncio.create_task(self.periodic_report_task())
        
        # FIX: Restore State from Exchange (Persistence)
        await self.risk.sync_from_exchange(self.client)
        
        # Force Initial Stream Update (Subscribe to restored positions)
        await self.stream.update_subscriptions(self.scanner.active_universe, self.risk.get_active_symbols())
        
        # Print Initial Report
        self._print_global_report()


        
        while True:
            try:
                # Consume Event
                event = await self.stream.event_queue.get()
                
                # Parse
                stream_name = event['stream']
                data = event['data']
                kline = data['k']
                
                symbol = kline['s']
                interval = kline['i']
                
                is_closed = kline['x']
                close_price = float(kline['c'])
                high_price = float(kline['h'])
                low_price = float(kline['l'])
                event_time = int(kline['T']) # Close time
                

                # ROUTING
                # ROUTING
                # Updated: Use BOTH 1s and 1m for Risk Updates to ensure redundancy.
                # RiskManager efficiently filters for active positions.
                if interval == '1s' or interval == '1m':
                    # Use 'close' or 'low' depending on logic.
                    # Risk manager handles "Has position?" check.
                    # We pass 'low' for SL check (pessimistic) and 'high' for HWM.
                    await self.risk.update_risk(symbol, current_price=close_price, high=high_price, low=low_price, event_time=event_time)
                    
                if interval == '1m' and is_closed:
                    # STRATEGY CHECK (Low Frequency - 15m Boundary)
                    # Check Alignment
                    # kline['t'] is Open Time.
                    open_time_ms = kline['t']
                    minutes_since_epoch = int(open_time_ms / 60000)
                    
                    # Logic: If 12:14 candle closes, next is 12:15.
                     # Logic: If 12:14 candle closes, next is 12:15.
                    if (minutes_since_epoch + 1) % 15 == 0:
                         # logger.info(f"⚡ 到达 15m 边界: {symbol} (正在扫描...)")
                         # Fire & Forget Strategy Task (Don't block consumer)
                         asyncio.create_task(self.process_strategy(symbol))
                
            except Exception as e:
                logger.error(f"🧠 引擎循环错误: {e}")
                await asyncio.sleep(0.1)

    async def sync_universe_loop(self):
        """
        Periodically sync active universe from Scanner -> Stream
        """
        while True:
            new_universe = self.scanner.active_universe
            active_pos = self.risk.get_active_symbols()
            
            # We sync if universe changes OR if we have new positions (Stream needs to know)
            # Actually StreamManager.update_subscriptions takes both.
            # We should check if either changed.
            
            # For simplicity, just update every loop if universe is ready?
            # Or strict diff. 
            # Simple cyclic update is safer for consistency.
            if new_universe:
                 await self.stream.update_subscriptions(new_universe, active_pos)
                
            await asyncio.sleep(10) # 10s Sync

    async def process_strategy(self, symbol):
        """
        Async Strategy Pipeline
        """
        try:
            # 0. Check if already in position (Prevent Duplicate/Pyramiding)
            if symbol in self.risk.get_active_symbols():
                # self.live_logger.log_info(f"Skipping {symbol}: Already in position")
                return

            self.scan_tracker['total'] += 1

            # 1. Fetch History (Futures API - Direct 15m)
            # Use WARMUP_CANDLES (1000) which fits in single API call (max 1500)
            limit = self.config.WARMUP_CANDLES 
            # Note: futures_klines returns raw list
            klines = await self.client.futures_klines(symbol=symbol, interval='1m', limit=limit)
            
            if not klines: return
            
            # 2. Convert to DataFrame
            # Log Processing Start
            try:
                balance = await self.executor.get_balance_usdt()
                # Use last closed candle timestamp
                proc_time_str = klines[-1][0] 
                proc_dt = datetime.fromtimestamp(proc_time_str/1000, tz=timezone.utc)
                self.live_logger.log_processing(proc_dt.strftime("%Y-%m-%d %H:%M:%S+00:00"), balance)
            except:
                pass

            df_15m = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'q_vol', 'trades', 'tb_base', 'tb_quote', 'ignore'])
            df_15m['timestamp'] = pd.to_datetime(df_15m['timestamp'], unit='ms')
            df_15m.set_index('timestamp', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df_15m[col] = df_15m[col].astype(float)
            
            # 3. (Resampling Removed - Already 15m)
            
            # CRITICAL FIX: Ensure we only use COMPLETED candles.
            # If the last row is the current developing candle (e.g. 07:30 opens, we are at 07:30:05),
            # we must DROP it to avoid "repainting" or using 5s of data as a full candle.
            # We want to trade based on the 07:15 candle that just CLOSED at 07:30:00.
            if not df_15m.empty:
                last_ts = df_15m.index[-1]
                now_utc = datetime.now(timezone.utc)
                # Check if last_ts is within the current 15m window?
                # Actually, simpler: if last_ts matches the minute we are answering for?
                # But safer: comparison.
                # If last_ts is less than 15 mins ago? 
                # No, if we are at 07:30:05. 07:30 candle starts at 07:30.
                # If last_ts == 07:30, it is developing.
                # We can't rely on 'now' perfectly if system time drifts.
                # But generally, if the last candle's close time (implied) is > now?
                # Let's use the logic: If last_ts is "close" to now (within 15m), it's probably the current one.
                # Actually, simpler: data is fetched with limit.
                # If we just DROP the last row if it equals the "current 15m floor"?
                
                # Robust Logic:
                # Calculate current 15m floor
                current_minute = now_utc.minute
                current_floor_minute = current_minute - (current_minute % 15)
                current_floor = now_utc.replace(minute=current_floor_minute, second=0, microsecond=0)
                
                # If data has the current floor (developing), drop it.
                # Note: last_ts is tz-naive or aware? 
                # df['timestamp'] was pd.to_datetime(..., unit='ms') -> naive usually unless localized?
                # engine.py line 130 uses 'tz=timezone.utc' for logging.
                # But df conversion (L136) is standard.
                # Let's assume naive matches or handle comparison safely.
                # Actually, simpler check: 
                # The 'klines' fetch included the latest data.
                # If we run at 07:30:05. We fetch. 
                # If last row is 07:30, drop it.
                
                # Let's just blindly drop the last row IF it is < 15m old?
                # No.
                
                # USE GAP CHECK:
                # If last_ts > now - 15min? 
                # No.
                
                # Let's use the "current_floor" method but be careful with TZs.
                # If df_15m index is naive, we make current_floor naive.
                last_ts_naive = last_ts.replace(tzinfo=None)
                current_floor_naive = current_floor.replace(tzinfo=None)
                
                if last_ts_naive >= current_floor_naive:
                    df_15m = df_15m.iloc[:-1]
                    # logger.debug(f"Dropped developing candle: {last_ts}")
            
            if len(df_15m) < 50: return
            
            # 4. Quality Filter Check (Backtest Alignment)
            # Calculate 24h Volume (approx from 15m data)
            # Slice last 96 candles (24h)
            vol_check_slice = df_15m.tail(96)
            volume_24h_usd = (vol_check_slice['close'] * vol_check_slice['volume']).sum()
            
            is_good, reason = self.quality_filter.check_quality(symbol, vol_check_slice, volume_24h_usd)
            
            # Prepare basic metrics for logging quality check
            last_close = df_15m['close'].iloc[-1]
            last_vol = df_15m['volume'].iloc[-1]
            
            if not is_good:
                self.scan_tracker['quality_reject'] += 1
                # logger.info(f"🚫 质量过滤拒绝 {symbol}: {reason}") # Suppress per-symbol log
                self.live_logger.log_rejection(symbol, reason)
                self.signal_logger.log_signal(
                    timestamp=df_15m.index[-1],
                    symbol=symbol,
                    stage='QUALITY',
                    status='REJECT',
                    reason=reason,
                    metrics={'volume': last_vol},
                    price=last_close
                )
                return

            self.signal_logger.log_signal(
                timestamp=df_15m.index[-1],
                symbol=symbol,
                stage='QUALITY',
                status='PASS',
                metrics={'volume': last_vol},
                price=last_close
            )

            # 5. Check Signal
            signal = self.strategy.check_signal(symbol, df_15m)
            
            if signal:
                if signal.get('status') == 'REJECTED':
                    self.scan_tracker['strategy_reject'] += 1
                    self.signal_logger.log_signal(
                        timestamp=df_15m.index[-1],
                        symbol=symbol,
                        stage='STRATEGY',
                        status='REJECT',
                        reason=signal.get('reason'),
                        metrics=signal.get('metrics', {}),
                        price=df_15m['close'].iloc[-1]
                    )
                else:
                    self.signal_logger.log_signal(
                        timestamp=df_15m.index[-1],
                        symbol=symbol,
                        stage='STRATEGY',
                        status='SIGNAL',
                        metrics=signal.get('metrics', {}),
                        price=df_15m['close'].iloc[-1]
                    )
            else:
                 # Signal is None (usually not enough data, but we filtered for that)
                 pass
            
            if signal and signal.get('side') == 'LONG':
                 self.scan_tracker['signal'] += 1
                 logger.info(f"🎯 发现信号: {symbol} 做多 (LONG)")
                 # 5. Open Position
                 entry_price = float(df_15m['close'].iloc[-1])
                 
                 # STRICT BACKTEST ALIGNMENT: Tier-based leverage assignment
                 # Get rank from Scanner (Top 200 filtered)
                 coin_rank = self.scanner.coin_volume_ranking.get(symbol, 999)
                 
                 if coin_rank <= 50:
                     leverage = 50  # Mainstream coins
                 elif coin_rank <= 200:
                     leverage = 20  # Mid-tier coins
                 else:
                     leverage = 10  # Fallback
                 
                 logger.info(f"💪 阶梯杠杆 (排名 {coin_rank}): {leverage}x")
                 
                 # 6. Risk-Based Sizing (Backtest Alignment)
                 # A. Calculate ATR Stop Loss
                 sl_price = self.risk.calculate_stop_loss(df_15m, entry_price, side='LONG')
                 
                 # B. Fetch Balance
                 balance = await self.executor.get_balance_usdt()
                 
                 # C. Calculate Quantity
                 quantity = self.risk.calculate_position_size(balance, entry_price, sl_price, leverage)
                 
                 if quantity > 0:
                     pos_result = await self.executor.open_position(
                        symbol=symbol, 
                        side='BUY', 
                        price=entry_price, 
                        leverage=leverage,
                        quantity=quantity,
                        stop_loss=sl_price
                     )
                     
                     if pos_result:
                         self.risk.register_position(
                             symbol=symbol,
                             entry_price=pos_result['entry_price'],
                             quantity=pos_result['quantity'],
                             stop_loss=pos_result['stop_loss']
                         )
                 else:
                     logger.warning(f"⚠️ {symbol} 计算数量为 0 (余: {balance}, 止损: {sl_price})")

        except Exception as e:
            self.scan_tracker['error'] += 1
            import traceback
            logger.error(f"策略错误 {symbol}: {e}\n{traceback.format_exc()}")

    async def periodic_report_task(self):
        """
        Runs every 10 seconds check. 
        If minute is 0, 15, 30, 45 AND seconds > 15, print summary.
        This allows 15s for the 15m scan logs to finish before printing summary.
        """
        logger.info("📊 定期报告任务已启动 (T+15秒报告)")
        while True:
            try:
                now = datetime.now()
                # Report only on 15m boundaries (00, 15, 30, 45) at second 15
                if now.minute % 15 == 0 and now.second >= 15:
                    self._print_global_report()
                    await asyncio.sleep(60) # Sleep 60s to avoid double report in same minute
                
                await asyncio.sleep(1) # Fast polling
            except Exception as e:
                logger.error(f"报告任务错误: {e}")
                await asyncio.sleep(60)

    def _print_global_report(self):
        report_lines = []
        report_lines.append("\n" + "="*60)
        report_lines.append(f"📊 1m 全局扫描报告 ({datetime.now().strftime('%H:%M')})")
        report_lines.append(f"总扫描: {self.scan_tracker['total']} | 信号: {self.scan_tracker['signal']}")
        report_lines.append(f"质量过滤拒绝: {self.scan_tracker['quality_reject']} | 策略拒绝: {self.scan_tracker['strategy_reject']} | 异常: {self.scan_tracker['error']}")
        report_lines.append("-" * 60)
        
        # Position Detail
        report_lines.append(self.risk.get_position_report())
        report_lines.append("="*60 + "\n")
        
        logger.info("\n".join(report_lines))
        
        # Reset counters
        for k in self.scan_tracker:
            self.scan_tracker[k] = 0
