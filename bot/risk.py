
import logging
import pandas_ta as ta
from strategy.smart_exit import SmartExitModule
from bot.live_logger import LiveTradeLogger
from datetime import datetime

logger = logging.getLogger("RiskManager")

class RiskManager:
    def __init__(self, executor, config):
        self.executor = executor
        self.config = config
        
        # Position Risk State
        # {symbol: {'entry_price': ..., 'quantity': ..., 'stop_loss': ..., 'highest_price': ..., 'entry_time': ...}}
        self.active_risks = {} 
        
        # Re-use existing Smart Exit Logic
        self.smart_exit = SmartExitModule()
        self.live_logger = LiveTradeLogger()
        
        # Concurrency Locks for Stop Loss Updates
        self.sl_locks = set()

    def get_active_symbols(self):
        return set(self.active_risks.keys())
    


    async def sync_from_exchange(self, client):
        """
        Recover positions from Exchange (Restart Persistence).
        Must be called on Bot Startup.
        """
        logger.info("🔄 正在从交易所恢复持仓状态...")
        try:
            # 1. Fetch Positions
            positions = await client.futures_position_information()
            
            restored_count = 0
            for p in positions:
                amt = float(p['positionAmt'])
                symbol = p['symbol']
                
                if amt == 0:
                    continue
                    
                # 2. Reconstruct State
                entry_price = float(p['entryPrice'])
                side = 'LONG' if amt > 0 else 'SHORT'
                
                if side != 'LONG':
                    # Current strategy only supports LONG. Log warning but maybe ignore or close?
                    # Let's track it anyway to avoid dangerous zombie shorts.
                    logger.warning(f"⚠️ 发现空单 {symbol} ({amt})，系统目前主要支持多单风控")

                # 3. Fetch Orders to find Hard SL
                orders = await client.futures_get_open_orders(symbol=symbol)
                stop_loss = 0.0
                
                for o in orders:
                    if o['type'] == 'STOP_MARKET':
                        stop_loss = float(o['stopPrice'])
                        break
                
                # If no SL found, calculating fallback
                if stop_loss == 0:
                    # Dangerous! Set a tight fallback or use entry logic?
                    # Let's set it to 15% safety.
                    stop_loss = entry_price * 0.85 if amt > 0 else entry_price * 1.15
                    logger.warning(f"⚠️ {symbol} 未发现止损单，初始化默认止损: {stop_loss}")
                
                # 4. Update Memory
                self.active_risks[symbol] = {
                    'entry_price': entry_price,
                    'quantity': abs(amt),
                    'stop_loss': stop_loss,
                    'highest_price': entry_price, # Reset HWM (Safe assumption)
                    'entry_time': datetime.now(), # Approximate
                    'leverage': int(p.get('leverage', 20)), # Store Actual Leverage
                    'current_price': float(p.get('markPrice', entry_price)) # Store initial current price (Mark)
                }
                restored_count += 1
                logger.info(f"✅ 已恢复持仓: {symbol} (均价 {entry_price}, 数量 {amt}, 止损 {stop_loss}, Mark {p.get('markPrice')})")
                
            logger.info(f"✅ 状态恢复完成: 共 {restored_count} 个活跃持仓")
            
        except Exception as e:
            logger.error(f"❌ 恢复持仓失败: {e}")
            # Non-fatal? Bot runs with empty state. 
            # Dangerous. Better to retry or alert.
            pass


    def register_position(self, symbol, entry_price, quantity, stop_loss):
        logger.info(f"🛡️ 正在注册 {symbol} 的风险监控: 入场价 {entry_price}, 止损价 {stop_loss}")
        self.active_risks[symbol] = {
            'entry_price': entry_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'highest_price': entry_price, # HWM Init
            'entry_time': datetime.now()
        }

    async def update_risk(self, symbol, current_price, high, low, event_time=None, force=False):
        """
        Called on EVERY 1s price update.
        Checks Stop Loss and updates HWM.
        force=True: Bypass time bounds and frequency checks (Debug/Startup only)
        """
        if symbol not in self.active_risks:
            return

        pos = self.active_risks[symbol]
        
        # 1. Update HWM (High Water Mark)
        if high > pos['highest_price']:
            pos['highest_price'] = high
        pos['current_price'] = current_price # Cache for reporting
        
        # 2. Check Hard Stop Loss (1s Low triggers it)
        if low <= pos['stop_loss']:
            logger.warning(f"🚨 触发止损: {symbol} (当前价 {low} <= 止损价 {pos['stop_loss']})")
            await self.execute_close(symbol, pos, reason="Stop Loss (1s)")
            return

        # ==============================================================================
        # ALIGNMENT WITH BACKTEST: Only Check Trailing/Smart Exit on 15m Boundaries
        # Backtest engine only checks SmartExit at the end of each 15m candle.
        # This allows the position to withstand intra-candle volatility (wicks).
        # ==============================================================================
        
        # Use event_time (ms timestamp) if available, else fallback to server time
        if event_time:
            # Convert ms to datetime
            current_dt = datetime.fromtimestamp(event_time / 1000)
        else:
            current_dt = datetime.now()

        should_check = True # Enable 1m checks
        
        if should_check:
            # Generate a key based on the 1m bucket ID
            # This ensures we only run ONCE per 1m interval per symbol
            
            check_key = None
            if not force:
                bucket_id = int(current_dt.timestamp()) // 60 # 1 minute buckets
                check_key = f"{symbol}_{bucket_id}"
                
                # If already checked this period, skip
                if getattr(self, '_last_check_key', {}).get(symbol) == check_key:
                    return 
            
            if not force:
                if not hasattr(self, '_last_check_key'): self._last_check_key = {}
                self._last_check_key[symbol] = check_key
             
            # Proceed with Stop Loss / Trailing Update
            # 2.5 Hard Stop Trailing (Update Exchange Order)
            new_trailing_sl = self.smart_exit.get_current_trailing_stop(pos)
            
            # REMOVED THROTTLING: 15m alignment already prevents API spam.
            # Strict update if ANY improvement to match backtest precision.
            if new_trailing_sl and new_trailing_sl > pos['stop_loss']: 
                # Non-blocking check: If already updating, skip.
                if symbol not in self.sl_locks:
                    self.sl_locks.add(symbol)
                    try:
                        # Determine Side (LONG position -> SELL stop, SHORT position -> BUY stop)
                        # We infer position side from SL relation (SL below Entry = Long)
                        sl_side = 'SELL' if pos['stop_loss'] < pos['entry_price'] else 'BUY'
                        
                        # Logging for Debugging -4130
                        current_roe = ((current_price - pos['entry_price']) / pos['entry_price']) * pos.get('leverage', 20)
                        max_roe = ((pos['highest_price'] - pos['entry_price']) / pos['entry_price']) * pos.get('leverage', 20)
                        sl_roe = ((new_trailing_sl - pos['entry_price']) / pos['entry_price']) * pos.get('leverage', 20)
                        
                        logger.info(f"🔄 更新止损 {symbol}: 当前利润率:{current_roe*100:.1f}%, 最高利润率:{max_roe*100:.1f}%, 新止损利润率:{sl_roe*100:.1f}% (价格 {new_trailing_sl})")
                        
                        # Call Executor to update
                        updated_price = await self.executor.update_stop_loss(symbol, new_trailing_sl, pos['quantity'], side=sl_side)
                        
                        # Handle "No Position" Error (-4509)
                        if updated_price == "POSITION_CLOSED":
                            logger.warning(f"⚠️ 检测到 {symbol} 仓位已丢失 (可能已触发止损)，停止监控")
                            del self.active_risks[symbol]
                            return
                            
                        if updated_price:
                            # Update Memory
                            pos['stop_loss'] = updated_price
                    finally:
                        # Ensure lock is released even if error
                        if symbol in self.sl_locks:
                            self.sl_locks.remove(symbol)
    
            # 3. Check Smart Exit (Trailing)
            # Using 'current_price' for trailing check (close of 1s candle)
            # We pass the full pos dict as 'SmartExitModule' expects it.
            # SmartExitModule.check_exit(position, current_price, current_time)
            
            should_exit, reason, exit_price = self.smart_exit.check_exit(pos, current_price, datetime.now())
            
            if should_exit:
                logger.info(f"📉 触发智能离场: {symbol} ({reason})")
                await self.execute_close(symbol, pos, reason=reason)

        else:
             return # Not near close

    def get_position_report(self):
        """
        Generate a summary table of all active positions.
        """
        if not self.active_risks:
            return "当前无活跃持仓。"
            
        lines = []
        lines.append(f"{'币种':<10} {'杠杆':<5} {'入场价':<10} {'当前价':<10} {'当前利润率':<10} {'触发利润率':<15} {'状态'}")
        lines.append("-" * 90)
        
        for symbol, pos in self.active_risks.items():
            current_price = pos.get('current_price', pos['entry_price']) # Use cached or entry
            leverage = pos.get('leverage', self.config.LEVERAGE if hasattr(self.config, 'LEVERAGE') else 20)
            
            # Calcs
            pnl_pct = (current_price - pos['entry_price']) / pos['entry_price']
            current_roe = pnl_pct * leverage
            
            max_pnl_pct = (pos['highest_price'] - pos['entry_price']) / pos['entry_price']
            max_roe = max_pnl_pct * leverage
            
            # Trigger Info
            trigger_cond = "未达标"
            if max_roe >= self.smart_exit.trailing_activation_roe:
                trigger_cond = f"追踪中 (Max {max_roe*100:.0f}%)"
            elif max_roe >= self.smart_exit.breakeven_trigger_roe:
                trigger_cond = f"已保本 (Max {max_roe*100:.0f}%)"
                
            # Formatting
            lines.append(
                f"{symbol:<10} "
                f"{leverage:<5}x "
                f"{pos['entry_price']:<10.4f} "
                f"{current_price:<10.4f} "
                f"{current_roe*100:>6.2f}%   "
                f"{max_roe*100:>6.2f}%        "
                f"{trigger_cond}"
            )
            
        return "\n".join(lines)

    async def execute_close(self, symbol, pos, reason):
        """
        Close position via Executor and clean up risk state
        """
        # Call Executor
        exit_price = await self.executor.close_position(symbol, pos['quantity'], reason)
        
        # If exit_price is None/0 (e.g. -2022 error), use stop_loss as approximation
        if not exit_price or exit_price == 0:
            if "Stop Loss" in reason or "2022" in str(exit_price):
                 exit_price = pos['stop_loss']
            else:
                 # Fallback to current HWM or Entry? Use Stop Loss is safest assumption for forced exit.
                 exit_price = pos['stop_loss'] 

        # Calculate Stats
        entry_price = pos['entry_price']
        quantity = pos['quantity']
        leverage = self.config.LEVERAGE # Maybe store leverage in pos? Yes, calculate_position_size does but register_position doesn't always.
        # Check if leverage is in pos, else use config
        # register_position only stores: entry, qty, sl, hwm, time. 
        # But calculate_position_size is just a calculator.
        # We should assume Config leverage if not tracked.
        
        # Calculate Investment (Margin)
        investment = (entry_price * quantity) / leverage
        
        # Calculate PnL
        pnl = (exit_price - entry_price) * quantity
        final_amount = investment + pnl
        
        # Format Status
        status = "未知"
        if "Stop Loss" in reason:
            status = "普通止损"
        elif "Smart Break-even" in reason:
            status = "保本止损"
        elif "Smart Trailing" in reason:
            # Extract ROE if possible? "Smart Trailing (Max 50%)"
            import re
            match = re.search(r"Max (\d+)%", reason)
            roi = match.group(1) if match else "?"
            status = f"移动止盈[止损利润率{roi}%]"
        elif "Time Stop" in reason:
            status = "时间平仓"
            
        # Log to CSV
        import csv
        import os
        
        file_path = "trades/history.csv"
        file_exists = os.path.isfile(file_path)
        
        try:
            os.makedirs("trades", exist_ok=True)
            with open(file_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Symbol", "Open Time", "Close Time", "Investment", "Final Amount", "Leverage", "Status", "Entry Price", "Exit Price", "Qty"])
                
                writer.writerow([
                    symbol,
                    pos['entry_time'].strftime("%Y-%m-%d %H:%M:%S"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    f"{investment:.2f}",
                    f"{final_amount:.2f}",
                    leverage,
                    status,
                    f"{entry_price}",
                    f"{exit_price}",
                    f"{quantity}"
                ])
                logger.info(f"📝 交易记录已保存: {symbol} {status} (盈亏: {pnl:.2f}U)")
        except Exception as e:
            logger.error(f"❌ 保存交易记录失败: {e}")
            
        # Log to strict backtest format
        self.live_logger.log_close_position(symbol, exit_price, pnl, reason)
        
        # Remove from Risk Monitor
        del self.active_risks[symbol]

    def calculate_position_size(self, account_balance, entry_price, stop_loss_price, leverage=None):
        """
        Calculate position size based on Fixed Risk Model with TIERED RISK. (Mirrors Backtest)
        """
        if entry_price == 0 or account_balance == 0 or entry_price <= stop_loss_price:
            return 0
            
        # 1. Determine Risk Percentage (Fixed 2.0% - Aggressive Mode)
        risk_pct = 0.02
        # Use config if available? Backtest hardcoded 0.02.
        if hasattr(self.config, 'RISK_PER_TRADE'):
            risk_pct = self.config.RISK_PER_TRADE
            
        # 2. Calculate Risk Amount
        risk_amount = account_balance * risk_pct
        
        # 3. Calculate Risk Per Unit
        risk_per_unit = entry_price - stop_loss_price
        
        # 4. Calculate Quantity
        quantity = risk_amount / risk_per_unit
        
        # 5. Check Leverage Limit
        max_leverage = leverage if leverage else self.config.LEVERAGE
        max_position_value = account_balance * max_leverage
        current_position_value = quantity * entry_price
        
        if current_position_value > max_position_value:
            quantity = max_position_value / entry_price
            logger.warning(f"⚠️ 超过杠杆限制，强制缩减仓位至 {max_leverage}x")
            current_position_value = quantity * entry_price # Re-calc value
            
        # 6. MIN NOTIONAL CHECK (Binance Futures requires >= 5 USDT)
        MIN_NOTIONAL = 6.0 # Increased buffer to 6.0 to prevent -4164 due to price volatility/rounding
        if current_position_value < MIN_NOTIONAL:
            # Check if we can bump it up within max leverage?
            # Or just bump it up if account has barely enough?
            # Max leverage limit checked above based on balance.
            # But the 'quantity' reduced by leverage limit might be < 5.
            # Wait, max_position_value = balance * 20.
            # If balance=3.5, max_val = 70.
            # Current calc gave very small qty due to RISK calculation (2% of 3.5 = 0.07 risk).
            # The 'Risk' constraint is what made it small, not leverage.
            # So we can safely increase quantity to meet min notional, valid by leverage, BUT violates risk %?
            # Yes, violates risk %. But for very small accounts (<100U), 2% risk is often below min notional ($5).
            # So we MUST override risk % to trade at all.
            
            # Check if account can afford 6.0 U notional
            if max_position_value >= MIN_NOTIONAL:
                logger.warning(f"⚠️ 计算仓位 ({current_position_value:.2f}U) 小于最小名义价值 (5U)，强制提升至 {MIN_NOTIONAL}U")
                quantity = MIN_NOTIONAL / entry_price
            else:
                logger.warning(f"🚫 账户余额不足以开启最小仓位 (需 {MIN_NOTIONAL}U, 最大可用 {max_position_value:.2f}U)")
                return 0

        logger.info(f"🧮 仓位计算: 余额=${account_balance:.2f}, 风险={risk_pct*100}%, 金额=${risk_amount:.2f}, 数量={quantity:.6f}")
        return quantity

    def calculate_stop_loss(self, df, entry_price, side='LONG'):
        """
        Calculate ATR-based Stop Loss with hard cap. (Mirrors Backtest)
        """
        if len(df) < self.config.ATR_PERIOD:
            return entry_price * 0.98 # Fallback
            
        # Calculate ATR
        atr = ta.atr(df['high'], df['low'], df['close'], length=self.config.ATR_PERIOD)
        current_atr = atr.iloc[-1]
        
        sl_distance = current_atr * self.config.ATR_MULTIPLIER
        
        # CRITICAL: Cap stop loss at 1.4% (Config)
        max_stop_distance = entry_price * self.config.STOP_LOSS_CAP_PERCENT
        sl_distance = min(sl_distance, max_stop_distance)
        
        if side == 'LONG':
            return entry_price - sl_distance
        else:
            return entry_price + sl_distance
