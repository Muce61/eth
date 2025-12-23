
import logging
import math
from binance.exceptions import BinanceAPIException, BinanceOrderException
from bot.live_logger import LiveTradeLogger

logger = logging.getLogger("OrderExecutor")

class OrderExecutor:
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.symbols_info = {} # Cache for precision
        self.live_logger = LiveTradeLogger()

    async def get_symbol_precision(self, symbol):
        """
        Fetch and cache symbol precision with RETRY logic.
        """
        if symbol in self.symbols_info:
            return self.symbols_info[symbol]

        import asyncio
        for attempt in range(3):
            try:
                info = await self.client.futures_exchange_info()
                found = False
                for s in info['symbols']:
                    if s['symbol'] == symbol:
                        self.symbols_info[symbol] = {
                            'qty': s['quantityPrecision'],
                            'price': s['pricePrecision']
                        }
                        return self.symbols_info[symbol]
                
                # If symbol not found in info?
                if not found and attempt == 2:
                     logger.error(f"❌ 交易对 {symbol} 不存在于交易所信息中")
                     
            except Exception as e:
                logger.warning(f"⚠️ 获取交易规范失败 ({symbol}), 第 {attempt+1} 次重试: {e}")
                await asyncio.sleep(1)
        
        # If all retries fail, DO NOT FALLBACK TO 2.
        # Returning None or raising error ensures we don't trade with wrong precision.
        raise ValueError(f"CRITICAL: 无法获取 {symbol} 精度，拒绝执行以防止本金损失")

    async def round_qty(self, symbol, quantity):
        prec = await self.get_symbol_precision(symbol)
        p = prec.get('qty', 0)
        if p == 0:
            return int(quantity)
        return round(quantity, p)

    async def round_price(self, symbol, price):
        prec = await self.get_symbol_precision(symbol)
        p = prec.get('price', 2)
        if p == 0:
            return int(price)
        return round(price, p)
    
    async def get_balance_usdt(self):
        """
        Fetch USDT Balance (Paper or Real)
        """
        if self.config.PAPER_MODE:
            return 1000.0 # Paper Mock
            
        try:
            balances = await self.client.futures_account_balance()
            return next((float(b['balance']) for b in balances if b['asset'] == 'USDT'), 0.0)
        except Exception as e:
            logger.error(f"❌ 无法获取余额: {e}")
            return 0.0

    async def open_position(self, symbol, side, amount_usdt=None, price=None, leverage=None, quantity=None, stop_loss=None):
        """
        Open Futures Position (Market Order).
        Supports calculated quantity/SL or auto-calc based on margin.
        """
        try:
            # 1. Determine Position Size (Margin)
            if leverage is None:
                leverage = self.config.LEVERAGE

            # A. Explicit Quantity provided (from Risk Manager)
            if quantity is not None:
                qty_str = str(await self.round_qty(symbol, quantity))
                
            # B. Calculate from Margin
            else:
                if amount_usdt is None:
                    balance = await self.get_balance_usdt()
                    amount_usdt = balance * self.config.TRADE_MARGIN_PERCENT

                # Notional Size = Margin * Leverage
                notional = amount_usdt * leverage
                qty_calc = notional / price
                qty_str = str(await self.round_qty(symbol, qty_calc))
            
            # PAPER TRADING CHECK
            if self.config.PAPER_MODE:
                sl_val = stop_loss if stop_loss else (price * 0.98)
                logger.info(f"📝 [模拟] 开仓 {symbol}: {side} {qty_str} @ {price} (杠杆 {leverage}x, 止损 {sl_val})")
                return {
                    'symbol': symbol,
                    'entry_price': price,
                    'quantity': float(qty_str),
                    'stop_loss': sl_val, 
                    'leverage': leverage,
                    'is_paper': True
                }

            logger.info(f"📤 正在下单 {symbol}: {side} {qty_str} (杠杆 {leverage}x)")
            
            # 1.5 Cancel Any Existing Open Orders (Robust Loop)
            # Ensure no residual orders block our new position's SL/TP
            try:
                for _ in range(3):
                    await self.client.futures_cancel_all_open_orders(symbol=symbol)
                    # Verify empty
                    open_orders = await self.client.futures_get_open_orders(symbol=symbol)
                    if not open_orders:
                        logger.info(f"🧹 已清理 {symbol} 的历史挂单")
                        break
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"⚠️ 清理挂单过程异常: {e}")

            # 2. Set Leverage
            try:
                await self.client.futures_change_leverage(symbol=symbol, leverage=leverage)
            except Exception as e:
                logger.warning(f"⚠️ 设置杠杆失败 (可能已设置): {e}")

            # 3. Place Market Order
            order = await self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=qty_str
            )
            
            avg_price = float(order.get('avgPrice', 0.0))
            if avg_price == 0.0:
                avg_price = float(price)
            logger.info(f"✅ 市价单已成交: {symbol} @ {avg_price} (数量: {qty_str})")

            # 4. Place Stop Loss (Exchange Side)
            # If explicit SL provided, use it. Else calculate Cap.
            if stop_loss:
                sl_price = await self.round_price(symbol, stop_loss)
            else:
                sl_pct = self.config.STOP_LOSS_CAP_PERCENT
                sl_val = avg_price * (1 - sl_pct) if side == 'BUY' else avg_price * (1 + sl_pct)
                sl_price = await self.round_price(symbol, sl_val)
            
            # Robust SL Placement with Retry for -4130
            try:
                sl_side = 'SELL' if side == 'BUY' else 'BUY'
                await self.client.futures_create_order(
                    symbol=symbol,
                    side=sl_side,
                    type='STOP_MARKET',
                    stopPrice=str(sl_price),
                    closePosition='true' # Important for Futures
                )
                logger.info(f"🛡️ 交易所止损已设置: {symbol} @ {sl_price}")
            except BinanceAPIException as e:
                if e.code == -4130:
                    logger.warning(f"⚠️ 止损冲突 (-4130)，尝试再次清理并重试...")
                    await asyncio.sleep(1.0)
                    try:
                        # Force cancel again
                        await self.client.futures_cancel_all_open_orders(symbol=symbol)
                        await asyncio.sleep(0.5)
                        # Retry SL
                        await self.client.futures_create_order(
                            symbol=symbol,
                            side=sl_side,
                            type='STOP_MARKET',
                            stopPrice=str(sl_price),
                            closePosition='true'
                        )
                        logger.info(f"🛡️ (重试成功) 交易所止损已设置: {symbol} @ {sl_price}")
                    except Exception as e2:
                        logger.error(f"❌ 重试设置止损仍失败: {e2}")
                else:
                     logger.error(f"❌ 无法设置交易所止损: {e}")
            except Exception as e:
                logger.error(f"❌ 无法设置交易所止损 (未知错误): {e}")

            # Return Details for Risk Manager
            
            # Log Open to "Backtest Style" Log
            self.live_logger.log_open_position(symbol, avg_price, sl_price, float(qty_str))
            
            return {
                'symbol': symbol,
                'entry_price': avg_price,
                'quantity': float(qty_str),
                'stop_loss': sl_price 
            }

        except BinanceAPIException as e:
            logger.error(f"❌ 币安 API 错误 (开仓 {symbol}): {e}")
        except Exception as e:
            logger.error(f"❌ 执行错误 (开仓 {symbol}): {e}")

        return None

    async def close_position(self, symbol, quantity, reason="Unknown"):
        """
        Close Futures Position (Market)
        """
        logger.info(f"📤 正在平仓 {symbol}: 原因 {reason}")
        try:
            # PAPER TRADING CHECK
            if self.config.PAPER_MODE:
                logger.info(f"📝 [模拟] 平仓 {symbol} ({reason})")
                return

            # 1. Cancel All Open Orders (SL/TP)
            await self.client.futures_cancel_all_open_orders(symbol=symbol)
            
            # 2. Market Close
            qty_str = str(await self.round_qty(symbol, quantity))
            side = 'SELL' 
            
            order = await self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=qty_str,
                reduceOnly=True
            )
            
            avg_price = float(order.get('avgPrice', 0.0))
            if avg_price == 0.0:
                # Try to fallback to current market price if API returns 0 (handling Async)
                # But for logs, 0 might indicate partial fill or immediate return.
                # Just return 0 and handle in Risk.
                pass
            
            logger.info(f"✅ {symbol} 已平仓 ({reason}) @ {avg_price}")
            return avg_price
            
        except BinanceAPIException as e:
            if e.code == -2022:
                logger.warning(f"⚠️ 平仓被拒绝 (可能已触发交易所止损/无仓位): {e}")
                return 0.0 # Return 0 to indicate "Closed but price unknown/already gone"
            logger.error(f"❌ 币安 API 错误 (平仓 {symbol}): {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 平仓失败 {symbol}: {e}")
            return None

    async def update_stop_loss(self, symbol, new_stop_price, quantity, side='SELL'):
        """
        Update the hard Stop Loss order on the exchange.
        Includes ROBUST cancellation verification.
        """
        import asyncio
        try:
            # 1. ROBUST CANCELLATION LOOP
            for attempt in range(3):
                # A. Cancel
                await self.client.futures_cancel_all_open_orders(symbol=symbol)
                await asyncio.sleep(1.0) # Wait for propagation
                
                # B. Verify
                open_orders = await self.client.futures_get_open_orders(symbol=symbol)
                if not open_orders:
                    break # Clean!
                
                logger.warning(f"⚠️ 挂单清理未完成 ({len(open_orders)} 个残留)，第 {attempt+1} 次重试...")
                await asyncio.sleep(1.0)
            
            # 2. Round Price
            sl_price = await self.round_price(symbol, new_stop_price)
            
            # 3. Place New Order with Retry Logic
            try:
                await self.client.futures_create_order(
                    symbol=symbol,
                    side=side, 
                    type='STOP_MARKET',
                    stopPrice=str(sl_price),
                    closePosition='true'
                )
            except BinanceAPIException as e:
                # Retry if Duplicate Order Error (-4130) or Immediate Trigger (-2021)
                if e.code == -4130 or e.code == -2021:
                    logger.warning(f"⚠️ 更新止损受阻 (Code {e.code})，检查是否需要立即平仓...")
                    
                    # 4. PRICE CHECK FALLBACK (The Backtest Alignment Fix)
                    try:
                        ticker = await self.client.futures_symbol_ticker(symbol=symbol)
                        current_price = float(ticker['price'])
                        
                        # Check if stop is breached
                        is_breached = False
                        if side == 'SELL' and current_price <= float(sl_price):
                            is_breached = True
                        elif side == 'BUY' and current_price >= float(sl_price):
                            is_breached = True
                            
                        # If breached OR very close (within 0.1%), Force Close
                        if is_breached or e.code == -2021:
                            logger.warning(f"🚨 止损价 {sl_price} 已触发或极其接近 (当前 {current_price}) - 执行市价平仓以对齐策略!")
                            # PASS ACTUAL QUANTITY HERE
                            await self.close_position(symbol, quantity=quantity, reason="Hard Stop Triggered (Fallback)")
                            return sl_price # Return "success" as position is closed (Goal achieved)
                        else:
                            # Not breached, but API rejected. Last try.
                            logger.warning(f"⚠️ 价格未突破但订单被拒，尝试最终强制重置...")
                            await self.client.futures_cancel_all_open_orders(symbol=symbol)
                            await asyncio.sleep(1.0)
                            
                            try:
                                await self.client.futures_create_order(
                                    symbol=symbol,
                                    side=side, 
                                    type='STOP_MARKET',
                                    stopPrice=str(sl_price),
                                    closePosition='true'
                                )
                                logger.info(f"🛡️ (重试成功) 交易所止损已设置: {symbol} @ {sl_price}")
                            except BinanceAPIException as final_e:
                                if final_e.code == -4130:
                                    logger.error(f"❌ 最终重试仍遇冲突 (-4130)，无法设置止损! 执行安全平仓以保护利润。")
                                    # PASS ACTUAL QUANTITY HERE
                                    await self.close_position(symbol, quantity=quantity, reason="Unable to Set Stop (Safety Exit)")
                                    return float(current_price) # Approximate exit price
                                else:
                                    raise final_e

                    except Exception as crash_e:
                        logger.error(f"❌ 止损兜底逻辑执行失败: {crash_e}")
                        return None

                elif e.code == -4509:
                    logger.warning(f"⚠️ 无法更新止损 (-4509): 仓位可能已不存在")
                    return "POSITION_CLOSED"
                else:
                    logger.error(f"❌ 无法设置交易所止损: {e}")
                    raise e
            
            logger.info(f"🔄 止盈线跟随: {symbol} 止损单已提升至 {sl_price}")
            return sl_price
            
        except Exception as e:
            # Avoid double error logging for handled -4130 (which returned None)
            # But here we catch generic Exceptions.
            # The inner return None exits function.
            logger.error(f"❌ 更新止损失败 {symbol}: {e}")
            return None
