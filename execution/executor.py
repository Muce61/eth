import time
import logging
from data.binance_client import BinanceClient
from config.settings import Config

class Executor(BinanceClient):
    def __init__(self):
        super().__init__()
        self.config = Config()
        self.logger = logging.getLogger('trading_bot')

    def place_order(self, symbol, side, quantity, order_type='MARKET', price=None, params=None):
        """
        Place an order on Binance Futures.
        """
        if params is None:
            params = {}
            
        self.logger.info(f"正在提交 {side} {order_type} 订单: {quantity} {symbol}...")
        
        # Let exception propagate to caller
        order = self.exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=side,
            amount=quantity,
            price=price,
            params=params
        )
        self.logger.info(f"订单已提交: {order['id']}")
        return order

    def set_leverage(self, symbol, leverage):
        # Let exception propagate
        self.exchange.set_leverage(leverage, symbol)
        self.logger.info(f"{symbol} 杠杆设置为 {leverage}x")

    def set_margin_mode(self, symbol, margin_mode='ISOLATED'):
        try:
            self.exchange.set_margin_mode(margin_mode, symbol)
            self.logger.info(f"{symbol} 保证金模式设置为 {margin_mode}")
        except Exception as e:
            if "No need to change" in str(e):
                self.logger.info(f"{symbol} 保证金模式已经是 {margin_mode}")
            else:
                self.logger.warning(f"设置保证金模式失败 {symbol}: {e}")

    def place_stop_loss(self, symbol, side, quantity, stop_price):
        """
        Place a STOP_MARKET order.
        If Long, SL is Sell. If Short, SL is Buy.
        """
        sl_side = 'sell' if side.lower() == 'buy' else 'buy'
        params = {
            'stopPrice': stop_price,
            'reduceOnly': True
        }
        return self.place_order(symbol, sl_side, quantity, 'STOP_MARKET', params=params)

    def cancel_all_orders(self, symbol):
        try:
            self.exchange.cancel_all_orders(symbol)
            self.logger.info(f"已撤销 {symbol} 所有挂单")
        except Exception as e:
            self.logger.warning(f"撤单失败: {e}")
