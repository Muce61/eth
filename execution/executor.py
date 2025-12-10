import time
import logging
import ccxt
import hmac
import hashlib
import urllib.parse
import json
import requests
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
        Place a Stop Loss using the Algo Order Endpoint (Primary Strategy).
        We bypass the standard endpoint which often returns -4120.
        Fallback: Soft Stop (handled by caller if this returns None).
        """
        sl_side = 'sell' if side.lower() == 'buy' else 'buy'
        
        # Directly use Algo Order (The "Power Mode")
        self.logger.info(f"🛡️ 设置强力链上止损 (Algo Endpoint): 触发价 {stop_price} (侧: {sl_side})")
        return self.place_algo_order(symbol, sl_side, quantity, stop_price)

    def place_algo_order(self, symbol, side, quantity, stop_price):
        """
        Manually construct a signed request to /fapi/v1/algoOrder to bypass CCXT/API limitations.
        """
        try:
            # 1. Prepare Params (Standard)
            market = self.exchange.market(symbol)
            symbol_raw = market['id']
            
            # 1. Prepare Parameters
            params = {
                'symbol': symbol_raw,
                'side': side.upper(),
                'quantity': str(quantity),
                'reduceOnly': 'true',
                'type': 'STOP_MARKET',
                'stopPrice': str(stop_price),
                'triggerPrice': str(stop_price), # Mandatory for Algo Endpoint
                'workingType': 'CONTRACT_PRICE',
                'algoType': 'CONDITIONAL', # Mandatory for Algo Endpoint
                'closePosition': 'false', # We specify quantity, so closePosition is false
                'priceProtect': 'true',
                'timestamp': int(time.time() * 1000),
                'recvWindow': 5000
            }
            
            # 2. Generate Signature (Manual HMAC SHA256)
            # This bypasses all CCXT Testnet/Sandbox confusion by implementing the raw auth protocol.
            query_string = urllib.parse.urlencode(params)
            signature = hmac.new(
                self.exchange.secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            # 3. Construct Request
            final_query = f"{query_string}&signature={signature}"
            full_url = "https://fapi.binance.com/fapi/v1/algoOrder?" + final_query
            
            headers = {
                'X-MBX-APIKEY': self.exchange.apiKey,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # 4. Execute Request
            self.logger.info(f"🚀 发送 Algo Order (Manual Sign): {symbol} {side} {quantity}")
            
            # We use a fresh requests call to avoid any middleware interference
            response = requests.post(full_url, headers=headers)
            
            # 5. Handle Response
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"✅ Algo Order 成功: ID {data.get('clientAlgoId', 'Unknown')}")
                return data
            else:
                self.logger.error(f"❌ Algo Order 失败 (HTTP {response.status_code}): {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Algo Order 异常: {e}")
            return None
    def cancel_all_orders(self, symbol):
        try:
            self.exchange.cancel_all_orders(symbol)
            self.logger.info(f"已撤销 {symbol} 所有挂单")
        except Exception as e:
            self.logger.warning(f"撤单失败: {e}")
