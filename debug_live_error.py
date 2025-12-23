
import asyncio
import logging
import traceback
import pandas as pd
from unittest.mock import MagicMock, AsyncMock
from bot.engine import BotEngine
from bot.executor import OrderExecutor
from bot.stream import StreamManager # Mock
from bot.risk import RiskManager 
from config.settings import Config
from strategy.momentum import MomentumStrategy

# Mock Objects
class MockBinanceClient:
    KLINE_INTERVAL_1MINUTE = '1m'
    
    async def get_historical_klines(self, symbol, interval, limit):
        # Return 50ms dummy klines
        # Format: [open_time, open, high, low, close, vol, close_time, ...]
        now = 1700000000000
        data = []
        for i in range(limit):
             ts = now - (limit - i) * 60000
             # Open, High, Low, Close, Vol
             data.append([ts, "1.0", "1.1", "0.9", "1.0", "1000.0", ts+59999, "1000.0", 100, "100", "100", "0"])
        return data
    
    async def futures_exchange_info(self):
         return {'symbols': [{'symbol': '0GUSDT', 'quantityPrecision': 0, 'pricePrecision': 2}]}

    async def futures_account_balance(self):
        # Simulate Error or valid return
        # return [{'asset': 'USDT', 'balance': '1000.0'}]
        raise Exception("API Error: Timestamp for this request is outside of the recvWindow.")
    
    async def futures_create_order(self, **kwargs):
        raise Exception("Order Limit Reached")

async def run_test():
    logging.basicConfig(level=logging.ERROR)
    
    client = MockBinanceClient()
    config = Config()
    config.PAPER_MODE = False
    
    scanner = MagicMock()
    scanner.coin_volume_ranking = {'0GUSDT': 10} # Top 50
    scanner.active_universe = {'0GUSDT'}

    stream = MagicMock()
    
    executor = OrderExecutor(client, config)
    # Correct Init: RiskManager(executor, config)
    risk = RiskManager(executor, config) 
    
    engine = BotEngine(client, scanner, stream, executor, risk, config)
    
    # Init Logger
    engine.live_logger = MagicMock()
    engine.signal_logger = MagicMock()
    
    # Inject Signal
    engine.strategy = MagicMock()
    # Force strategy signal to Long
    engine.strategy.check_signal.return_value = {
        'symbol': '0GUSDT',
        'side': 'LONG',
        'metrics': {'rsi': 55}
    }
    
    # Also Mock Engine's Quality Filter to pass
    engine.quality_filter = MagicMock()
    engine.quality_filter.check_quality.return_value = (True, "")

    # TEST calculate_stop_loss DIRECTLY
    print("Testing calculate_stop_loss directly...")
    try:
        # Create dummy DF with High/Low/Close
        dates = pd.date_range('2024-01-01', periods=20, freq='15min')
        df_dummy = pd.DataFrame({
            'high': [100.0] * 20,
            'low': [90.0] * 20,
            'close': [95.0] * 20
        }, index=dates)
        
        sl = risk.calculate_stop_loss(df_dummy, 95.0, 'LONG')
        print(f"Calculate Stop Loss Result: {sl}")
    except Exception:
         print("Caught Exception in calculate_stop_loss:")
         traceback.print_exc()

    print("Running process_strategy('0GUSDT')...")
    try:
        await engine.process_strategy('0GUSDT')
    except Exception:
        print("Caught Exception in process_strategy:")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
