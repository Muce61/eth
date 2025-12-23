
import asyncio
import logging
import signal
import sys
import os
import socket
from binance import AsyncClient, BinanceSocketManager
from config.settings import Config
from bot.executor import OrderExecutor
from bot.scanner import MarketScanner
from bot.stream import StreamManager
from bot.engine import BotEngine
from bot.risk import RiskManager

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/live_bot.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("LiveBot")

class LiveBot:
    def __init__(self):
        self.config = Config()
        self.is_running = True
        self.client = None
        self.bsm = None
        
        # Components (Initialized in start)
        self.scanner = None
        self.stream_manager = None
        self.engine = None
        self.executor = None
        self.risk_manager = None

    def _check_local_proxy(self, port=7897):
        """
        Check if a local proxy is active on the given port.
        Returns Proxy URL if active, else None.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        try:
            result = sock.connect_ex(('127.0.0.1', port))
            if result == 0:
                return f"http://127.0.0.1:{port}"
        except Exception:
            pass
        finally:
            sock.close()
        return None

    async def start(self):
        """
        Main Async Entry Point
        """
        logger.info("🚀 正在启动异步实盘机器人...")
        
        # 1. Setup Proxy (Explicit only)
        # Prefer Direct Connection for Binance WebSocket stability
        proxy_url = os.getenv("PROXY_URL")
        
        requests_params = {}
        if proxy_url:
            logger.info(f"🔌 使用显式代理: {proxy_url}")
            requests_params = {'proxy': proxy_url}
        else:
            logger.info("🌍 使用直接连接 (已验证 API 访问)")

        # 2. Initialize Binance Client
        api_key = self.config.API_KEY
        api_secret = self.config.SECRET
        
        # Validate Keys
        if not api_key or not api_secret:
            logger.error("❌ .env 文件中缺失 API Key")
            return

        self.client = await AsyncClient.create(api_key, api_secret, requests_params=requests_params)
        self.bsm = BinanceSocketManager(self.client)
        logger.info("✅ Binance 异步客户端初始化成功")

        # 2. Initialize Components
        self.executor = OrderExecutor(self.client, self.config)
        self.risk_manager = RiskManager(self.executor, self.config)
        self.stream_manager = StreamManager(self.client, self.bsm)
        self.scanner = MarketScanner(self.client, self.config)
        
        # Engine connects everything
        self.engine = BotEngine(
            self.client,
            self.scanner,
            self.stream_manager,
            self.executor,
            self.risk_manager,
            self.config
        )

        # 3. Start Tasks
        try:
            # A. Start Stream Manager (WebSockets)
            stream_task = asyncio.create_task(self.stream_manager.start())
            
            # B. Start Engine (The Brain)
            engine_task = asyncio.create_task(self.engine.start())
            
            # C. Start Periodic Scanner
            scanner_task = asyncio.create_task(self.scanner.start_loop())

            logger.info("✅ 所有系统已就绪，等待事件中...")
            
            # Keep Alive
            while self.is_running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("🛑 机器人正在停止...")
        except Exception as e:
            logger.error(f"❌ 主循环发生严重错误: {e}", exc_info=True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """
        Graceful Shutdown
        """
        logger.info("🔻 正在关机...")
        self.is_running = False
        
        if self.stream_manager:
            await self.stream_manager.stop()
            
        if self.client:
            await self.client.close_connection()
            logger.info("✅ Binance 客户端已关闭")
            
        logger.info("👋 再见")

def handle_signal(sig, frame):
    logger.info("⚠️ 收到退出信号")
    # We can't await here, but the loop checking is_running will catch it
    # Ideally, we cancel the main task.
    # For now, simplistic exit.
    pass 

async def main():
    bot = LiveBot()
    
    # Signal Handling for Docker/Ctrl+C
    loop = asyncio.get_running_loop()
    stop_signal = asyncio.Event()
    
    def signal_handler():
        bot.is_running = False
        stop_signal.set()
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await bot.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
