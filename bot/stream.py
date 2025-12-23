
import asyncio
import logging

logger = logging.getLogger("StreamManager")

class StreamManager:
    def __init__(self, client, bsm):
        self.client = client
        self.bsm = bsm
        self.event_queue = asyncio.Queue()
        self.active_subscriptions = set() # Set of current stream names
        self.tasks = []
        self.keep_running = True
        self.known_symbols = set()

    async def start(self):
        """
        Main loop checks for subscription updates/health
        """
        logger.info("📡 行情推送管理已启动")
        while self.keep_running:
            await asyncio.sleep(1)

    async def update_subscriptions(self, universe_symbols, active_position_symbols):
        """
        Re-calibrates streams based on:
        1. Universe -> kline_1m (Strategy Trigger)
        2. Active Positions -> kline_1s (Risk Monitor)
        """
        target_streams = set()
        
        # 1. Universe Streams (1m)
        for s in universe_symbols:
            target_streams.add(f"{s.lower()}@kline_1m")
            
        # 2. Risk Streams (1s)
        for s in active_position_symbols:
            target_streams.add(f"{s.lower()}@kline_1s")

        # Detect Change
        if target_streams != self.active_subscriptions:
            logger.info(f"📡 正在更新订阅: {len(target_streams)} 个流")
            
            # Cancel old listeners
            for t in self.tasks:
                t.cancel()
            self.tasks = []
            
            self.active_subscriptions = target_streams
            
            if not target_streams:
                return

            # Chunk streams (Binance limit ~1024 chars url, safe bet ~20 streams per socket?)
            # python-binance creates a URL.
            # Let's be conservative: 50 streams per socket.
            streams_list = list(target_streams)
            chunk_size = 50
            chunks = [streams_list[i:i + chunk_size] for i in range(0, len(streams_list), chunk_size)]
            
            for i, chunk in enumerate(chunks):
                task = asyncio.create_task(self._socket_listener(chunk, i))
                self.tasks.append(task)
                
            logger.info(f"✅ 已启动 {len(chunks)} 个 Socket 监听器")

    async def _socket_listener(self, streams, index):
        """
        Async Task to listen to a batch of streams.
        Outer loop handles RECONNECTION.
        """
        while self.keep_running:
            try:
                # 1. Connect (Futures Stream)
                # Using context manager handles valid connection life-cycle.
                async with self.bsm.futures_multiplex_socket(streams) as stream:
                    logger.info(f"🔗 Socket {index} 已连接 (流数量: {len(streams)})")
                    
                    # 2. Receive Loop
                    while self.keep_running:
                        try:
                            # 3. Read (Wait 10s max to detect silence?)
                            # Binance ping is every 3 mins.
                            # Standard recv is indefinite.
                            msg = await asyncio.wait_for(stream.recv(), timeout=60)
                            
                            # Push to centralized Queue
                            if msg:
                                await self.event_queue.put(msg)
                                
                        except asyncio.TimeoutError:
                            # No Data for 60s -> Reconnect
                            logger.warning(f"⚠️ Socket {index} 心跳超时 (60s 无数据)，正在重连...")
                            break 
                            
                        except asyncio.CancelledError:
                            raise
                            
                        except Exception as e:
                            logger.error(f"⚠️ Socket {index} 读取错误: {e}，正在重连...")
                            break # Break inner loop to trigger outer re-connect
                            
            except asyncio.CancelledError:
                break
                
            except Exception as e:
                logger.error(f"❌ Socket {index} 连接失败: {e}")
            
            # Rate limit reconnection (5s safe backoff)
            if self.keep_running:
                logger.info(f"⏳ Socket {index} 将在 5秒后重试...")
                await asyncio.sleep(5)

    async def stop(self):
        self.keep_running = False
        for t in self.tasks:
            t.cancel()
