
import asyncio
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger("MarketScanner")

class MarketScanner:
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.active_universe = set() # Set of symbols
        self.last_update_time = 0
        self.coin_volume_ranking = {} # Symbol -> Rank

        # Backtest Alignment: Volume Ranking only updates daily
        self.last_ranking_date = None

    async def start_loop(self):
        """
        Periodic scanning loop (Every 60s)
        """
        logger.info("🔄 市场扫描循环已启动")
        while True:
            try:
                await self.scan_market()
            except Exception as e:
                logger.error(f"❌ 扫描循环错误: {e}")
            
            # Wait 60s
            await asyncio.sleep(60)

    async def scan_market(self):
        """
        Fetch Top Gainers & Update Universe
        Logic:
        1. Daily (00:00 UTC): Update 'daily_volume_universe' (Top 200 by Volume).
        2. Minutely: Scan 'daily_volume_universe' for Top Gainers (Active Universe).
        """
        start_ts = time.time()
        
        try:
            # 1. Fetch 24hr Ticker (Futures API)
            tickers = await self.client.futures_ticker() 
            
            # --- DAILY STATIC UNIVERSE UPDATE ---
            now_utc = datetime.now(timezone.utc)
            today_date = now_utc.date()
            
            # Update if first run OR new day
            if self.last_ranking_date is None or today_date > self.last_ranking_date:
                # Update Volume Ranking (Daily Static)
                usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT')]
                usdt_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
                
                # Update Ranking Map
                self.coin_volume_ranking = {t['symbol']: i + 1 for i, t in enumerate(usdt_tickers)}
                
                # Update Static Universe (Top 200)
                self.daily_static_universe = set(t['symbol'] for t in usdt_tickers[:200])
                
                self.last_ranking_date = today_date
                logger.info(f"📅 每日静态名单已更新: {len(self.daily_static_universe)} 个币种 (Date: {today_date})")
                
            # --- MINUTELY GAINER SCAN ---
            candidates = []
            for t in tickers:
                symbol = t['symbol']
                
                if not symbol.endswith('USDT'):
                    continue
                    
                # STRICT BACKTEST ALIGNMENT: Only scan coins in the Daily Static Universe
                if symbol not in self.daily_static_universe:
                    continue
                    
                # Change Check
                price_change = float(t['priceChangePercent'])
                if self.config.CHANGE_THRESHOLD_MIN <= price_change <= self.config.CHANGE_THRESHOLD_MAX:
                    candidates.append({
                        'symbol': symbol,
                        'change': price_change,
                        'volume': float(t['quoteVolume'])
                    })

            # Sort by Top Gainers
            candidates.sort(key=lambda x: x['change'], reverse=True)
            
            # Select Top N
            top_selection = candidates[:self.config.TOP_GAINER_COUNT] 
            
            new_universe = set(c['symbol'] for c in top_selection)
            
            # Compare with Active Universe
            added = new_universe - self.active_universe
            removed = self.active_universe - new_universe
            
            if added or removed:
                self.active_universe = new_universe
                logger.info(f"🔄 交易池更新耗时 {time.time() - start_ts:.2f}s")
                logger.info(f"   ➕ 新增: {list(added)}")
                logger.info(f"   ➖ 移除: {list(removed)}")
                
        except Exception as e:
            logger.error(f"市场扫描失败: {e}")


