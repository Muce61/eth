from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, List

import ccxt
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def _exchange_factory(exchange_id: str, enable_rate_limit: bool = True, timeout_ms: int = 20000):
    if not hasattr(ccxt, exchange_id):
        raise ValueError(f"Unsupported exchange id: {exchange_id}")
    klass = getattr(ccxt, exchange_id)
    return klass({
        "enableRateLimit": enable_rate_limit,
        "timeout": timeout_ms,
    })


@dataclass
class OHLCVFetcher:
    enable_rate_limit: bool = True
    requests_timeout_ms: int = 20000

    @retry(
        retry=retry_if_exception_type((ccxt.NetworkError, ccxt.DDoSProtection, ccxt.RequestTimeout)),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _fetch_once(
        self,
        exchange: ccxt.Exchange,
        symbol: str,
        timeframe: str,
        since_ms: Optional[int],
        limit: int,
    ) -> List[list]:
        return exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit)

    def fetch_historical(
        self,
        exchange_id: str,
        symbol: str,
        timeframe: str,
        since_ms: int,
        until_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        exchange = _exchange_factory(exchange_id, self.enable_rate_limit, self.requests_timeout_ms)
        try:
            exchange.load_markets()
        except Exception as e:
            logger.warning("load_markets 失败: {}", e)

        all_rows: List[list] = []
        next_since = since_ms
        last_batch_last_ts: Optional[int] = None

        while True:
            if until_ms is not None and next_since is not None and next_since > until_ms:
                break

            rows = self._fetch_once(exchange, symbol, timeframe, next_since, limit)
            if not rows:
                logger.info("无更多数据，提前结束。")
                break

            # 筛掉超出 until_ms 的数据
            if until_ms is not None:
                rows = [r for r in rows if r[0] <= until_ms]
                if not rows:
                    break

            all_rows.extend(rows)

            batch_last_ts = rows[-1][0]
            # 防止死循环：若最后一个时间戳与上一次相同，强制推进 1ms
            if last_batch_last_ts is not None and batch_last_ts <= last_batch_last_ts:
                next_since = last_batch_last_ts + 1
            else:
                next_since = batch_last_ts + 1
            last_batch_last_ts = batch_last_ts

            logger.info("已获取: {} ~ {} (累积 {} 行)", rows[0][0], rows[-1][0], len(all_rows))

            # ccxt 内置限速；这里做一点点节流，避免触发风控
            time.sleep(0.05)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = df["timestamp"].astype("int64")
        df = df.drop_duplicates(subset=["timestamp"])  # 去重
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df 