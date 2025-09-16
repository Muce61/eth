import os
import sys
from typing import Optional

import typer
from loguru import logger

from src.config import AppConfig
from src.data.ohlcv_fetcher import OHLCVFetcher
from src.storage.file_storage import write_ohlcv_to_file
from src.utils.time_utils import parse_time_to_ms

app = typer.Typer(help="量化策略 - 数据获取 CLI")


@app.command("fetch-historical")
def fetch_historical(
    exchange: str = typer.Option("binance", help="交易所 ID（ccxt 名称）"),
    symbol: str = typer.Option("ETH/USDT", help="交易对，如 ETH/USDT"),
    timeframe: str = typer.Option("1m", help="K线周期，如 1m, 5m, 15m, 1h, 4h, 1d"),
    since: str = typer.Option("2023-01-01", help="起始时间（日期或毫秒时间戳），例如 2023-01-01 或 1672531200000"),
    until: str = typer.Option("now", help="结束时间（日期、毫秒时间戳或 now）"),
    out: str = typer.Option("data/raw", help="输出目录"),
    format: str = typer.Option("parquet", help="输出格式：parquet 或 csv"),
    limit: int = typer.Option(1000, help="单次请求最大条数（受交易所限制）"),
):
    """抓取历史 K 线并保存到文件。"""
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    config = AppConfig()
    os.makedirs(out, exist_ok=True)

    start_ms = parse_time_to_ms(since)
    end_ms = None if until.lower() == "now" else parse_time_to_ms(until)

    fetcher = OHLCVFetcher(enable_rate_limit=True, requests_timeout_ms=20000)

    logger.info(
        "开始抓取: exchange={}, symbol={}, timeframe={}, since={}, until={}",
        exchange,
        symbol,
        timeframe,
        start_ms,
        end_ms if end_ms is not None else "now",
    )

    df = fetcher.fetch_historical(
        exchange_id=exchange,
        symbol=symbol,
        timeframe=timeframe,
        since_ms=start_ms,
        until_ms=end_ms,
        limit=limit,
    )

    if df is None or df.empty:
        logger.warning("未获取到任何数据。")
        raise typer.Exit(code=0)

    file_path = write_ohlcv_to_file(
        df=df,
        base_dir=out,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        file_format=format,
    )

    logger.info("已写入: {} ({} 行)", file_path, len(df))


if __name__ == "__main__":
    app() 