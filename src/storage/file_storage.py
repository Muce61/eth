import os
from pathlib import Path

import pandas as pd
from loguru import logger


def _symbol_to_dirname(symbol: str) -> str:
    return symbol.replace("/", "-")


def _build_path(base_dir: str, exchange: str, symbol: str, timeframe: str, file_format: str) -> Path:
    directory = Path(base_dir) / exchange / _symbol_to_dirname(symbol)
    directory.mkdir(parents=True, exist_ok=True)
    ext = "parquet" if file_format.lower() == "parquet" else "csv"
    return directory / f"{timeframe}.{ext}"


def write_ohlcv_to_file(
    df: pd.DataFrame,
    base_dir: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    file_format: str = "parquet",
) -> str:
    path = _build_path(base_dir, exchange, symbol, timeframe, file_format)

    if path.exists():
        try:
            if file_format.lower() == "parquet":
                old = pd.read_parquet(path)
            else:
                old = pd.read_csv(path)
            df = pd.concat([old, df], ignore_index=True)
            logger.info("检测到已有文件，进行增量合并与去重: {}", path)
        except Exception as e:
            logger.warning("读取旧文件失败，直接覆盖写入: {}", e)

    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if file_format.lower() == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)

    return str(path) 