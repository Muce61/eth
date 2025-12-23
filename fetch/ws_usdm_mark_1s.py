#!/usr/bin/env python3
from __future__ import annotations

import os
import csv
import time
import argparse
import asyncio
import datetime as dt
from typing import Optional

import websockets


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


async def collect_mark_1s(symbol: str, out_dir: str, reconnect_delay: float = 3.0):
    ensure_dir(out_dir)
    stream = f"{symbol.lower()}@markPrice@1s"
    url = f"wss://fstream.binance.com/ws/{stream}"
    writer = None
    cur_day: Optional[str] = None
    out_fp: Optional[str] = None

    while True:
        try:
            async with websockets.connect(url, max_size=1 << 20, ping_interval=15, ping_timeout=30) as ws:
                while True:
                    msg = await ws.recv()
                    # 轻量解析（避免依赖 ujson）
                    try:
                        import json
                        obj = json.loads(msg)
                        ts = int(obj.get("E") or int(time.time() * 1000))
                        px = float(obj.get("p") or obj.get("markPrice"))
                    except Exception:
                        continue

                    day = dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y%m%d")
                    if day != cur_day:
                        if writer:
                            try:
                                writer = None
                                f.close()  # type: ignore[name-defined]
                            except Exception:
                                pass
                        out_fp = os.path.join(out_dir, f"{symbol.upper()}_{day}.csv")
                        new = not os.path.exists(out_fp)
                        f = open(out_fp, "a", newline="", encoding="utf-8")  # type: ignore[assignment]
                        writer = csv.writer(f)
                        if new:
                            writer.writerow([
                                "open_time","open","high","low","close",
                                "volume","close_time","quote_asset_volume","number_of_trades",
                                "taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore",
                            ])
                        cur_day = day
                    if writer:
                        # 以同一价格填充OHLC；补齐列
                        writer.writerow([ts, px, px, px, px, 0, ts + 999, 0, 0, 0, 0, 0])
        except Exception:
            await asyncio.sleep(reconnect_delay)


def main():
    ap = argparse.ArgumentParser(description="Collect Binance USDM mark price 1s via WebSocket")
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join("klines_data_usdm", f"{args.symbol.upper()}_1s_mark")
    asyncio.run(collect_mark_1s(args.symbol, out_dir))


if __name__ == "__main__":
    main()



