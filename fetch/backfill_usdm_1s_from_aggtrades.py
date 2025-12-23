#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import csv
import time
import math
import json
import argparse
import datetime as dt
from typing import List, Tuple, Optional, Dict

import requests


FAPI = "https://fapi.binance.com"


def iso_utc(d: dt.datetime) -> str:
    return d.replace(tzinfo=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_ranges(days: int, until: Optional[dt.datetime] = None) -> List[Tuple[int, int, str]]:
    end = until or dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    start = end - dt.timedelta(days=days)
    out: List[Tuple[int, int, str]] = []
    cur = dt.datetime(start.year, start.month, start.day, tzinfo=dt.timezone.utc)
    while cur < end:
        nxt = min(cur + dt.timedelta(days=1), end)
        out.append((int(cur.timestamp()*1000), int(nxt.timestamp()*1000)-1, cur.strftime("%Y%m%d")))
        cur = nxt
    return out


def fetch_aggtrades(symbol: str, start_ms: int, end_ms: int, session: requests.Session, timeout: float = 15.0) -> List[Dict]:
    url = FAPI + "/fapi/v1/aggTrades"
    out: List[Dict] = []
    cur = start_ms
    limit = 1000
    while cur <= end_ms:
        params = {
            "symbol": symbol,
            "startTime": cur,
            "endTime": end_ms,
            "limit": limit,
        }
        r = session.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            time.sleep(1.0)
            continue
        r.raise_for_status()
        arr = r.json()
        if not arr:
            break
        out.extend(arr)
        last_t = int(arr[-1]["T"])  # trade time ms
        nxt = last_t + 1
        if nxt <= cur:
            nxt = cur + 1
        cur = nxt
        # 节流
        time.sleep(0.05)
    return out


def to_1s_ohlc(trades: List[Dict]) -> List[Tuple[int, float, float, float, float, float]]:
    # 返回 (ts_open_ms, open, high, low, close, volume_base)
    by_sec: Dict[int, List[Tuple[float, float]]] = {}
    for t in trades:
        ts = int(t["T"]) // 1000 * 1000
        px = float(t["p"]) if isinstance(t.get("p"), str) else float(t["p"])  # price
        qty = float(t["q"]) if isinstance(t.get("q"), str) else float(t["q"])  # qty
        by_sec.setdefault(ts, []).append((px, qty))
    rows: List[Tuple[int, float, float, float, float, float]] = []
    for ts in sorted(by_sec.keys()):
        arr = by_sec[ts]
        prices = [p for p, _ in arr]
        qtys = [q for _, q in arr]
        open_px = prices[0]
        high_px = max(prices)
        low_px = min(prices)
        close_px = prices[-1]
        vol = sum(qtys)
        rows.append((ts, open_px, high_px, low_px, close_px, vol))
    return rows


def write_daily_csv(symbol: str, out_dir: str, ymd: str, rows: List[Tuple[int, float, float, float, float, float]]):
    os.makedirs(out_dir, exist_ok=True)
    fp = os.path.join(out_dir, f"{symbol}_{ymd}.csv")
    with open(fp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "open_time","open","high","low","close",
            "volume","close_time","quote_asset_volume","number_of_trades",
            "taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore",
        ])
        for ts, op, hi, lo, cl, vol in rows:
            w.writerow([ts, op, hi, lo, cl, vol, ts+999, 0, 0, 0, 0, 0])
    return fp


def main():
    ap = argparse.ArgumentParser(description="Backfill USDM 1s OHLC from aggTrades (Binance)")
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--since-iso", default=None)
    ap.add_argument("--until-iso", default=None)
    ap.add_argument("--proxy", default=os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"))
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join("klines_data_usdm", f"{args.symbol}_1s_mark")
    sess = requests.Session()
    if args.proxy:
        sess.proxies.update({"http": args.proxy, "https": args.proxy})

    if args.since_iso and args.until_iso:
        since = dt.datetime.fromisoformat(args.since_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        until = dt.datetime.fromisoformat(args.until_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        ranges = []
        cur = since
        while cur < until:
            nxt = min(cur + dt.timedelta(days=1), until)
            ranges.append((int(cur.timestamp()*1000), int(nxt.timestamp()*1000)-1, cur.strftime("%Y%m%d")))
            cur = nxt
    else:
        ranges = day_ranges(args.days)

    for s_ms, e_ms, ymd in ranges:
        try:
            trades = fetch_aggtrades(args.symbol, s_ms, e_ms, session=sess)
            if not trades:
                continue
            rows = to_1s_ohlc(trades)
            fp = write_daily_csv(args.symbol, out_dir, ymd, rows)
            print(fp)
        except Exception as e:
            print(f"ERROR {ymd}: {e}")
            time.sleep(1.0)


if __name__ == "__main__":
    main()



