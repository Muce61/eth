#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
import math
import csv
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import requests

BINANCE_FAPI = "https://fapi.binance.com"
ENDPOINT = "/fapi/v1/markPriceKlines"


def parse_iso_utc(s: str) -> int:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def day_range_utc(start_iso: str, end_iso: str) -> List[tuple[int, int, str]]:
    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    if end <= start:
        end = start + timedelta(days=1)
    out = []
    cur = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    while cur < end:
        nxt = min(cur + timedelta(days=1), end)
        out.append((int(cur.timestamp()*1000), int(nxt.timestamp()*1000)-1, cur.strftime("%Y%m%d")))
        cur = nxt
    return out


def fetch_mark_1s(symbol: str, start_ms: int, end_ms: int, session: Optional[requests.Session]=None, timeout: float=20.0) -> List[list]:
    s = session or requests.Session()
    rows: List[list] = []
    limit = 1000
    cur = start_ms
    while cur <= end_ms:
        params = {
            "symbol": symbol,
            "interval": "1s",
            "startTime": cur,
            "endTime": end_ms,
            "limit": limit,
        }
        r = s.get(BINANCE_FAPI + ENDPOINT, params=params, timeout=timeout)
        r.raise_for_status()
        arr = r.json()
        if not arr:
            break
        for k in arr:
            # kline format: [openTime, open, high, low, close, ...]
            ot = int(k[0])
            op = float(k[1]); hi = float(k[2]); lo = float(k[3]); cl = float(k[4])
            rows.append([ot, op, hi, lo, cl])
        # next page
        last_ot = int(arr[-1][0])
        nxt = last_ot + 1000  # +1s
        if nxt <= cur:
            nxt = cur + 1000
        cur = nxt
        # backoff to avoid bans
        time.sleep(0.05)
    return rows


def write_csv_day(out_dir: str, symbol: str, ymd: str, rows: List[list]):
    os.makedirs(out_dir, exist_ok=True)
    out_fp = os.path.join(out_dir, f"{symbol}_{ymd}.csv")
    with open(out_fp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # header columns (first five used downstream)
        w.writerow([
            "open_time","open","high","low","close",
            "volume","close_time","quote_asset_volume","number_of_trades",
            "taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore"
        ])
        for ot, op, hi, lo, cl in rows:
            # fill extras with zeros
            w.writerow([ot, op, hi, lo, cl, 0, ot+999, 0, 0, 0, 0, 0])
    return out_fp


def main():
    ap = argparse.ArgumentParser(description="Fetch Binance USDM 1s mark price klines and write daily CSVs")
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--start-iso", required=True, help="UTC ISO, e.g. 2025-09-01T00:00:00Z")
    ap.add_argument("--end-iso", required=True, help="UTC ISO, e.g. 2025-10-01T00:00:00Z")
    ap.add_argument("--out-dir", default=None, help="Defaults to klines_data_usdm/{SYMBOL}_1s_mark/")
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join("klines_data_usdm", f"{args.symbol}_1s_mark")
    s = requests.Session()
    for s_ms, e_ms, ymd in day_range_utc(args.start_iso, args.end_iso):
        rows = fetch_mark_1s(args.symbol, s_ms, e_ms, session=s, timeout=args.timeout)
        if not rows:
            continue
        fp = write_csv_day(out_dir, args.symbol, ymd, rows)
        print(fp)


if __name__ == "__main__":
    main()
