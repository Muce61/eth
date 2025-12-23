#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Binance Vision bulk USDM 1s mark price klines for a symbol and date range.

Primary source:
  https://data.binance.vision/data/futures/um/daily/markPriceKlines/{SYMBOL}/1s/{SYMBOL}-markPriceKlines-1s-YYYY-MM-DD.zip

Fallback (optional): indexPriceKlines (same path replacing 'markPriceKlines' -> 'indexPriceKlines').

Output:
  klines_data_usdm/{SYMBOL}_1s_mark/{SYMBOL}_1s_mark_YYYYMMDD.csv with columns:
  open_time,open,high,low,close

Progress logs include downloaded_count, estimated_remaining, percent.
"""

from __future__ import annotations

import os
import io
import sys
import csv
import json
import time
import zipfile
import argparse
from datetime import datetime, timezone, timedelta, date
from typing import List, Optional, Dict

import requests


VISION_BASE = "https://data.binance.vision"


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _iter_dates(start_date: date, end_date: date):
    cur = start_date
    one = timedelta(days=1)
    while cur <= end_date:
        yield cur
        cur += one


def _requests_proxies() -> Optional[Dict[str, str]]:
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if http_proxy or https_proxy:
        p: Dict[str, str] = {}
        if http_proxy:
            p["http"] = http_proxy
        if https_proxy:
            p["https"] = https_proxy
        return p
    return None


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _download_zip(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=60, proxies=_requests_proxies())
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None


def _convert_kline_csv_to_ohlc(mark_csv_bytes: bytes) -> List[List[float]]:
    """Convert Binance Vision kline CSV bytes to list of [open_time, open, high, low, close].
    Kline CSV columns: open time, open, high, low, close, close time, ...
    """
    rows: List[List[float]] = []
    s = mark_csv_bytes.decode("utf-8")
    rdr = csv.reader(io.StringIO(s))
    for line in rdr:
        if not line:
            continue
        try:
            ot = int(line[0])
            o = float(line[1]); h = float(line[2]); l = float(line[3]); c = float(line[4])
        except Exception:
            # header or malformed
            continue
        rows.append([ot, o, h, l, c])
    return rows


def _write_day_csv(out_csv: str, rows: List[List[float]]) -> int:
    _ensure_dir(os.path.dirname(out_csv) or ".")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["open_time", "open", "high", "low", "close"])  # header
        for r in rows:
            w.writerow(r)
    return len(rows)


def main():
    p = argparse.ArgumentParser(description="Fetch USDM 1s mark price klines from Binance Vision (daily zips)")
    p.add_argument("--symbol", default="SOLUSDT", help="Symbol like SOLUSDT")
    p.add_argument("--start-date", help="Start date YYYY-MM-DD (UTC)")
    p.add_argument("--end-date", help="End date YYYY-MM-DD (UTC, inclusive)")
    p.add_argument("--lookback-days", type=int, default=370, help="If no dates provided, use lookback days from today-1h")
    p.add_argument("--out-base", default="klines_data_usdm", help="Output root")
    p.add_argument("--fallback-index", action="store_true", help="Fallback to indexPriceKlines if mark missing")
    p.add_argument("--fallback-aggtrades", action="store_true", help="Fallback to aggTrades daily zip and resample 1s")
    args = p.parse_args()

    sym = args.symbol.strip().upper()
    today = _utc_today()
    if args.start_date and args.end_date:
        sd = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        ed = today
        sd = ed - timedelta(days=max(1, int(args.lookback_days)))

    out_dir = os.path.join(args.out_base, f"{sym}_1s_mark")
    _ensure_dir(out_dir)

    dates = list(_iter_dates(sd, ed))
    total_days = len(dates)
    downloaded = 0
    t0 = time.time()

    sess = requests.Session()

    for i, d in enumerate(dates, 1):
        ymd = d.strftime("%Y-%m-%d")
        out_csv = os.path.join(out_dir, f"{sym}_1s_mark_{d.strftime('%Y%m%d')}.csv")
        # skip if exists and has many rows
        if os.path.exists(out_csv):
            try:
                with open(out_csv, "r", encoding="utf-8") as f:
                    lines = sum(1 for _ in f)
                if lines > 1000:  # likely already populated
                    downloaded += 1
                    continue
            except Exception:
                pass

        base_url = f"{VISION_BASE}/data/futures/um/daily/markPriceKlines/{sym}/1s/{sym}-markPriceKlines-1s-{ymd}.zip"
        zbytes = _download_zip(sess, base_url)
        rows: List[List[float]] = []
        if zbytes:
            try:
                with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                    # Take first CSV entry
                    names = zf.namelist()
                    for name in names:
                        if name.endswith('.csv'):
                            with zf.open(name) as f:
                                csv_bytes = f.read()
                            rows = _convert_kline_csv_to_ohlc(csv_bytes)
                            break
            except Exception:
                rows = []

        if not rows and args.fallback_index:
            idx_url = f"{VISION_BASE}/data/futures/um/daily/indexPriceKlines/{sym}/1s/{sym}-indexPriceKlines-1s-{ymd}.zip"
            zbytes = _download_zip(sess, idx_url)
            if zbytes:
                try:
                    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.csv'):
                                with zf.open(name) as f:
                                    csv_bytes = f.read()
                                rows = _convert_kline_csv_to_ohlc(csv_bytes)
                                break
                except Exception:
                    rows = []

        # fallback 2: aggTrades (resample to 1s OHLC)
        if not rows and args.fallback_aggtrades:
            at_url = f"{VISION_BASE}/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{ymd}.zip"
            zbytes = _download_zip(sess, at_url)
            if zbytes:
                try:
                    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.csv'):
                                with zf.open(name) as f:
                                    csv_bytes = f.read()
                                # parse aggTrades csv -> dict[sec] -> prices
                                prices_by_sec = {}
                                s = csv_bytes.decode('utf-8', errors='ignore')
                                rdr = csv.reader(io.StringIO(s))
                                for line in rdr:
                                    if not line:
                                        continue
                                    # try common formats: [aggId,price,qty,firstId,lastId,timestamp,...]
                                    try:
                                        price = float(line[1])
                                        ts = int(line[5])
                                    except Exception:
                                        # ignore header/malformed
                                        continue
                                    sec = int(ts // 1000 * 1000)
                                    prices_by_sec.setdefault(sec, []).append(price)
                                rows = []
                                for sec in sorted(prices_by_sec.keys()):
                                    ps = prices_by_sec[sec]
                                    o = ps[0]; h = max(ps); l = min(ps); c = ps[-1]
                                    rows.append([sec, float(o), float(h), float(l), float(c)])
                                break
                except Exception:
                    rows = []

        wrote = _write_day_csv(out_csv, rows) if rows else 0
        downloaded += 1 if wrote > 0 else 0

        elapsed = max(1e-6, time.time() - t0)
        percent = round(i / total_days * 100.0, 2)
        print(json.dumps({
            "day": d.isoformat(),
            "csv": os.path.basename(out_csv),
            "rows": wrote,
            "progress": f"{i}/{total_days}",
            "percent": percent,
            "days_per_sec": round(i / elapsed, 2),
        }, ensure_ascii=False))
        # polite pacing for CDN
        time.sleep(0.2)

    print(json.dumps({"status": "ok", "out_dir": out_dir, "total_days": total_days}, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())


