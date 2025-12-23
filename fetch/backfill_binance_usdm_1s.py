#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance USDM 1s kline fetcher for SOL/other symbols.

Features:
- Preferred basis: mark price 1s klines (fapi/v1/markPriceKlines)
- Fallback basis: index price 1s klines (fapi/v1/indexPriceKlines)
- Last-price fallback: aggregate trades to 1s OHLC (fapi/v1/aggTrades)
- Day-by-day backfill from (UTC now - N days) to (now - end_offset_hours)
- Writes daily CSVs under klines_data_usdm/{SYMBOL}_1s_{basis}/
- Progress logs: fetched_rows, estimated_remaining_rows, percent

Usage example:
  HTTP_PROXY=http://127.0.0.1:7897 HTTPS_PROXY=http://127.0.0.1:7897 \
  python scripts/backfill_binance_usdm_1s.py --symbol SOLUSDT \
    --basis mark --start-lookback-days 370 --end-offset-hours 1
"""

from __future__ import annotations

import os
import sys
import csv
import time
import math
import json
import argparse
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Tuple, Dict

import requests
import pandas as pd


BINANCE_FAPI = "https://fapi.binance.com"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


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


def _symbol_cli_to_no_slash(sym: str) -> str:
    s = sym.strip().upper()
    return s.replace("/", "")


def _sec_floor_ms(ts_ms: int) -> int:
    return int(ts_ms // 1000 * 1000)


def _write_day_csv(rows: List[Tuple[int, float, float, float, float]], out_csv: str) -> int:
    _ensure_dir(os.path.dirname(out_csv) or ".")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["open_time", "open", "high", "low", "close"])  # header
        for r in rows:
            w.writerow(r)
    return len(rows)


def _fetch_1s_mark_or_index(
    session: requests.Session,
    symbol_no: str,
    basis: str,
    start_ms: int,
    end_ms: int,
    log_prefix: str,
    max_retries: int = 3,
    max_rate_limit_loops: int = 60,
    long_cooldown_sec: float = 60.0,
) -> List[Tuple[int, float, float, float, float]]:
    """Fetch 1s klines from markPriceKlines / indexPriceKlines.
    Returns list of (open_time, open, high, low, close).
    """
    assert basis in ("mark", "index")
    path = "/fapi/v1/markPriceKlines" if basis == "mark" else "/fapi/v1/indexPriceKlines"
    rows: List[Tuple[int, float, float, float, float]] = []
    cursor = int(start_ms)
    proxies = _requests_proxies()

    rate_limit_loops = 0
    while cursor < end_ms:
        params = {
            "symbol": symbol_no,
            "interval": "1s",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        }
        attempt = 0
        data = None
        while attempt < max_retries:
            try:
                r = session.get(BINANCE_FAPI + path, params=params, timeout=30, proxies=proxies)
                if r.status_code == 429:
                    # 降速等待后重试
                    time.sleep(1.0 + attempt * 0.5)
                    attempt += 1
                    continue
                r.raise_for_status()
                data = r.json()
                break
            except Exception:
                time.sleep(1.0 + attempt * 0.5)
                attempt += 1
        if data is None:
            # 不中止当日窗口，继续等待后重试下一轮以避免生成断档
            print(json.dumps({"warn": f"{log_prefix}: rate_limited_or_failed, will_retry", "cursor": cursor}, ensure_ascii=False))
            rate_limit_loops += 1
            # 达到阈值则执行长冷却，随后返回让上层做 fallback（index/last）
            if rate_limit_loops >= max_rate_limit_loops:
                print(json.dumps({
                    "warn": f"{log_prefix}: too_many_rate_limits_switch_to_fallback",
                    "cursor": cursor,
                    "cooldown_sec": long_cooldown_sec
                }, ensure_ascii=False))
                time.sleep(long_cooldown_sec)
                break
            time.sleep(2.0)
            continue

        if not isinstance(data, list) or not data:
            break

        batch_n = 0
        last_open_time = None
        for k in data:
            # Kline array spec: [openTime, open, high, low, close, ...]
            ot = int(k[0])
            if ot < start_ms or ot > end_ms:
                continue
            o = float(k[1]); h = float(k[2]); l = float(k[3]); c = float(k[4])
            rows.append((ot, o, h, l, c))
            last_open_time = ot
            batch_n += 1

        if batch_n == 0:
            # 无数据，轻微等待后继续，避免偶发空窗造成断档
            time.sleep(0.2)
            continue

        # progress cursor
        if last_open_time is None:
            break
        next_cursor = int(last_open_time) + 1000
        if next_cursor <= cursor:  # safety
            next_cursor = cursor + 1000
        cursor = next_cursor

        if len(rows) % 10000 == 0:
            print(json.dumps({"progress_batch": len(rows)}, ensure_ascii=False))
        # 全局限速，避免触发429
        time.sleep(0.15)

    # Deduplicate and sort just in case
    if rows:
        df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close"]).drop_duplicates(
            subset=["open_time"], keep="last"
        ).sort_values("open_time").reset_index(drop=True)
        rows = list(df.itertuples(index=False, name=None))
    return rows


def _fetch_1s_from_aggtrades(
    session: requests.Session,
    symbol_no: str,
    start_ms: int,
    end_ms: int,
    max_retries: int = 3,
) -> List[Tuple[int, float, float, float, float]]:
    """Fetch aggregated trades and resample to 1s OHLC."""
    path = "/fapi/v1/aggTrades"
    proxies = _requests_proxies()
    cursor = int(start_ms)
    rows: List[Tuple[int, float, float, float, float]] = []
    acc: Dict[int, List[float]] = {}

    while cursor < end_ms:
        params = {
            "symbol": symbol_no,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        }
        attempt = 0
        data = None
        while attempt < max_retries:
            try:
                r = session.get(BINANCE_FAPI + path, params=params, timeout=30, proxies=proxies)
                if r.status_code == 429:
                    time.sleep(0.5 + attempt)
                    attempt += 1
                    continue
                r.raise_for_status()
                data = r.json()
                break
            except Exception:
                time.sleep(0.5 + attempt)
                attempt += 1
        if data is None:
            print(json.dumps({"warn": "aggTrades fetch failed, stop window", "cursor": cursor}, ensure_ascii=False))
            break

        if not isinstance(data, list) or not data:
            break

        last_t = None
        for t in data:
            price = float(t.get("p"))
            ts = int(t.get("T"))
            sec = _sec_floor_ms(ts)
            acc.setdefault(sec, []).append(price)
            last_t = ts

        if last_t is None:
            break
        next_cursor = int(last_t) + 1
        if next_cursor <= cursor:
            next_cursor = cursor + 1
        cursor = next_cursor

    if acc:
        secs = sorted(acc.keys())
        # Build OHLC per second
        prev_close: Optional[float] = None
        for s in secs:
            prices = acc[s]
            o = prices[0]
            h = max(prices)
            l = min(prices)
            c = prices[-1]
            # fill safety: ensure low<=open/close<=high already true
            rows.append((s, float(o), float(h), float(l), float(c)))
            prev_close = c
    return rows


def _iter_days(start_dt: date, end_dt: date):
    cur = start_dt
    one = timedelta(days=1)
    while cur <= end_dt:
        yield cur
        cur += one


def main():
    p = argparse.ArgumentParser(description="Backfill Binance USDM 1s klines (mark/index/last) by day")
    p.add_argument("--symbol", default="SOLUSDT", help="Symbol, e.g. SOLUSDT or SOL/USDT")
    p.add_argument("--basis", default="mark", choices=["mark", "index", "last"], help="Price basis for 1s")
    p.add_argument("--start-lookback-days", type=int, default=370, help="Lookback days if no explicit start")
    p.add_argument("--end-offset-hours", type=int, default=1, help="End at now-<hours> (UTC)")
    p.add_argument("--out-base", default="klines_data_usdm", help="Output base directory")
    args = p.parse_args()

    sym_no = _symbol_cli_to_no_slash(args.symbol)

    # Time window
    end_dt_utc = _utc_now() - timedelta(hours=max(0, int(args.end_offset_hours)))
    start_dt_utc = end_dt_utc - timedelta(days=max(1, int(args.start_lookback_days)))
    start_date = start_dt_utc.date()
    end_date = end_dt_utc.date()

    # Output path
    subdir = f"{sym_no}_1s_{args.basis}"
    out_dir = os.path.join(args.out_base, subdir)
    _ensure_dir(out_dir)

    # Estimate total seconds
    total_seconds = int((end_dt_utc - datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)).total_seconds())
    # Count existing fetched rows (rough estimation by summing CSV rows - header)
    fetched_rows_existing = 0
    try:
        for fn in os.listdir(out_dir):
            if not fn.endswith(".csv"):
                continue
            fpath = os.path.join(out_dir, fn)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    n = sum(1 for _ in f)
                if n > 1:
                    fetched_rows_existing += (n - 1)
            except Exception:
                pass
    except FileNotFoundError:
        pass

    print(json.dumps({
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "basis": args.basis,
        "out_dir": out_dir,
        "estimated_total_seconds": total_seconds,
        "fetched_rows_existing": fetched_rows_existing,
        "estimated_remaining_rows": max(0, total_seconds - fetched_rows_existing),
    }, ensure_ascii=False))

    session = requests.Session()

    fetched_rows_runtime = 0
    t_start = time.time()
    # 逐日顺序抓取：严格从 start_date → end_date 顺序执行
    for d in _iter_days(start_date, end_date):
        day_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        day_start_ms = int(day_start.timestamp() * 1000)
        day_end_ms = int(min(day_end, end_dt_utc).timestamp() * 1000)
        if day_start_ms >= day_end_ms:
            continue

        out_csv = os.path.join(out_dir, f"{sym_no}_1s_{args.basis}_{d.strftime('%Y%m%d')}.csv")
        # Skip if file exists and seems complete (>= 60*60*24 - tolerate early end day)
        if os.path.exists(out_csv):
            try:
                with open(out_csv, "r", encoding="utf-8") as f:
                    lines = sum(1 for _ in f)
                # 若已有文件但不完整，则强制补齐该日，不跳过
                if lines >= 60 * 60 * 24 + 1:
                    # 已有完整日（含表头），顺序推进
                    pass
                else:
                    # 不完整：继续抓取以补齐
                    pass
            except Exception:
                pass

        rows: List[Tuple[int, float, float, float, float]] = []
        if args.basis in ("mark", "index"):
            rows = _fetch_1s_mark_or_index(session, sym_no, args.basis, day_start_ms, day_end_ms, f"{args.basis}-day")
            if not rows and args.basis == "mark":
                # Fallback to index, then last
                rows = _fetch_1s_mark_or_index(session, sym_no, "index", day_start_ms, day_end_ms, "index-day")
                if not rows:
                    rows = _fetch_1s_from_aggtrades(session, sym_no, day_start_ms, day_end_ms)
        else:
            rows = _fetch_1s_from_aggtrades(session, sym_no, day_start_ms, day_end_ms)

        wrote = _write_day_csv(rows, out_csv) if rows else 0
        fetched_rows_runtime += wrote

        elapsed = max(1e-6, time.time() - t_start)
        fetched_total = fetched_rows_existing + fetched_rows_runtime
        est_rem = max(0, total_seconds - fetched_total)
        pct = (fetched_total / total_seconds * 100.0) if total_seconds > 0 else 0.0
        print(json.dumps({
            "day": d.isoformat(),
            "basis": args.basis,
            "written_for_day": wrote,
            "fetched_total": fetched_total,
            "estimated_remaining_rows": est_rem,
            "percent": round(pct, 2),
            "rows_per_sec_runtime": round(fetched_rows_runtime / elapsed, 2),
        }, ensure_ascii=False))

    print(json.dumps({"status": "ok", "out_dir": out_dir, "fetched_total": fetched_rows_existing + fetched_rows_runtime}, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())


