#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit and repair Binance USDM 1s mark-price daily CSVs under klines_data_usdm/{SYMBOL}_1s_mark/.

Strategy per day (UTC 00:00:00 → +24h):
1) Load existing rows, normalize to UTC ms and drop duplicates
2) If missing seconds exist:
   a) Try Binance Vision markPriceKlines daily zip and merge
   b) If still missing and --fallback-index, try indexPriceKlines zip
   c) If still missing and --fallback-aggtrades, try aggTrades zip → resample 1s OHLC
   d) Remaining missing seconds are forward-filled using last close
3) Write back day CSV with exactly 86,400 rows

Progress JSON is printed per day with counts for missing and supplements.
"""

from __future__ import annotations

import os
import io
import csv
import json
import zipfile
import argparse
from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Optional

import requests
import pandas as pd
import numpy as np


VISION_BASE = "https://data.binance.vision"


def _requests_proxies() -> Optional[Dict[str, str]]:
    hp = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    sp = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if hp or sp:
        p: Dict[str, str] = {}
        if hp:
            p["http"] = hp
        if sp:
            p["https"] = sp
        return p
    return None


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _download_zip(url: str, session: requests.Session) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=60, proxies=_requests_proxies())
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None


def _parse_kline_csv(csv_bytes: bytes) -> pd.DataFrame:
    s = csv_bytes.decode("utf-8", errors="ignore")
    rdr = csv.reader(io.StringIO(s))
    rows = []
    for line in rdr:
        if not line:
            continue
        try:
            ot = int(line[0])  # open time (ms)
            o = float(line[1]); h = float(line[2]); l = float(line[3]); c = float(line[4])
            rows.append((ot, o, h, l, c))
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["timestamp","open","high","low","close"])
    df = pd.DataFrame(rows, columns=["timestamp","open","high","low","close"]).drop_duplicates(
        subset=["timestamp"], keep="last"
    ).sort_values("timestamp").reset_index(drop=True)
    return df


def _parse_aggtrades_csv(csv_bytes: bytes) -> pd.DataFrame:
    # Expected columns: aggId,price,qty,firstId,lastId,timestamp,buyerMaker, ...
    s = csv_bytes.decode("utf-8", errors="ignore")
    rdr = csv.reader(io.StringIO(s))
    prices_by_sec: Dict[int, List[float]] = {}
    for line in rdr:
        if not line:
            continue
        try:
            price = float(line[1])
            ts = int(line[5])  # ms
        except Exception:
            continue
        sec = int(ts // 1000 * 1000)
        prices_by_sec.setdefault(sec, []).append(price)
    if not prices_by_sec:
        return pd.DataFrame(columns=["timestamp","open","high","low","close"])
    rows = []
    for sec in sorted(prices_by_sec.keys()):
        ps = prices_by_sec[sec]
        o = ps[0]; h = max(ps); l = min(ps); c = ps[-1]
        rows.append((sec, float(o), float(h), float(l), float(c)))
    return pd.DataFrame(rows, columns=["timestamp","open","high","low","close"]).sort_values("timestamp").reset_index(drop=True)


def _normalize_day_df(df: pd.DataFrame) -> pd.DataFrame:
    # Accept columns: open_time/open/high/low/close or timestamp/open/high/low/close
    cols = list(df.columns)
    if "timestamp" in cols:
        ts = pd.to_numeric(df["timestamp"], errors="coerce")
    else:
        if "open_time" not in cols:
            # assume first column is open_time
            df = df.copy()
            df.columns = ["open_time","open","high","low","close"][:len(cols)]
        ts = pd.to_numeric(df["open_time"], errors="coerce")
    ts = ts.dropna().astype("int64")
    is_ms = bool(len(ts) > 0 and int(ts.iloc[0]) > 1e12)
    if is_ms:
        timestamp = ts
    else:
        timestamp = (ts * 1000).astype("int64")
    try:
        out = pd.DataFrame({
            "timestamp": timestamp,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
        })
    except Exception:
        out = pd.DataFrame({"timestamp": timestamp})
    out = out.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
    return out


def _expect_range_for_day(dt_day: date) -> (int, int):
    start = datetime(dt_day.year, dt_day.month, dt_day.day, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _merge_and_fill(full_start_ms: int, full_end_ms: int, frames: List[pd.DataFrame]) -> pd.DataFrame:
    # merge
    dfs = [f[["timestamp","open","high","low","close"]] for f in frames if f is not None and not f.empty]
    if dfs:
        dfx = pd.concat(dfs, ignore_index=True)
        dfx = dfx.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
    else:
        dfx = pd.DataFrame(columns=["timestamp","open","high","low","close"])
    # build full index
    full_ts = np.arange(full_start_ms, full_end_ms, 1000, dtype=np.int64)
    base = pd.DataFrame({"timestamp": full_ts})
    merged = base.merge(dfx, on="timestamp", how="left")
    # forward fill close; backfill head if needed
    merged["close"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["close"] = merged["close"].ffill().bfill()
    # fill open/high/low where missing with close
    for c in ("open","high","low"):
        merged[c] = pd.to_numeric(merged[c], errors="coerce")
        merged[c] = merged[c].fillna(merged["close"]) 
    # ensure bounds
    merged["high"] = merged[["high","open","close"]].max(axis=1)
    merged["low"] = merged[["low","open","close"]].min(axis=1)
    return merged[["timestamp","open","high","low","close"]]


def main():
    p = argparse.ArgumentParser(description="Audit & repair USDM 1s mark daily CSVs to full 86400 rows (UTC)")
    p.add_argument("--symbol", default="SOLUSDT", help="e.g., SOLUSDT")
    p.add_argument("--root", default="klines_data_usdm", help="Root directory containing {SYMBOL}_1s_mark/")
    p.add_argument("--start-date", help="YYYY-MM-DD (UTC); if omitted, scan all")
    p.add_argument("--end-date", help="YYYY-MM-DD (UTC)")
    p.add_argument("--fallback-index", action="store_true")
    p.add_argument("--fallback-aggtrades", action="store_true")
    args = p.parse_args()

    sym = args.symbol.strip().upper()
    mark_dir = os.path.join(args.root, f"{sym}_1s_mark")
    if not os.path.isdir(mark_dir):
        print(json.dumps({"error": f"missing dir {mark_dir}"}, ensure_ascii=False))
        return

    sess = requests.Session()
    files = [fn for fn in sorted(os.listdir(mark_dir)) if fn.endswith('.csv')]

    # Filter by date window
    if args.start_date or args.end_date:
        sd = datetime.strptime(args.start_date, '%Y-%m-%d').date() if args.start_date else date(1970,1,1)
        ed = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else date(2100,1,1)
        def _in_range(fn: str) -> bool:
            try:
                ymd = fn.rsplit('_',1)[-1].split('.')[0]
                dt = datetime.strptime(ymd, '%Y%m%d').date()
                return sd <= dt <= ed
            except Exception:
                return False
        files = [fn for fn in files if _in_range(fn)]

    for fn in files:
        try:
            ymd = fn.rsplit('_',1)[-1].split('.')[0]
            dt = datetime.strptime(ymd, '%Y%m%d').date()
        except Exception:
            continue

        fp = os.path.join(mark_dir, fn)
        try:
            df0 = pd.read_csv(fp)
        except Exception as e:
            print(json.dumps({"file": fn, "error": f"read_failed: {e}"}, ensure_ascii=False))
            continue

        dfn = _normalize_day_df(df0)
        start_ms, end_ms = _expect_range_for_day(dt)
        # initial gaps
        present = set(int(x) for x in dfn['timestamp'].tolist())
        expected_n = (end_ms - start_ms) // 1000
        missing0 = int(expected_n - len(present)) if len(present) <= expected_n else 0

        frames = [dfn]
        added_from_mark = 0
        added_from_index = 0
        added_from_agg = 0

        # Vision mark
        mark_url = f"{VISION_BASE}/data/futures/um/daily/markPriceKlines/{sym}/1s/{sym}-markPriceKlines-1s-{dt.strftime('%Y-%m-%d')}.zip"
        z = _download_zip(mark_url, sess)
        if z:
            try:
                with zipfile.ZipFile(io.BytesIO(z)) as zf:
                    for name in zf.namelist():
                        if name.endswith('.csv'):
                            rows = zf.read(name)
                            dmk = _parse_kline_csv(rows)
                            before = len(set(int(x) for x in dmk['timestamp'].tolist()))
                            frames.append(dmk)
                            added_from_mark = before
                            break
            except Exception:
                pass

        # Vision index fallback
        if args.fallback_index:
            idx_url = f"{VISION_BASE}/data/futures/um/daily/indexPriceKlines/{sym}/1s/{sym}-indexPriceKlines-1s-{dt.strftime('%Y-%m-%d')}.zip"
            z = _download_zip(idx_url, sess)
            if z:
                try:
                    with zipfile.ZipFile(io.BytesIO(z)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.csv'):
                                rows = zf.read(name)
                                didx = _parse_kline_csv(rows)
                                before = len(set(int(x) for x in didx['timestamp'].tolist()))
                                frames.append(didx)
                                added_from_index = before
                                break
                except Exception:
                    pass

        # Vision aggTrades fallback
        if args.fallback_aggtrades:
            at_url = f"{VISION_BASE}/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{dt.strftime('%Y-%m-%d')}.zip"
            z = _download_zip(at_url, sess)
            if z:
                try:
                    with zipfile.ZipFile(io.BytesIO(z)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.csv'):
                                rows = zf.read(name)
                                dagg = _parse_aggtrades_csv(rows)
                                before = len(set(int(x) for x in dagg['timestamp'].tolist()))
                                frames.append(dagg)
                                added_from_agg = before
                                break
                except Exception:
                    pass

        fixed = _merge_and_fill(start_ms, end_ms, frames)
        missing_after = int(((fixed['timestamp'].iloc[1:].values - fixed['timestamp'].iloc[:-1].values) != 1000).sum()) if len(fixed) > 1 else 0

        # write back
        # write back as PARQUET
        parquet_fp = fp.replace(".csv", ".parquet")
        
        # Optimize types for Parquet
        if 'timestamp' in fixed.columns:
             fixed['timestamp'] = pd.to_datetime(fixed['timestamp'], unit='ms')
             fixed.set_index('timestamp', inplace=True)
             
        try:
            fixed.to_parquet(parquet_fp, compression='snappy')
            # Verify and delete CSV
            if os.path.exists(parquet_fp) and os.path.getsize(parquet_fp) > 0:
                os.remove(fp)
        except Exception as e:
            print(f"Error saving parquet {parquet_fp}: {e}")
            # Fallback to keep CSV? No, we need space.
            # But if parquet failed, we better keep CSV for now.
            pass

        print(json.dumps({
            'file': fn,
            'date': dt.isoformat(),
            'missing_before': missing0,
            'added_from_mark': added_from_mark,
            'added_from_index': added_from_index,
            'added_from_agg': added_from_agg,
            'rows_after': int(len(fixed)),
            'missing_after': missing_after
        }, ensure_ascii=False))


if __name__ == '__main__':
    main()


