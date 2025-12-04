#!/usr/bin/env python3
"""
从Binance Data Portal下载aggTrades数据并转换为1秒K线
时间范围: 2024-12-01 到 2025-12-01
"""
import requests
import pandas as pd
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import time
import io
import zipfile

# Top 50 精选币种
TIER1_MAJOR = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 
    'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOGEUSDT'
]

TIER2_HIGH_VOL = [
    'MATICUSDT', 'LINKUSDT', 'DOTUSDT', 'UNIUSDT',
    'LTCUSDT', 'BCHUSDT', 'ATOMUSDT', 'ETCUSDT',
    'XLMUSDT', 'FILUSDT', 'TRXUSDT', 'AAVEUSDT',
    'ALGOUSDT', 'ICPUSDT', 'NEARUSDT'
]

TIER3_ACTIVE = [
    '1000PEPEUSDT', '1000SHIBUSDT', 'BONKUSDT', 'FLOKIUSDT',
    'WIFUSDT', 'PENDLEUSDT', 'STXUSDT', 'INJUSDT',
    'SUIUSDT', 'APTUSDT', 'ARBUSDT', 'OPUSDT',
    'IMXUSDT', 'LDOUSDT', 'RNDRUSDT', 'GRTUSDT', 'SANDUSDT'
]

TIER4_RESEARCH = [
    'VIRTUALUSDT', 'ENAUSDT', 'TRUMPUSDT', 'ASTERUSDT',
    'HYPEUSDT', 'ZECUSDT', 'HBARUSDT', 'PNUTUSDT',
    'ONDOUSDT', 'WLDUSDT'
]

TOP_50 = TIER1_MAJOR + TIER2_HIGH_VOL + TIER3_ACTIVE + TIER4_RESEARCH

# Binance Data Portal URL
BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"

def download_aggtrades(symbol, date_str, output_dir):
    """
    下载单日aggTrades数据
    
    URL格式: 
    https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12-01.zip
    """
    url = f"{BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
    
    try:
        # print(f"  下载 {symbol} {date_str}... ", end='', flush=True)
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            # 检查是否是XML错误 (Binance有时返回200但内容是XML Error)
            if response.content.startswith(b'<?xml') or response.content.startswith(b'<Error>'):
                # print(f"✗ XML Error")
                return None
                
            # 保存ZIP文件
            zip_path = output_dir / f"{symbol}-{date_str}.zip"
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            # print(f"✓ ({len(response.content)//1024}KB)")
            return zip_path
        else:
            # print(f"✗ HTTP {response.status_code}")
            return None
            
    except Exception as e:
        # print(f"✗ {str(e)[:50]}")
        return None

def extract_and_convert_to_1s(zip_path, output_dir):
    """
    解压ZIP并将aggTrades转换为1秒K线
    """
    try:
        # 验证是否为有效ZIP
        if not zipfile.is_zipfile(zip_path):
            # print(f"    无效ZIP文件: {zip_path.name}")
            zip_path.unlink() # 删除无效文件
            return 0
            
        # 解压ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # 获取CSV文件名
            csv_name = zip_ref.namelist()[0]
            
            # 读取CSV
            with zip_ref.open(csv_name) as csv_file:
                df = pd.read_csv(csv_file)
        
        # 转换时间戳为datetime (列名是 transact_time)
        df['timestamp'] = pd.to_datetime(df['transact_time'], unit='ms')
        df['second'] = df['timestamp'].dt.floor('1s')  # Fix: 1S -> 1s
        
        # 按秒聚合
        klines_1s = df.groupby('second').agg({
            'price': ['first', 'max', 'min', 'last'],
            'quantity': 'sum'
        }).reset_index()
        
        klines_1s.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # 保存1秒K线
        # zip_path.stem 格式: BTCUSDT-2024-12-01
        parts = zip_path.stem.split('-')
        if len(parts) >= 4:
            symbol = parts[0]
            date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
        else:
            # Fallback
            symbol = zip_path.stem.split('-')[0]
            date_str = zip_path.stem.replace(f"{symbol}-", "")
            
        output_file = output_dir / f"{symbol}-{date_str}.csv"
        
        klines_1s.to_csv(output_file, index=False)
        
        # 删除ZIP文件以节省空间
        zip_path.unlink()
        
        return len(klines_1s)
        
    except Exception as e:
        print(f"    转换失败: {e}")
        return 0

def process_single_date(args):
    """处理单个日期的任务 (用于多线程)"""
    symbol, date, raw_dir, processed_dir = args
    date_str = date.strftime('%Y-%m-%d')
    
    # 检查是否已存在
    output_file = processed_dir / f"{symbol}-{date_str}.csv"
    if output_file.exists():
        return 1  # 已存在
    
    # 下载
    zip_path = download_aggtrades(symbol, date_str, raw_dir)
    
    if zip_path:
        # 转换
        rows = extract_and_convert_to_1s(zip_path, processed_dir)
        return rows if rows > 0 else 0
    return 0

def merge_daily_to_monthly(symbol, year, month, processed_dir, monthly_dir):
    """合并单日文件为月度文件"""
    pattern = f"{symbol}-{year}-{month:02d}-*.csv"
    daily_files = sorted(processed_dir.glob(pattern))
    
    if not daily_files:
        return False
    
    print(f"  合并 {symbol} {year}-{month:02d}: {len(daily_files)}天 ... ", end='', flush=True)
    
    try:
        dfs = []
        for f in daily_files:
            df = pd.read_csv(f)
            dfs.append(df)
        
        merged = pd.concat(dfs, ignore_index=True)
        merged['timestamp'] = pd.to_datetime(merged['timestamp'])
        merged = merged.sort_values('timestamp').drop_duplicates()
        
        # 保存月度文件
        output_file = monthly_dir / f"{symbol}-{year}-{month:02d}.csv"
        merged.to_csv(output_file, index=False)
        
        # 删除日度文件
        for f in daily_files:
            f.unlink()
        
        file_size_mb = output_file.stat().st_size // 1024 // 1024
        print(f"✓ {len(merged):,}行, {file_size_mb}MB")
        return True
        
    except Exception as e:
        print(f"✗ {e}")
        return False

def main():
    from concurrent.futures import ThreadPoolExecutor
    
    print("="*70)
    print("Binance 1秒K线数据下载器 (多线程加速版 🚀)")
    print("="*70)
    print(f"时间范围: 2024-12-01 到 2025-12-01")
    print(f"币种数量: {len(TOP_50)}")
    print(f"预计下载: ~150GB (1年)")
    print("="*70)
    
    # 创建目录
    raw_dir = Path("/Users/muce/1m_data/1s_data/raw")
    processed_dir = Path("/Users/muce/1m_data/1s_data/processed")
    monthly_dir = Path("/Users/muce/1m_data/1s_data/monthly")
    
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    monthly_dir.mkdir(parents=True, exist_ok=True)
    
    # 生成日期列表
    start_date = datetime(2024, 12, 1)
    end_date = datetime(2025, 12, 1)
    
    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append(current)
        current += timedelta(days=1)
    
    print(f"\n总天数: {len(date_list)}天")
    print(f"总下载任务: {len(TOP_50)} × {len(date_list)} = {len(TOP_50) * len(date_list):,}个文件")
    
    print("自动开始下载 (10线程并行)...")
    
    # 统计
    total_downloaded = 0
    total_failed = 0
    
    # 按币种下载
    for i, symbol in enumerate(TOP_50, 1):
        print(f"\n[{i}/{len(TOP_50)}] {symbol} 正在下载...")
        
        # 准备任务
        tasks = [(symbol, date, raw_dir, processed_dir) for date in date_list]
        
        # 多线程执行
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(process_single_date, tasks))
        
        # 统计结果
        success_count = sum(1 for r in results if r > 0)
        fail_count = len(results) - success_count
        
        total_downloaded += success_count
        total_failed += fail_count
        
        print(f"  {symbol} 完成: ✓{success_count} ✗{fail_count}")
        
        # 合并月度文件
        print(f"  合并月度文件...")
        for year in [2024, 2025]:
            for month in range(1, 13):
                merge_daily_to_monthly(symbol, year, month, processed_dir, monthly_dir)
    
    print("\n" + "="*70)
    print("下载完成摘要")
    print("="*70)
    print(f"成功: {total_downloaded:,}个文件")
    print(f"失败: {total_failed:,}个文件")
    
    # 检查磁盘使用
    import subprocess
    result = subprocess.run(['du', '-sh', str(monthly_dir)], capture_output=True, text=True)
    print(f"磁盘使用: {result.stdout.strip()}")
    
    print("\n✅ 所有数据已保存至:", monthly_dir)

if __name__ == "__main__":
    main()
