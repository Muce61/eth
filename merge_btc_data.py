#!/usr/bin/env python3
"""
手动合并已下载的1秒数据到月度文件
"""
import pandas as pd
from pathlib import Path
from glob import glob

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

# 主程序
processed_dir = Path("/Users/muce/1m_data/1s_data/processed")
monthly_dir = Path("/Users/muce/1m_data/1s_data/monthly")

print("合并BTCUSDT数据...")
for year in [2024, 2025]:
    for month in range(1, 13):
        merge_daily_to_monthly('BTCUSDT', year, month, processed_dir, monthly_dir)

print("\n完成！")
