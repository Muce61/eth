#!/usr/bin/env python3
"""
多周期数据预处理脚本
Preprocess Multi-Timeframe Data

功能:
1. 读取所有15m数据
2. 重采样生成 1h 和 4h 数据
3. 保存到独立目录，加速回测加载
"""

import pandas as pd
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import time

# 配置
SOURCE_DIR = "/Users/muce/1m_data/processed_15m_data"
TARGET_DIR_1H = "/Users/muce/1m_data/processed_1h_data"
TARGET_DIR_4H = "/Users/muce/1m_data/processed_4h_data"

def ensure_dirs():
    """创建目标目录"""
    for d in [TARGET_DIR_1H, TARGET_DIR_4H]:
        Path(d).mkdir(parents=True, exist_ok=True)

def process_single_file(file_path):
    """处理单个文件"""
    try:
        symbol = file_path.stem
        
        # 读取15m数据
        df = pd.read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        df = df.sort_index()
        
        # 生成 1H 数据
        df_1h = df.resample('1h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # 生成 4H 数据
        df_4h = df.resample('4h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # 保存
        df_1h.to_csv(f"{TARGET_DIR_1H}/{symbol}.csv")
        df_4h.to_csv(f"{TARGET_DIR_4H}/{symbol}.csv")
        
        return f"✅ {symbol}: 1H({len(df_1h)}) 4H({len(df_4h)})"
        
    except Exception as e:
        return f"❌ {file_path.name}: {str(e)}"

def main():
    print("="*60)
    print("🚀 开始多周期数据预处理")
    print("="*60)
    
    ensure_dirs()
    
    # 获取所有文件
    source_path = Path(SOURCE_DIR)
    files = list(source_path.glob("*USDT.csv"))
    print(f"找到 {len(files)} 个数据文件")
    
    start_time = time.time()
    
    # 并行处理
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_single_file, files))
    
    # 统计结果
    success = [r for r in results if "✅" in r]
    failed = [r for r in results if "❌" in r]
    
    print(f"\n处理完成!")
    print(f"成功: {len(success)}")
    print(f"失败: {len(failed)}")
    print(f"耗时: {time.time() - start_time:.2f}秒")
    print(f"1H数据目录: {TARGET_DIR_1H}")
    print(f"4H数据目录: {TARGET_DIR_4H}")

if __name__ == "__main__":
    main()
