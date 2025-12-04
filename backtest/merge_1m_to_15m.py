"""
数据预处理脚本: 1m -> 15m 合并
(Data Pre-processing: Merge 1m to 15m)

目标: 将1分钟K线数据预先重采样为15分钟数据，以加速回测。
源目录: /Users/muce/1m_data/new_backtest_data_1year_1m
目标目录: /Users/muce/1m_data/processed_15m_data
"""

import pandas as pd
from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor
import os

# 配置
SOURCE_DIR = Path('/Users/muce/1m_data/new_backtest_data_1year_1m')
TARGET_DIR = Path('/Users/muce/1m_data/processed_15m_data')

def process_file(file_path):
    try:
        # 读取1m数据
        df = pd.read_csv(file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # 重采样为15m
        # 规则:
        # Open: 第一分钟的Open
        # High: 15分钟内的最高High
        # Low: 15分钟内的最低Low
        # Close: 最后一分钟的Close
        # Volume: 15分钟Volume总和
        df_15m = df.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # 移除无效行 (比如中间有断档导致的NaN)
        df_15m.dropna(inplace=True)
        
        # 重置索引，保存为CSV
        target_path = TARGET_DIR / file_path.name
        df_15m.to_csv(target_path)
        
        return f"✓ {file_path.name}: {len(df)} -> {len(df_15m)} rows"
        
    except Exception as e:
        return f"✗ {file_path.name}: {str(e)}"

def merge_1m_to_15m():
    print("="*80)
    print("🚀 开始数据合并: 1m -> 15m")
    print(f"源目录: {SOURCE_DIR}")
    print(f"目标目录: {TARGET_DIR}")
    print("="*80)
    
    # 创建目标目录
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    # 获取所有CSV文件
    files = list(SOURCE_DIR.glob('*.csv'))
    print(f"找到 {len(files)} 个文件")
    
    start_time = time.time()
    
    # 使用多进程加速处理
    # Mac上通常核数较多，并行处理IO密集型任务效果好
    max_workers = os.cpu_count() or 4
    print(f"使用 {max_workers} 个进程并行处理...")
    
    success_count = 0
    fail_count = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_file, files))
        
        for res in results:
            if res.startswith("✓"):
                success_count += 1
                # 每完成50个打印一次进度，避免刷屏
                if success_count % 50 == 0:
                    print(f"进度: {success_count}/{len(files)}")
            else:
                fail_count += 1
                print(res)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "="*80)
    print("✅ 合并完成!")
    print(f"成功: {success_count}")
    print(f"失败: {fail_count}")
    print(f"耗时: {duration:.2f}秒")
    print(f"数据已保存至: {TARGET_DIR}")
    print("="*80)

if __name__ == "__main__":
    merge_1m_to_15m()
