"""
最近7天 Debug 回测脚本
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backtest.real_engine import RealBacktestEngine
from utils.backtest_metrics import calculate_comprehensive_metrics

def main():
    print("="*80)
    print("🔍 Debug 回测: 最近7天 (11.25 - 12.01)")
    print("="*80)
    
    # 初始化回测引擎
    engine = RealBacktestEngine(initial_balance=100)
    
    # 运行回测
    print("正在运行回测...")
    engine.run(start_date='2025-11-25', end_date='2025-12-01')
    
    # 计算指标
    metrics = calculate_comprehensive_metrics(
        engine.trades, 
        engine.initial_balance, 
        engine.balance
    )
    
    # 打印结果
    print("\n" + "="*60)
    print("Debug 结果")
    print("="*60)
    print(f"📊 总收益率: {metrics['total_return_pct']:.2f}%")
    print(f"📈 交易数量: {metrics['total_trades']}")
    
    if metrics['total_trades'] == 0:
        print("\n⚠️ 依然没有交易！请检查:")
        print("1. 数据是否加载成功？")
        print("2. 过滤条件是否过严？(Vol > 3.0, RSI > 55)")
        print("3. 是否有 debug 日志输出？")

if __name__ == "__main__":
    main()
