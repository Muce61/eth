#!/usr/bin/env python3
"""
7个月完整回测 (2025年5月-11月)
验证数据优化参数的整体表现
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest.real_engine import RealBacktestEngine
from datetime import datetime
import pandas as pd

def run_month_backtest(month, year=2025):
    """运行单月回测"""
    # 确定月份的起止日期
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    engine = RealBacktestEngine()
    engine.load_data()
    engine.run(start_date=start_date, end_date=end_date)
    
    total_return = ((engine.balance - engine.initial_balance) / engine.initial_balance) * 100
    
    return {
        'month': f"{year}-{month:02d}",
        'trades': len(engine.trades),
        'wins': len([t for t in engine.trades if t['pnl'] > 0]),
        'losses': len([t for t in engine.trades if t['pnl'] <= 0]),
        'win_rate': len([t for t in engine.trades if t['pnl'] > 0]) / len(engine.trades) * 100 if engine.trades else 0,
        'total_pnl': sum([t['pnl'] for t in engine.trades]),
        'total_return': total_return,
        'final_balance': engine.balance,
        'max_drawdown': 0.0  # Simplified for now
    }

if __name__ == "__main__":
    print("="*60)
    print("7个月完整回测 (2025年5-11月)")
    print("="*60)
    print("数据优化参数: RSI>59, Vol>3.2, ADX 33-60")
    print("="*60)
    
    results = []
    
    # 5-11月逐月回测
    for month in range(5, 12):
        print(f"\n{'='*60}")
        print(f"🔄 运行 {month}月 回测...")
        print(f"{'='*60}")
        
        result = run_month_backtest(month)
        results.append(result)
        
        print(f"✅ {result['month']}月完成:")
        print(f"  收益率: {result['total_return']:.2f}%")
        print(f"  交易数: {result['trades']}")
        print(f"  胜率: {result['win_rate']:.1f}%")
        print(f"  最大回撤: {result['max_drawdown']:.2f}%")
    
    # 汇总统计
    print(f"\n{'='*60}")
    print("📊 7个月汇总统计")
    print(f"{'='*60}")
    
    df = pd.DataFrame(results)
    
    # 计算复合收益 (按月复利)
    cumulative_balance = 100.0
    for result in results:
        cumulative_balance *= (1 + result['total_return'] / 100)
    
    total_cumulative_return = (cumulative_balance - 100) / 100 * 100
    
    print(f"\n总体表现:")
    print(f"  总交易数: {df['trades'].sum()}")
    print(f"  总胜率: {(df['wins'].sum() / df['trades'].sum() * 100):.2f}%")
    print(f"  **复合收益率**: {total_cumulative_return:.2f}%")
    print(f"  最终余额: ${cumulative_balance:.2f}")
    print(f"  平均月度收益: {df['total_return'].mean():.2f}%")
    print(f"  最佳月份: {df.loc[df['total_return'].idxmax(), 'month']} ({df['total_return'].max():.2f}%)")
    print(f"  最差月份: {df.loc[df['total_return'].idxmin(), 'month']} ({df['total_return'].min():.2f}%)")
    
    print(f"\n逐月详情:")
    print(df.to_string(index=False))
    
    # 保存结果
    df.to_csv('backtest_7month_results.csv', index=False)
    print(f"\n✅ 结果已保存至: backtest_7month_results.csv")
    
    print(f"\n{'='*60}")
