#!/usr/bin/env python3
"""
超高杠杆盈利交易特征分析
Analyze Ultra-Leverage Winning Trades

分析维度:
1. 信号强度分布
2. Volume Ratio区间
3. RSI区间
4. ADX区间
5. 持仓时间
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_winners():
    try:
        df = pd.read_csv('ultra_leverage_backtest.csv')
    except FileNotFoundError:
        print("❌ 未找到回测结果文件: ultra_leverage_backtest.csv")
        return

    if df.empty:
        print("⚠️ 回测结果为空")
        return

    print("="*60)
    print("📊 超高杠杆交易分析")
    print("="*60)
    
    # 区分盈亏
    winners = df[df['pnl'] > 0]
    losers = df[df['pnl'] <= 0]
    
    print(f"总交易: {len(df)}")
    print(f"盈利: {len(winners)} ({len(winners)/len(df)*100:.1f}%)")
    print(f"亏损: {len(losers)} ({len(losers)/len(df)*100:.1f}%)")
    
    if winners.empty:
        print("没有盈利交易可分析")
        return

    print(f"\n🏆 盈利交易特征:")
    
    # 1. 信号强度
    print("\n[信号强度]")
    print(winners['signal_strength'].describe())
    
    # 2. 杠杆分布
    print("\n[杠杆分布]")
    print(winners['leverage'].value_counts().sort_index())
    
    # 3. 持仓时间
    print("\n[持仓时间]")
    # duration是字符串，需要转换
    print(winners['duration'].describe())
    
    # 4. 胜率 vs 信号强度
    print("\n[胜率 vs 信号强度]")
    bins = [0, 70, 80, 90, 100]
    df['strength_bin'] = pd.cut(df['signal_strength'], bins)
    win_rates = df.groupby('strength_bin')['pnl'].apply(lambda x: (x > 0).mean() * 100)
    counts = df.groupby('strength_bin')['pnl'].count()
    
    for interval, win_rate in win_rates.items():
        count = counts[interval]
        print(f"  {interval}: {win_rate:.1f}% (样本: {count})")
        
    # 5. 胜率 vs 杠杆
    print("\n[胜率 vs 杠杆]")
    lev_win_rates = df.groupby('leverage')['pnl'].apply(lambda x: (x > 0).mean() * 100)
    lev_counts = df.groupby('leverage')['pnl'].count()
    
    for lev, win_rate in lev_win_rates.items():
        count = lev_counts[lev]
        print(f"  {lev}x: {win_rate:.1f}% (样本: {count})")

if __name__ == "__main__":
    analyze_winners()
