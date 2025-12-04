#!/usr/bin/env python3
"""
盈利交易特征分析
分析所有盈利交易的特征，找出可以扩展入场条件的模式
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# 读取交易数据
df = pd.read_csv('backtest_trades_june_optimized.csv')

# 分离盈利和亏损交易
winners = df[df['pnl'] > 0].copy()
losers = df[df['pnl'] <= 0].copy()

print("="*60)
print("盈利交易特征分析")
print("="*60)
print(f"\n总交易数: {len(df)}")
print(f"盈利交易: {len(winners)} ({len(winners)/len(df)*100:.1f}%)")
print(f"亏损交易: {len(losers)} ({len(losers)/len(df)*100:.1f}%)")

# RSI分析
print(f"\n{'='*60}")
print("📊 RSI特征分析")
print(f"{'='*60}")
print(f"\n盈利交易RSI统计:")
print(f"  最小值: {winners['rsi'].min():.2f}")
print(f"  25%分位: {winners['rsi'].quantile(0.25):.2f}")
print(f"  中位数: {winners['rsi'].median():.2f}")
print(f"  75%分位: {winners['rsi'].quantile(0.75):.2f}")
print(f"  最大值: {winners['rsi'].max():.2f}")
print(f"  平均值: {winners['rsi'].mean():.2f}")

print(f"\n亏损交易RSI统计:")
print(f"  平均值: {losers['rsi'].mean():.2f}")
print(f"  中位数: {losers['rsi'].median():.2f}")

# 按RSI区间分析胜率
print(f"\n按RSI区间分析胜率:")
rsi_bins = [0, 55, 60, 65, 70, 75, 80, 100]
for i in range(len(rsi_bins)-1):
    lower, upper = rsi_bins[i], rsi_bins[i+1]
    in_range = df[(df['rsi'] > lower) & (df['rsi'] <= upper)]
    if len(in_range) > 0:
        win_rate = len(in_range[in_range['pnl'] > 0]) / len(in_range) * 100
        avg_pnl = in_range['pnl'].mean()
        print(f"  RSI {lower:>3}-{upper:<3}: {len(in_range):>3}笔, 胜率{win_rate:>5.1f}%, 平均PnL ${avg_pnl:>6.2f}")

# Volume分析
print(f"\n{'='*60}")
print("📊 Volume Ratio特征分析")
print(f"{'='*60}")
print(f"\n盈利交易Volume统计:")
print(f"  最小值: {winners['vol'].min():.2f}")
print(f"  25%分位: {winners['vol'].quantile(0.25):.2f}")
print(f"  中位数: {winners['vol'].median():.2f}")
print(f"  75%分位: {winners['vol'].quantile(0.75):.2f}")
print(f"  最大值: {winners['vol'].max():.2f}")
print(f"  平均值: {winners['vol'].mean():.2f}")

print(f"\n按Volume区间分析胜率:")
vol_bins = [0, 3.0, 3.5, 4.0, 5.0, 10.0, 100]
for i in range(len(vol_bins)-1):
    lower, upper = vol_bins[i], vol_bins[i+1]
    in_range = df[(df['vol'] > lower) & (df['vol'] <= upper)]
    if len(in_range) > 0:
        win_rate = len(in_range[in_range['pnl'] > 0]) / len(in_range) * 100
        avg_pnl = in_range['pnl'].mean()
        print(f"  Vol {lower:>4.1f}-{upper:<5.1f}: {len(in_range):>3}笔, 胜率{win_rate:>5.1f}%, 平均PnL ${avg_pnl:>6.2f}")

# ADX分析
print(f"\n{'='*60}")
print("📊 ADX特征分析")
print(f"{'='*60}")
print(f"\n盈利交易ADX统计:")
print(f"  最小值: {winners['adx'].min():.2f}")
print(f"  25%分位: {winners['adx'].quantile(0.25):.2f}")
print(f"  中位数: {winners['adx'].median():.2f}")
print(f"  75%分位: {winners['adx'].quantile(0.75):.2f}")
print(f"  最大值: {winners['adx'].max():.2f}")
print(f"  平均值: {winners['adx'].mean():.2f}")

print(f"\n按ADX区间分析胜率:")
adx_bins = [0, 20, 25, 30, 35, 40, 50, 100]
for i in range(len(adx_bins)-1):
    lower, upper = adx_bins[i], adx_bins[i+1]
    in_range = df[(df['adx'] >= lower) & (df['adx'] < upper)]
    if len(in_range) > 0:
        win_rate = len(in_range[in_range['pnl'] > 0]) / len(in_range) * 100
        avg_pnl = in_range['pnl'].mean()
        print(f"  ADX {lower:>3}-{upper:<3}: {len(in_range):>3}笔, 胜率{win_rate:>5.1f}%, 平均PnL ${avg_pnl:>6.2f}")

# 识别被过滤掉的盈利模式
print(f"\n{'='*60}")
print("🔍 识别被过滤掉的盈利模式")
print(f"{'='*60}")

# 当前过滤器: RSI>55, Vol>3.0, ADX 25-60
potentially_missed = winners[
    (winners['rsi'] <= 55) | 
    (winners['vol'] <= 3.0) | 
    (winners['adx'] < 25) | 
    (winners['adx'] > 60)
]

if len(potentially_missed) > 0:
    print(f"\n发现{len(potentially_missed)}笔盈利交易可能被当前过滤器排除:")
    print(f"  总盈利: ${potentially_missed['pnl'].sum():.2f}")
    print(f"  平均盈利: ${potentially_missed['pnl'].mean():.2f}")
    
    # 细分原因
    low_rsi = potentially_missed[potentially_missed['rsi'] <= 55]
    low_vol = potentially_missed[potentially_missed['vol'] <= 3.0]
    low_adx = potentially_missed[potentially_missed['adx'] < 25]
    high_adx = potentially_missed[potentially_missed['adx'] > 60]
    
    print(f"\n被排除原因分布:")
    print(f"  RSI<=55: {len(low_rsi)}笔, 盈利${low_rsi['pnl'].sum():.2f}")
    print(f"  Vol<=3.0: {len(low_vol)}笔, 盈利${low_vol['pnl'].sum():.2f}")
    print(f"  ADX<25: {len(low_adx)}笔, 盈利${low_adx['pnl'].sum():.2f}")
    print(f"  ADX>60: {len(high_adx)}笔, 盈利${high_adx['pnl'].sum():.2f}")

# 推荐参数调整
print(f"\n{'='*60}")
print("✅ 推荐参数调整")
print(f"{'='*60}")

# 找到最优RSI下限
best_rsi = 55
best_score = 0
for test_rsi in range(45, 60, 1):
    would_include = df[df['rsi'] > test_rsi]
    if len(would_include) > 5:
        win_rate = len(would_include[would_include['pnl'] > 0]) / len(would_include)
        avg_pnl = would_include['pnl'].mean()
        score = win_rate * avg_pnl * len(would_include)
        if score > best_score:
            best_score = score
            best_rsi = test_rsi

# 找到最优Volume下限
best_vol = 3.0
best_score = 0
for test_vol in np.arange(2.0, 4.0, 0.2):
    would_include = df[df['vol'] > test_vol]
    if len(would_include) > 5:
        win_rate = len(would_include[would_include['pnl'] > 0]) / len(would_include)
        avg_pnl = would_include['pnl'].mean()
        score = win_rate * avg_pnl * len(would_include)
        if score > best_score:
            best_score = score
            best_vol = test_vol

# 找到最优ADX下限
best_adx = 25
best_score = 0
for test_adx in range(20, 35, 1):
    would_include = df[(df['adx'] >= test_adx) & (df['adx'] <= 60)]
    if len(would_include) > 5:
        win_rate = len(would_include[would_include['pnl'] > 0]) / len(would_include)
        avg_pnl = would_include['pnl'].mean()
        score = win_rate * avg_pnl * len(would_include)
        if score > best_score:
            best_score = score
            best_adx = test_adx

print(f"\n基于盈利最大化的参数推荐:")
print(f"  RSI下限: {best_rsi} (当前: 55)")
print(f"  Volume下限: {best_vol:.1f} (当前: 3.0)")
print(f"  ADX下限: {best_adx} (当前: 25)")

# 预估改进效果
new_filter = df[(df['rsi'] > best_rsi) & (df['vol'] > best_vol) & 
                (df['adx'] >= best_adx) & (df['adx'] <= 60)]
if len(new_filter) > 0:
    new_win_rate = len(new_filter[new_filter['pnl'] > 0]) / len(new_filter) * 100
    new_avg_pnl = new_filter['pnl'].mean()
    new_total_pnl = new_filter['pnl'].sum()
    
    print(f"\n预估改进效果:")
    print(f"  交易数: {len(df)} → {len(new_filter)}")
    print(f"  胜率: {len(winners)/len(df)*100:.1f}% → {new_win_rate:.1f}%")
    print(f"  平均PnL: ${df['pnl'].mean():.2f} → ${new_avg_pnl:.2f}")
    print(f"  总PnL: ${df['pnl'].sum():.2f} → ${new_total_pnl:.2f}")

print(f"\n{'='*60}")
