"""
盈利交易特征分析脚本 (Winning Trades Analysis)

目标: 分析盈利交易的共同特征，以优化入场条件
数据源: backtest_results/csv/backtest_trades_180d.csv (需确认是否存在，或使用6months)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def analyze_winners():
    print("="*80)
    print("🏆 盈利交易深度分析")
    print("="*80)
    
    # 尝试读取数据，优先使用6个月数据（样本量大）
    file_path = 'backtest_results/csv/backtest_trades_6months.csv'
    if not Path(file_path).exists():
        file_path = 'backtest_results/csv/backtest_trades_180d.csv'
        
    if not Path(file_path).exists():
        print(f"❌ 找不到数据文件: {file_path}")
        return

    print(f"正在分析数据: {file_path}")
    df = pd.read_csv(file_path)
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    
    # 筛选盈利交易
    winners = df[df['pnl'] > 0].copy()
    losers = df[df['pnl'] < 0].copy()
    
    print(f"总交易数: {len(df)}")
    print(f"盈利交易: {len(winners)} ({len(winners)/len(df)*100:.1f}%)")
    print()
    
    # 1. 核心指标分布对比 (RSI, ADX, Volume)
    print("="*60)
    print("📊 核心指标对比 (盈利 vs 亏损)")
    print("="*60)
    
    metrics = ['rsi', 'adx', 'volume_ratio', 'upper_wick_ratio']
    
    for metric in metrics:
        if metric in df.columns:
            win_mean = winners[metric].mean()
            loss_mean = losers[metric].mean()
            print(f"{metric.upper():<15} 盈利均值: {win_mean:>8.2f} | 亏损均值: {loss_mean:>8.2f} | 差异: {(win_mean-loss_mean)/loss_mean*100:>6.1f}%")
    print()
    
    # 2. 最佳交易时间段
    print("="*60)
    print("⏰ 最佳交易时段 (UTC)")
    print("="*60)
    winners['hour'] = winners['entry_time'].dt.hour
    hourly_win_rate = df.groupby(df['entry_time'].dt.hour)['pnl'].apply(lambda x: (x > 0).mean() * 100)
    hourly_profit = df.groupby(df['entry_time'].dt.hour)['pnl'].sum()
    
    print(f"{'小时':<6} {'胜率':<10} {'总盈亏':<15}")
    print("-" * 35)
    # 按总盈亏排序的前5个时段
    top_hours = hourly_profit.sort_values(ascending=False).head(5)
    for hour in top_hours.index:
        print(f"{hour:02d}:00  {hourly_win_rate[hour]:>6.1f}%    ${hourly_profit[hour]:>10.2f}")
    print()
    
    # 3. 最佳持仓时间
    print("="*60)
    print("⏳ 持仓时间分析")
    print("="*60)
    # Convert duration string to timedelta if needed, or parse
    # Assuming duration is already timedelta or string
    # For simplicity, let's look at pnl vs duration if possible, but duration format varies
    # We'll skip complex duration parsing for now and focus on categorical
    
    # 4. 暴利交易特征 (Top 10%)
    print("="*60)
    print("🚀 暴利交易特征 (Top 10% 盈利)")
    print("="*60)
    threshold = winners['pnl'].quantile(0.90)
    big_winners = winners[winners['pnl'] >= threshold]
    
    print(f"暴利交易门槛: >${threshold:.2f}")
    print(f"平均 RSI: {big_winners['rsi'].mean():.2f}")
    print(f"平均 ADX: {big_winners['adx'].mean():.2f}")
    print(f"平均 量比: {big_winners['volume_ratio'].mean():.2f}")
    
    # 5. 建议
    print("\n" + "="*60)
    print("💡 优化建议")
    print("="*60)
    
    if big_winners['rsi'].mean() > 70:
        print("- 强者恒强: 暴利交易通常发生在 RSI > 70 的高动能区域")
    
    if big_winners['volume_ratio'].mean() > 3:
        print("- 放量突破: 必须有巨大的成交量配合 (量比 > 3)")
        
    print("- 建议: 在 check_signal 中提高 Volume 和 RSI 的门槛，只做最有把握的交易")

if __name__ == "__main__":
    analyze_winners()
