#!/usr/bin/env python3
"""
Phase 1: 时间特征提取
从ultra_leverage_backtest.csv提取并增强时间相关特征
"""
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

def extract_time_features():
    """提取时间特征"""
    
    # 加载原始回测数据
    df = pd.read_csv('ultra_leverage_backtest.csv')
    
    print(f"加载数据: {len(df)}笔交易")
    print(f"盈利: {len(df[df['pnl'] > 0])}笔 ({len(df[df['pnl'] > 0])/len(df)*100:.1f}%)")
    
    # 转换时间
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    
    # 提取时间特征
    df['hour_of_day'] = df['entry_time'].dt.hour
    df['day_of_week'] = df['entry_time'].dt.dayofweek
    df['day_of_month'] = df['entry_time'].dt.day
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # 交易时段分类
    def classify_session(hour):
        if 0 <= hour < 8:
            return 'Asia'
        elif 8 <= hour < 16:
            return 'Europe'
        else:
            return 'US'
    
    df['trading_session'] = df['hour_of_day'].apply(classify_session)
    
    # 是否在最佳时段
    df['is_prime_hour'] = df['hour_of_day'].isin([15, 16, 17, 21, 22, 23]).astype(int)
    
    # 持仓时长特征
    df['duration_minutes'] = (df['exit_time'] - df['entry_time']).dt.total_seconds() / 60
    
    # 快速交易标记
    df['is_quick_trade'] = (df['duration_minutes'] <= 15).astype(int)  # 15分钟内
    
    # 盈亏标记
    df['is_winner'] = (df['pnl'] > 0).astype(int)
    
    # 保存增强数据
    output_path = 'data/enriched/trades_with_time_features.csv'
    df.to_csv(output_path, index=False)
    print(f"\n✅ 时间特征已提取，保存至: {output_path}")
    
    return df

def analyze_time_patterns(df):
    """分析时间模式"""
    
    winners = df[df['is_winner'] == 1]
    losers = df[df['is_winner'] == 0]
    
    print("\n" + "="*60)
    print("📊 时间维度分析")
    print("="*60)
    
    # 1. 小时分布
    print("\n### 1. 小时胜率分布")
    hour_stats = df.groupby('hour_of_day').agg({
        'is_winner': ['mean', 'count']
    }).round(3)
    hour_stats.columns = ['win_rate', 'count']
    hour_stats = hour_stats.sort_values('win_rate', ascending=False)
    print(hour_stats.head(10))
    
    # 2. 交易时段
    print("\n### 2. 交易时段胜率")
    session_stats = df.groupby('trading_session').agg({
        'is_winner': ['mean', 'count'],
        'roi': ['mean']
    }).round(3)
    session_stats.columns = ['win_rate', 'count', 'avg_roi']
    print(session_stats)
    
    # 3. 星期效应
    print("\n### 3. 星期效应 (0=周一, 6=周日)")
    dow_stats = df.groupby('day_of_week').agg({
        'is_winner': ['mean', 'count']
    }).round(3)
    dow_stats.columns = ['win_rate', 'count']
    print(dow_stats)
    
    # 4. 周末vs工作日
    print("\n### 4. 周末 vs 工作日")
    weekend_stats = df.groupby('is_weekend').agg({
        'is_winner': ['mean', 'count']
    }).round(3)
    weekend_stats.columns = ['win_rate', 'count']
    weekend_stats.index = ['Weekday', 'Weekend']
    print(weekend_stats)
    
    # 5. 持仓时长
    print("\n### 5. 持仓时长分析")
    print(f"平均持仓时长: {df['duration_minutes'].mean():.1f}分钟")
    print(f"盈利交易平均时长: {winners['duration_minutes'].mean():.1f}分钟")
    print(f"亏损交易平均时长: {losers['duration_minutes'].mean():.1f}分钟")
    
    quick_win_rate = df[df['is_quick_trade'] == 1]['is_winner'].mean()
    slow_win_rate = df[df['is_quick_trade'] == 0]['is_winner'].mean()
    print(f"15分钟内平仓胜率: {quick_win_rate:.1%}")
    print(f"超过15分钟胜率: {slow_win_rate:.1%}")
    
    # 6. 最佳时段
    print("\n### 6. 最佳时段 (15-17, 21-23)")
    prime_stats = df.groupby('is_prime_hour').agg({
        'is_winner': ['mean', 'count']
    }).round(3)
    prime_stats.columns = ['win_rate', 'count']
    prime_stats.index = ['Other Hours', 'Prime Hours']
    print(prime_stats)
    
    # 7. 关键发现
    print("\n" + "="*60)
    print("🔍 关键发现")
    print("="*60)
    
    # 找出最佳/最差小时
    best_hour = hour_stats.index[0]
    best_hour_wr = hour_stats.iloc[0]['win_rate']
    worst_hour = hour_stats.index[-1]
    worst_hour_wr = hour_stats.iloc[-1]['win_rate']
    
    print(f"✅ 最佳交易小时: {best_hour}点 (胜率 {best_hour_wr:.1%})")
    print(f"❌ 最差交易小时: {worst_hour}点 (胜率 {worst_hour_wr:.1%})")
    
    # 找出最佳时段
    best_session = session_stats['win_rate'].idxmax()
    best_session_wr = session_stats.loc[best_session, 'win_rate']
    print(f"✅ 最佳交易时段: {best_session} (胜率 {best_session_wr:.1%})")
    
    # 持仓时长建议
    if quick_win_rate > slow_win_rate:
        print(f"⚡ 建议: 快进快出策略更有效 (15分钟内胜率高 {(quick_win_rate - slow_win_rate)*100:.1f}%)")
    else:
        print(f"⏳ 建议: 耐心持有更有效 (长持仓胜率高 {(slow_win_rate - quick_win_rate)*100:.1f}%)")

def main():
    # Phase 1: 提取特征
    df = extract_time_features()
    
    # Phase 2: 分析模式
    analyze_time_patterns(df)
    
    print("\n" + "="*60)
    print("✅ Phase 1 完成！")
    print("="*60)
    print("\n下一步:")
    print("1. python3 scripts/research/coin_features.py  # 提取币种特征")
    print("2. python3 scripts/research/market_features.py  # 提取市场特征")

if __name__ == "__main__":
    main()
