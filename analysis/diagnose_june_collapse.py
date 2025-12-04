"""
6月份崩盘深度诊断脚本
"""

import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter

def analyze_june_collapse():
    print("="*80)
    print("🔍 6月份崩盘深度诊断")
    print("="*80)
    print()
    
    # 读取数据
    df = pd.read_csv('backtest_results/csv/backtest_trades_6months.csv')
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    
    # 筛选6月数据
    df_june = df[(df['exit_time'].dt.month == 6) & (df['exit_time'].dt.year == 2025)].copy()
    
    print(f"6月总交易数: {len(df_june)}")
    print(f"盈利交易: {len(df_june[df_june['pnl'] > 0])}")
    print(f"亏损交易: {len(df_june[df_june['pnl'] < 0])}")
    print(f"总盈亏: ${df_june['pnl'].sum():.2f}")
    print()
    
    # 1. 最惨烈的亏损交易 (Top 20)
    print("="*80)
    print("💀 TOP 20 最惨烈亏损交易")
    print("="*80)
    worst_trades = df_june.nsmallest(20, 'pnl')[['symbol', 'entry_time', 'exit_time', 'pnl', 'reason']]
    print(worst_trades.to_string(index=False))
    print(f"\nTop 20亏损总计: ${worst_trades['pnl'].sum():.2f}")
    print()
    
    # 2. 按币种统计亏损
    print("="*80)
    print("📊 亏损最严重的币种 (Top 15)")
    print("="*80)
    coin_pnl = df_june.groupby('symbol')['pnl'].agg(['sum', 'count', 'mean']).sort_values('sum')
    print(coin_pnl.head(15).to_string())
    print()
    
    # 3. 按日期统计
    print("="*80)
    print("📅 每日盈亏分布")
    print("="*80)
    df_june['date'] = df_june['exit_time'].dt.date
    daily_pnl = df_june.groupby('date').agg({
        'pnl': ['sum', 'count']
    }).round(2)
    daily_pnl.columns = ['日盈亏', '交易数']
    print(daily_pnl.to_string())
    
    worst_day = daily_pnl['日盈亏'].idxmin()
    worst_day_loss = daily_pnl.loc[worst_day, '日盈亏']
    print(f"\n最惨的一天: {worst_day}, 亏损: ${worst_day_loss:.2f}")
    print()
    
    # 4. 平仓原因分析
    print("="*80)
    print("🎯 平仓原因统计")
    print("="*80)
    reason_stats = df_june.groupby('reason').agg({
        'pnl': ['sum', 'count', 'mean']
    }).round(2)
    reason_stats.columns = ['总盈亏', '次数', '平均盈亏']
    print(reason_stats.sort_values('总盈亏').to_string())
    print()
    
    # 5. 计算真实的资金曲线和回撤
    print("="*80)
    print("📉 资金曲线与回撤分析")
    print("="*80)
    
    # 获取5月底余额
    df_may = df[(df['exit_time'].dt.month == 5) & (df['exit_time'].dt.year == 2025)]
    if len(df_may) > 0:
        may_end_balance = 100 + df_may['pnl'].sum()
    else:
        may_end_balance = 100
    
    print(f"5月底余额: ${may_end_balance:.2f}")
    
    # 按时间排序计算资金曲线
    df_june_sorted = df_june.sort_values('exit_time').copy()
    df_june_sorted['balance'] = may_end_balance + df_june_sorted['pnl'].cumsum()
    
    max_balance = df_june_sorted['balance'].expanding().max()
    max_balance_june = max_balance.max()
    min_balance_june = df_june_sorted['balance'].min()
    
    print(f"6月最高余额: ${max_balance_june:.2f}")
    print(f"6月最低余额: ${min_balance_june:.2f}")
    
    # 计算最大回撤
    drawdown = (df_june_sorted['balance'] - max_balance) / max_balance * 100
    max_dd_idx = drawdown.idxmin()
    max_drawdown = abs(drawdown.min())
    
    print(f"最大回撤: {max_drawdown:.2f}%")
    print(f"最大回撤发生时间: {df_june_sorted.loc[max_dd_idx, 'exit_time']}")
    print(f"最大回撤时余额: ${df_june_sorted.loc[max_dd_idx, 'balance']:.2f}")
    
    june_end_balance = df_june_sorted['balance'].iloc[-1]
    print(f"6月底余额: ${june_end_balance:.2f}")
    print(f"6月总收益率: {(june_end_balance - may_end_balance) / may_end_balance * 100:.2f}%")
    print()
    
    # 6. 连续亏损分析
    print("="*80)
    print("🔥 连续亏损分析")
    print("="*80)
    
    df_june_sorted['is_loss'] = df_june_sorted['pnl'] < 0
    
    # 找出最长连续亏损
    max_streak = 0
    current_streak = 0
    max_streak_loss = 0
    current_streak_loss = 0
    
    for _, row in df_june_sorted.iterrows():
        if row['is_loss']:
            current_streak += 1
            current_streak_loss += row['pnl']
            if current_streak > max_streak:
                max_streak = current_streak
                max_streak_loss = current_streak_loss
        else:
            current_streak = 0
            current_streak_loss = 0
    
    print(f"最长连续亏损: {max_streak}笔")
    print(f"最长连亏总损失: ${max_streak_loss:.2f}")
    print()
    
    # 7. 疑似异常交易
    print("="*80)
    print("⚠️  疑似异常交易 (单笔亏损 > $50)")
    print("="*80)
    abnormal = df_june[df_june['pnl'] < -50][['symbol', 'entry_time', 'exit_time', 'pnl', 'reason']]
    if len(abnormal) > 0:
        print(abnormal.to_string(index=False))
        print(f"\n异常交易数量: {len(abnormal)}")
        print(f"异常交易总亏损: ${abnormal['pnl'].sum():.2f}")
    else:
        print("未发现单笔亏损超过$50的异常交易")
    print()
    
    # 8. 关键结论
    print("="*80)
    print("💡 关键结论")
    print("="*80)
    
    stop_loss_count = len(df_june[df_june['reason'] == 'Stop Loss'])
    total_count = len(df_june)
    
    print(f"1. 止损比例: {stop_loss_count}/{total_count} = {stop_loss_count/total_count*100:.1f}%")
    
    win_rate = len(df_june[df_june['pnl'] > 0]) / total_count * 100
    print(f"2. 胜率: {win_rate:.2f}%")
    
    if max_drawdown > 50:
        print(f"3. ⚠️ 回撤超过50% ({max_drawdown:.2f}%)，说明存在严重风险管理问题")
    
    if len(abnormal) > 0:
        print(f"4. ⚠️ 发现{len(abnormal)}笔异常大额亏损，建议检查数据或策略逻辑")
    
    # 保存6月详细数据
    output_path = 'analysis/june_collapse_details.csv'
    df_june_sorted.to_csv(output_path, index=False)
    print(f"\n6月详细数据已保存至: {output_path}")

if __name__ == "__main__":
    analyze_june_collapse()
