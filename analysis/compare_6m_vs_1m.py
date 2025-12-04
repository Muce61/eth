"""
6个月 vs 1个月回测对比分析脚本
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_backtest_comparison():
    print("="*80)
    print("📊 6个月 vs 1个月回测对比分析")
    print("="*80)
    print()
    
    # 读取数据
    df_6m = pd.read_csv('backtest_results/csv/backtest_trades_6months.csv')
    df_1m = pd.read_csv('backtest_results/csv/backtest_trades_october.csv')
    
    # 转换时间
    df_6m['exit_time'] = pd.to_datetime(df_6m['exit_time'])
    df_1m['exit_time'] = pd.to_datetime(df_1m['exit_time'])
    
    # 计算指标的函数
    def calc_metrics(df, initial_balance=100):
        total_trades = len(df)
        winning_trades = len(df[df['pnl'] > 0])
        losing_trades = len(df[df['pnl'] < 0])
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        
        total_profit = df[df['pnl'] > 0]['pnl'].sum()
        total_loss = abs(df[df['pnl'] < 0]['pnl'].sum())
        profit_factor = total_profit / total_loss if total_loss > 0 else 0
        
        final_balance = initial_balance + df['pnl'].sum()
        total_return = (final_balance - initial_balance) / initial_balance * 100
        
        # 计算最大回撤
        df_sorted = df.sort_values('exit_time')
        cumulative_pnl = df_sorted['pnl'].cumsum()
        balance_curve = initial_balance + cumulative_pnl
        running_max = balance_curve.expanding().max()
        drawdown = (balance_curve - running_max) / running_max * 100
        max_drawdown = abs(drawdown.min())
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'total_return': total_return,
            'final_balance': final_balance,
            'max_drawdown': max_drawdown,
            'avg_win': df[df['pnl'] > 0]['pnl'].mean() if winning_trades > 0 else 0,
            'avg_loss': df[df['pnl'] < 0]['pnl'].mean() if losing_trades > 0 else 0
        }
    
    # 计算6个月和1个月的指标
    metrics_6m = calc_metrics(df_6m)
    metrics_1m = calc_metrics(df_1m)
    
    # 打印对比结果
    print("="*60)
    print("整体表现对比")
    print("="*60)
    print(f"{'指标':<20} {'6个月':<20} {'10月份':<20} {'差异':<20}")
    print("-"*80)
    print(f"{'总收益率':<20} {metrics_6m['total_return']:>18.2f}% {metrics_1m['total_return']:>18.2f}% {metrics_6m['total_return']-metrics_1m['total_return']:>18.2f}%")
    print(f"{'最终余额':<20} ${metrics_6m['final_balance']:>17.2f} ${metrics_1m['final_balance']:>17.2f} ${metrics_6m['final_balance']-metrics_1m['final_balance']:>17.2f}")
    print(f"{'胜率':<20} {metrics_6m['win_rate']:>18.2f}% {metrics_1m['win_rate']:>18.2f}% {metrics_6m['win_rate']-metrics_1m['win_rate']:>18.2f}%")
    print(f"{'最大回撤':<20} {metrics_6m['max_drawdown']:>18.2f}% {metrics_1m['max_drawdown']:>18.2f}% {metrics_6m['max_drawdown']-metrics_1m['max_drawdown']:>18.2f}%")
    print(f"{'盈亏比':<20} {metrics_6m['profit_factor']:>18.2f} {metrics_1m['profit_factor']:>18.2f} {metrics_6m['profit_factor']-metrics_1m['profit_factor']:>18.2f}")
    print(f"{'交易数量':<20} {metrics_6m['total_trades']:>18} {metrics_1m['total_trades']:>18} {metrics_6m['total_trades']-metrics_1m['total_trades']:>18}")
    print()
    
    # 按月份分析6个月数据
    print("="*60)
    print("6个月回测 - 月度表现分析")
    print("="*60)
    
    df_6m['month'] = df_6m['exit_time'].dt.to_period('M')
    monthly_stats = []
    
    for month in df_6m['month'].unique():
        month_data = df_6m[df_6m['month'] == month]
        month_metrics = calc_metrics(month_data, initial_balance=100)
        monthly_stats.append({
            'month': str(month),
            'trades': month_metrics['total_trades'],
            'win_rate': month_metrics['win_rate'],
            'return': month_metrics['total_return'],
            'max_dd': month_metrics['max_drawdown']
        })
    
    monthly_df = pd.DataFrame(monthly_stats).sort_values('month')
    print(f"{'月份':<15} {'交易数':<10} {'胜率':<10} {'收益率':<15} {'最大回撤':<15}")
    print("-"*65)
    for _, row in monthly_df.iterrows():
        print(f"{row['month']:<15} {row['trades']:<10} {row['win_rate']:>8.2f}% {row['return']:>13.2f}% {row['max_dd']:>13.2f}%")
    
    print()
    print("="*60)
    print("关键发现")
    print("="*60)
    
    # 分析差异原因
    if metrics_6m['total_return'] < metrics_1m['total_return']:
        print(f"⚠️  6个月平均表现 ({metrics_6m['total_return']:.2f}%) 低于10月单月 ({metrics_1m['total_return']:.2f}%)")
        print(f"   可能原因:")
        print(f"   1. 10月是特殊的高收益月份（牛市/剧烈波动期）")
        print(f"   2. 其他月份可能遭遇震荡市或熊市，拉低了整体表现")
        print(f"   3. 建议查看上面的月度分析，找出拖后腿的月份")
    else:
        print(f"✅ 6个月表现优于10月单月")
    
    print()
    if metrics_6m['win_rate'] < metrics_1m['win_rate']:
        print(f"⚠️  6个月平均胜率 ({metrics_6m['win_rate']:.2f}%) 低于10月 ({metrics_1m['win_rate']:.2f}%)")
        print(f"   可能原因: 市场环境差异，某些月份趋势性较弱")
    
    print()
    if metrics_6m['max_drawdown'] > metrics_1m['max_drawdown']:
        print(f"⚠️  6个月最大回撤 ({metrics_6m['max_drawdown']:.2f}%) 大于10月 ({metrics_1m['max_drawdown']:.2f}%)")
        print(f"   可能原因: 长期运行中累积了更大的连续亏损")

if __name__ == "__main__":
    analyze_backtest_comparison()
