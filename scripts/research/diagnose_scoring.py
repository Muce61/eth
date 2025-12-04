#!/usr/bin/env python3
"""
Phase 4: 评分系统诊断
深入分析为何高分交易胜率反而更低
"""
import pandas as pd
import numpy as np

def analyze_score_breakdown():
    """分析各评分维度与胜率的关系"""
    
    # 加载数据
    df = pd.read_csv('ultra_leverage_backtest.csv')
    
    print("="*70)
    print("🔬 评分系统深度诊断")
    print("="*70)
    
    # 添加胜负标记
    df['is_winner'] = (df['pnl'] > 0).astype(int)
    
    # 按信号强度分组
    df['score_bin'] = pd.cut(df['signal_strength'], 
                               bins=[0, 70, 75, 80, 85, 90, 100],
                               labels=['<70', '70-75', '75-80', '80-85', '85-90', '90+'])
    
    print("\n### 1. 信号强度 vs 胜率详细分析")
    score_stats = df.groupby('score_bin').agg({
        'is_winner': ['mean', 'count'],
        'pnl': 'sum',
        'roi': 'mean'
    }).round(3)
    score_stats.columns = ['win_rate', 'count', 'total_pnl', 'avg_roi']
    print(score_stats)
    
    # 关键发现
    print("\n⚠️ 关键异常:")
    if '90+' in score_stats.index:
        high_score_wr = score_stats.loc['90+', 'win_rate']
        low_score_wr = score_stats.loc['70-75', 'win_rate']
        print(f"90+分胜率: {high_score_wr:.1%}")
        print(f"70-75分胜率: {low_score_wr:.1%}")
        if high_score_wr < low_score_wr:
            print(f"❌ 高分反而比低分差 {(low_score_wr - high_score_wr)*100:.1f}%")
    
    # 尝试提取breakdown数据（如果存在）
    # Note: breakdown存储在signal_strength字段，需要特殊处理
    # 我们需要分析每个评分维度的贡献
    
    print("\n### 2. 各分数段的盈亏分布")
    for score_bin in score_stats.index:
        subset = df[df['score_bin'] == score_bin]
        if len(subset) > 0:
            wins = len(subset[subset['is_winner'] == 1])
            losses = len(subset[subset['is_winner'] == 0])
            total_pnl = subset['pnl'].sum()
            print(f"\n{score_bin}分段:")
            print(f"  盈利: {wins}笔, 亏损: {losses}笔")
            print(f"  累计PnL: ${total_pnl:.2f}")
            print(f"  平均ROI: {subset['roi'].mean():.1f}%")
    
    return df, score_stats

def analyze_by_indicators(df):
    """基于指标反向分析"""
    
    print("\n" + "="*70)
    print("🔍 指标有效性分析")
    print("="*70)
    
    # 我们需要加载包含详细指标的数据
    # 从增强数据加载
    try:
        df_enriched = pd.read_csv('data/enriched/trades_with_coin_features.csv')
        df = df.merge(df_enriched[['entry_time', 'hour_of_day', 'trading_session', 'is_quick_trade']], 
                       on='entry_time', how='left')
    except:
        print("⚠️ 无法加载增强数据，跳过详细指标分析")
        return df
    
    print("\n### 3. 交易时段 vs 信号强度 vs 胜率")
    # 分析高分交易集中在哪些时段
    high_score = df[df['signal_strength'] >= 90]
    if len(high_score) > 0:
        print("\n90+分交易时段分布:")
        session_dist = high_score.groupby('trading_session').agg({
            'is_winner': ['mean', 'count']
        }).round(3)
        session_dist.columns = ['win_rate', 'count']
        print(session_dist)
        
        print("\n90+分交易小时分布:")
        hour_dist = high_score.groupby('hour_of_day').agg({
            'is_winner': ['mean', 'count']
        }).round(3)
        hour_dist.columns = ['win_rate', 'count']
        print(hour_dist.head(10))
    
    return df

def identify_toxic_signals(df):
    """识别"有毒"的信号组合"""
    
    print("\n" + "="*70)
    print("☠️ 有毒信号识别")
    print("="*70)
    
    # 高分但低胜率的组合
    high_score_losers = df[(df['signal_strength'] >= 85) & (df['is_winner'] == 0)]
    
    print(f"\n发现 {len(high_score_losers)} 笔高分亏损交易 (≥85分)")
    
    if len(high_score_losers) > 0:
        # 分析这些交易的共同特征
        print("\n### 币种分布:")
        print(high_score_losers['symbol'].value_counts().head(10))
        
        print("\n### 方向分布:")
        print(high_score_losers['side'].value_counts())
        
        # 持仓时长
        if 'duration' in high_score_losers.columns:
            avg_duration = pd.to_timedelta(high_score_losers['duration']).mean()
            print(f"\n### 平均持仓时长: {avg_duration}")
        
        # ROI分布
        print("\n### ROI统计:")
        print(f"平均ROI: {high_score_losers['roi'].mean():.1f}%")
        print(f"中位ROI: {high_score_losers['roi'].median():.1f}%")
        print(f"最差ROI: {high_score_losers['roi'].min():.1f}%")
    
    return high_score_losers

def recommend_fixes(score_stats, high_score_losers):
    """基于分析推荐修复方案"""
    
    print("\n" + "="*70)
    print("💊 修复建议")
    print("="*70)
    
    fixes = []
    
    # 1. 阈值调整
    if '90+' in score_stats.index:
        wr_90 = score_stats.loc['90+', 'win_rate']
        best_bin = score_stats['win_rate'].idxmax()
        best_wr = score_stats.loc[best_bin, 'win_rate']
        
        print(f"\n### 1. 阈值策略")
        print(f"当前最佳分数段: {best_bin} (胜率 {best_wr:.1%})")
        
        if best_bin != '90+':
            # 找出最佳阈值下限
            if best_bin == '80-85':
                print("建议: 阈值设为 80-85 区间")
                fixes.append("THRESHOLD = 80  # 下限")
                fixes.append("THRESHOLD_MAX = 85  # 上限，拒绝过高分数")
            elif best_bin == '75-80':
                print("建议: 阈值设为 75-80 区间")
                fixes.append("THRESHOLD = 75")
                fixes.append("THRESHOLD_MAX = 80")
    
    # 2. 时间过滤强化
    print("\n### 2. 时间过滤")
    print("建议: 进一步缩小交易时段")
    fixes.append("BEST_HOURS = [6, 9]  # 只保留最优时段")
    
    # 3. 信号权重重构
    print("\n### 3. 评分权重调整")
    print("疑似问题:")
    print("- 趋势一致性权重过高(30分)")
    print("- 动量指标可能失效(40分)")
    print("- RSI标准可能反向")
    
    fixes.append("# 降低趋势权重")
    fixes.append("trend_weight = 20  # 从30降至20")
    fixes.append("momentum_weight = 30  # 从40降至30")
    fixes.append("# 增加实证有效指标权重")
    fixes.append("time_weight = 25  # 时间最重要")
    print("\n### 4. 生成优化配置")
    
    config_code = "\n".join(fixes)
    
    with open('data/enriched/recommended_fixes.py', 'w') as f:
        f.write("# 基于Phase 4分析的推荐修复方案\n\n")
        f.write(config_code)
    
    print("✅ 修复方案已保存至: data/enriched/recommended_fixes.py")

def main():
    print("Phase 4: 评分系统诊断")
    print("目标: 找出为何90+分胜率低于70-80分\n")
    
    # 1. 评分vs胜率分析
    df, score_stats = analyze_score_breakdown()
    
    # 2. 指标有效性分析
    df = analyze_by_indicators(df)
    
    # 3. 识别有毒信号
    high_score_losers = identify_toxic_signals(df)
    
    # 4. 推荐修复方案
    recommend_fixes(score_stats, high_score_losers)
    
    print("\n" + "="*70)
    print("✅ Phase 4 诊断完成")
    print("="*70)
    print("\n关键发现总结:")
    print("1. 需要查看具体哪些指标维度在90+分交易中占比高")
    print("2. 可能需要设置信号强度上限(拒绝过高分数)")
    print("3. 时间权重应该大幅提升")
    print("\n下一步:")
    print("1. 应用修复方案重新回测")
    print("2. 考虑机器学习重新训练评分模型")

if __name__ == "__main__":
    main()
