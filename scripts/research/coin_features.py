#!/usr/bin/env python3
"""
Phase 2: 币种特征分析
分析不同币种的胜率模式
"""
import pandas as pd
import numpy as np
from pathlib import Path

def load_leverage_data():
    """加载杠杆数据作为币种特征"""
    lev_df = pd.read_csv('leverage_brackets.csv')
    # Symbol格式转换: BTCUSDT -> BTCUSDTUSDT
    lev_df['symbol_data'] = lev_df['symbol'] + 'USDT'
    return lev_df[['symbol_data', 'max_leverage']].rename(columns={'symbol_data': 'symbol'})

def analyze_coin_patterns():
    """分析币种特征"""
    
    # 加载交易数据
    df = pd.read_csv('data/enriched/trades_with_time_features.csv')
    
    # 加载杠杆数据
    lev_df = load_leverage_data()
    df = df.merge(lev_df, on='symbol', how='left')
    
    print(f"分析 {len(df)} 笔交易")
    print(f"涉及 {df['symbol'].nunique()} 个币种")
    
    # 按币种统计
    coin_stats = df.groupby('symbol').agg({
        'is_winner': ['sum', 'count', 'mean'],
        'pnl': ['sum', 'mean'],
        'roi': 'mean',
        'max_leverage': 'first'
    }).round(3)
    
    coin_stats.columns = ['wins', 'total_trades', 'win_rate', 'total_pnl', 'avg_pnl', 'avg_roi', 'max_leverage']
    coin_stats = coin_stats.sort_values('win_rate', ascending=False)
    
    # 保存详细数据
    coin_stats.to_csv('data/enriched/coin_analysis.csv')
    
    print("\n" + "="*70)
    print("📊 币种维度分析")
    print("="*70)
    
    # 1. Top胜率币种
    print("\n### 1. Top 20 高胜率币种 (至少10笔交易)")
    top_winners = coin_stats[coin_stats['total_trades'] >= 10].head(20)
    print(top_winners[['wins', 'total_trades', 'win_rate', 'total_pnl', 'max_leverage']])
    
    # 2. 交易最频繁币种
    print("\n### 2. Top 10 交易最频繁币种")
    most_traded = coin_stats.sort_values('total_trades', ascending=False).head(10)
    print(most_traded[['total_trades', 'win_rate', 'total_pnl', 'max_leverage']])
    
    # 3. 盈利最多币种
    print("\n### 3. Top 10 累计盈利最高币种")
    most_profitable = coin_stats.sort_values('total_pnl', ascending=False).head(10)
    print(most_profitable[['total_trades', 'win_rate', 'total_pnl', 'max_leverage']])
    
    # 4. 杠杆vs胜率
    print("\n### 4. 杠杆级别 vs 胜率")
    # 按杠杆分组
    df['lev_group'] = pd.cut(df['max_leverage'], 
                              bins=[0, 20, 50, 75, 100, 200],
                              labels=['5-20x', '21-50x', '51-75x', '76-100x', '100x+'])
    
    lev_stats = df.groupby('lev_group').agg({
        'is_winner': ['mean', 'count'],
        'pnl': 'sum'
    }).round(3)
    lev_stats.columns = ['win_rate', 'count', 'total_pnl']
    print(lev_stats)
    
    # 5. 主流币 vs 山寨币
    print("\n### 5. 主流币 vs 山寨币")
    major_coins = ['BTCUSDTUSDT', 'ETHUSDTUSDT', 'BNBUSDTUSDT', 'SOLUSDTUSDT', 'XRPUSDTUSDT']
    df['is_major'] = df['symbol'].isin(major_coins).astype(int)
    
    major_stats = df.groupby('is_major').agg({
        'is_winner': ['mean', 'count'],
        'pnl': 'sum',
        'roi': 'mean'
    }).round(3)
    major_stats.columns = ['win_rate', 'count', 'total_pnl', 'avg_roi']
    major_stats.index = ['Altcoins', 'Major Coins']
    print(major_stats)
    
    # 6. 单币种深度分析（BTC和ETH）
    print("\n### 6. BTC vs ETH 详细对比")
    for coin in ['BTCUSDTUSDT', 'ETHUSDTUSDT']:
        coin_data = df[df['symbol'] == coin]
        if len(coin_data) > 0:
            wr = coin_data['is_winner'].mean()
            count = len(coin_data)
            total_pnl = coin_data['pnl'].sum()
            avg_roi = coin_data['roi'].mean()
            print(f"\n{coin}:")
            print(f"  交易数: {count}")
            print(f"  胜率: {wr:.1%}")
            print(f"  累计PnL: ${total_pnl:.2f}")
            print(f"  平均ROI: {avg_roi:.1f}%")
    
    # 7. 关键发现
    print("\n" + "="*70)
    print("🔍 关键发现")
    print("="*70)
    
    # 找出最佳币种
    if len(top_winners) > 0:
        best_coin = top_winners.index[0]
        best_wr = top_winners.iloc[0]['win_rate']
        best_trades = int(top_winners.iloc[0]['total_trades'])
        print(f"✅ 最佳币种: {best_coin} (胜率 {best_wr:.1%}, {best_trades}笔)")
    
    # 高杠杆币种表现
    high_lev_coins = coin_stats[coin_stats['max_leverage'] >= 100]
    if len(high_lev_coins) > 0:
        avg_wr_high_lev = high_lev_coins['win_rate'].mean()
        print(f"⚡ 高杠杆币种(≥100x)平均胜率: {avg_wr_high_lev:.1%}")
    
    # 主流vs山寨
    if 'Altcoins' in major_stats.index:
        alt_wr = major_stats.loc['Altcoins', 'win_rate']
        maj_wr = major_stats.loc['Major Coins', 'win_rate']
        diff = maj_wr - alt_wr
        if diff > 0:
            print(f"💎 主流币胜率高于山寨币 {diff:.1%}")
        else:
            print(f"🎲 山寨币胜率高于主流币 {abs(diff):.1%}")
    
    # 保存增强数据
    df.to_csv('data/enriched/trades_with_coin_features.csv', index=False)
    print(f"\n✅ 币种特征已提取，保存至: data/enriched/trades_with_coin_features.csv")
    
    return df, coin_stats

def identify_white_black_lists(coin_stats, min_trades=10):
    """识别白名单和黑名单币种"""
    
    print("\n" + "="*70)
    print("📋 币种白名单/黑名单")
    print("="*70)
    
    # 过滤：至少min_trades笔交易
    qualified = coin_stats[coin_stats['total_trades'] >= min_trades]
    
    # 白名单：胜率 > 40%
    whitelist = qualified[qualified['win_rate'] > 0.40].index.tolist()
    print(f"\n✅ 白名单 (胜率>40%, 至少{min_trades}笔): {len(whitelist)}个币种")
    for coin in whitelist[:15]:
        wr = qualified.loc[coin, 'win_rate']
        count = int(qualified.loc[coin, 'total_trades'])
        pnl = qualified.loc[coin, 'total_pnl']
        print(f"  - {coin}: {wr:.1%} ({count}笔, PnL: ${pnl:.2f})")
    
    # 黑名单：胜率 < 20%
    blacklist = qualified[qualified['win_rate'] < 0.20].index.tolist()
    print(f"\n❌ 黑名单 (胜率<20%, 至少{min_trades}笔): {len(blacklist)}个币种")
    for coin in blacklist[:15]:
        wr = qualified.loc[coin, 'win_rate']
        count = int(qualified.loc[coin, 'total_trades'])
        pnl = qualified.loc[coin, 'total_pnl']
        print(f"  - {coin}: {wr:.1%} ({count}笔, PnL: ${pnl:.2f})")
    
    # 保存名单
    with open('data/enriched/coin_whitelist.txt', 'w') as f:
        f.write('\n'.join(whitelist))
    
    with open('data/enriched/coin_blacklist.txt', 'w') as f:
        f.write('\n'.join(blacklist))
    
    print(f"\n✅ 白名单已保存: data/enriched/coin_whitelist.txt")
    print(f"⛔ 黑名单已保存: data/enriched/coin_blacklist.txt")
    
    return whitelist, blacklist

def main():
    print("Phase 2: 币种特征分析")
    print("="*70)
    
    # 分析币种模式
    df, coin_stats = analyze_coin_patterns()
    
    # 生成白名单/黑名单
    whitelist, blacklist = identify_white_black_lists(coin_stats, min_trades=10)
    
    print("\n" + "="*70)
    print("✅ Phase 2 完成！")
    print("="*70)
    print(f"\n发现:")
    print(f"- 白名单币种: {len(whitelist)}个")
    print(f"- 黑名单币种: {len(blacklist)}个")
    print(f"\n下一步:")
    print("1. python3 scripts/research/market_features.py  # 分析市场环境")
    print("2. python3 scripts/research/apply_filters.py    # 应用发现优化策略")

if __name__ == "__main__":
    main()
