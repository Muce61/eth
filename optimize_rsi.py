#!/usr/bin/env python3
"""
简化版参数优化 - RSI阈值扫描
快速测试RSI对收益的影响
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
import importlib

def test_rsi_threshold(rsi_value):
    """测试特定RSI阈值"""
    print(f"\n{'='*60}")
    print(f"🎯 测试 RSI > {rsi_value}")
    print(f"={'='*60}")
    
    # 读取策略文件
    strategy_file = 'strategy/momentum.py'
    with open(strategy_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 备份原始内容
    original_content = content
    
    #  修改LONG RSI阈值 (line 144: if rsi > 55)
    content = content.replace('if rsi > 55:', f'if rsi > {rsi_value}:')
    
    # 写入临时修改
    with open(strategy_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    try:
        # 重新加载模块
        if 'backtest.real_engine' in sys.modules:
            del sys.modules['backtest.real_engine']
        if 'strategy.momentum' in sys.modules:
            del sys.modules['strategy.momentum']
        
        from backtest.real_engine import RealBacktestEngine
        
        # 运行回测
        engine = RealBacktestEngine()
        engine.load_data()
        
        start = datetime(2025, 6, 1)
        end = datetime(2025, 6, 30)
        
        # 清除之前的trades(避免缓存影响)
        engine.trades = []
        engine.balance = 100.0
        
        engine.run(start_date=start, end_date=end)
        
        # 计算结果
        total_return = ((engine.balance - 100.0) / 100.0) * 100
        win_trades = [t for t in engine.trades if t['pnl'] > 0]
        loss_trades = [t for t in engine.trades if t['pnl'] <= 0]
        win_rate = len(win_trades) / len(engine.trades) * 100 if engine.trades else 0
        
        result = {
            'rsi': rsi_value,
            'return': total_return,
            'trades': len(engine.trades),
            'win_rate': win_rate,
            'avg_win': sum([t['pnl'] for t in win_trades]) / len(win_trades) if win_trades else 0,
            'avg_loss': sum([t['pnl'] for t in loss_trades]) / len(loss_trades) if loss_trades else 0
        }
        
        print(f"📊 结果:")
        print(f"  收益率: {result['return']:.2f}%")
        print(f"  交易数: {result['trades']}")
        print(f"  胜率: {result['win_rate']:.1f}%")
        print(f"  平均盈: ${result['avg_win']:.2f}")
        print(f"  平均亏: ${result['avg_loss']:.2f}")
        
        return result
        
    finally:
        # 恢复原始文件
        with open(strategy_file, 'w', encoding='utf-8') as f:
            f.write(original_content)

if __name__ == "__main__":
    results = []
    
    print("\n" + "="*60)
    print("🚀 RSI阈值优化扫描")
    print("="*60)
    print("基准: RSI > 55 (当前配置)")
    print("测试范围: [55, 57, 60, 62, 65]")
    
    for rsi in [55, 57, 60, 62, 65]:
        result = test_rsi_threshold(rsi)
        results.append(result)
    
    # 输出总结
    print("\n" + "="*60)
    print("📈 优化结果总结")
    print("="*60)
    
    best = max(results, key=lambda x: x['return'])
    baseline = results[0]
    
    print(f"\n🏆 最优配置: RSI > {best['rsi']}")
    print(f"  收益率: {best['return']:.2f}% (基准: {baseline['return']:.2f}%)")
    print(f"  提升: {best['return'] - baseline['return']:.2f}%")
    print(f"  胜率: {best['win_rate']:.1f}%")
    
    print(f"\n📊 所有配置对比:")
    print(f"{'RSI阈值':<10} {'收益率%':<10} {'交易数':<10} {'胜率%':<10}")
    print("-" * 40)
    for r in results:
        print(f"{r['rsi']:<10} {r['return']:<10.2f} {r['trades']:<10} {r['win_rate']:<10.1f}")
