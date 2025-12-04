#!/usr/bin/env python3
"""
参数扫描优化脚本 - 自动测试不同参数组合
目标: 找到最优参数配置，将六月收益从+2.73%提升至>+10%
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
from backtest.real_engine import RealBacktestEngine
from datetime import datetime
import pandas as pd
import json

class ParameterOptimizer:
    def __init__(self):
        self.results = []
        self.baseline = None
        
    def test_configuration(self, config_name, **params):
        """测试单个参数配置"""
        print(f"\n{'='*60}")
        print(f"测试配置: {config_name}")
        print(f"参数: {params}")
        print(f"{'='*60}")
        
        # 临时修改策略参数
        from strategy import momentum
        original_values = {}
        
        for param, value in params.items():
            if hasattr(momentum.MomentumStrategy, param):
                original_values[param] = getattr(momentum.MomentumStrategy, param)
                setattr(momentum.MomentumStrategy, param, value)
        
        # 运行回测
        engine = RealBacktestEngine()
        engine.load_data()
        
        # 六月回测
        start = datetime(2025, 6, 1)
        end = datetime(2025, 6, 30)
        engine.run(start, end, days=30)
        
        # 收集结果
        result = {
            'config_name': config_name,
            'params': params,
            'total_return': ((engine.balance - engine.initial_balance) / engine.initial_balance) * 100,
            'total_trades': len(engine.trades),
            'win_rate': len([t for t in engine.trades if t['pnl'] > 0]) / len(engine.trades) * 100 if engine.trades else 0,
            'avg_win': sum([t['pnl'] for t in engine.trades if t['pnl'] > 0]) / len([t for t in engine.trades if t['pnl'] > 0]) if [t for t in engine.trades if t['pnl'] > 0] else 0,
            'avg_loss': sum([t['pnl'] for t in engine.trades if t['pnl'] <= 0]) / len([t for t in engine.trades if t['pnl'] <= 0]) if [t for t in engine.trades if t['pnl'] <= 0] else 0,
            'max_dd': engine._calculate_max_drawdown(),
            'final_balance': engine.balance
        }
        
        self.results.append(result)
        
        # 恢复原始参数
        for param, value in original_values.items():
            setattr(momentum.MomentumStrategy, param, value)
        
        print(f"\n📊 结果:")
        print(f"  收益率: {result['total_return']:.2f}%")
        print(f"  总交易: {result['total_trades']}")
        print(f"  胜率: {result['win_rate']:.1f}%")
        print(f"  平均盈利: ${result['avg_win']:.2f}")
        print(f"  平均亏损: ${result['avg_loss']:.2f}")
        print(f"  最大回撤: {result['max_dd']:.2f}%")
        
        return result
    
    def optimize_rsi_threshold(self):
        """优化RSI阈值"""
        print("\n" + "="*60)
        print("🎯 Phase 2.1: RSI阈值优化")
        print("="*60)
        
        # 基准测试
        self.baseline = self.test_configuration("Baseline (RSI>55)", rsi_threshold_long=55)
        
        # 扫描不同RSI值
        for rsi in [57, 60, 62]:
            self.test_configuration(f"RSI>{rsi}", rsi_threshold_long=rsi)
    
    def optimize_volume_ratio(self):
        """优化Volume Ratio"""
        print("\n" + "="*60)
        print("🎯 Phase 2.2: Volume Ratio优化")
        print("="*60)
        
        for vol in [3.0, 3.3, 3.5, 4.0]:
            self.test_configuration(f"Vol>{vol}", min_volume_ratio=vol)
    
    def optimize_adx_range(self):
        """优化ADX范围"""
        print("\n" + "="*60)
        print("🎯 Phase 2.3: ADX范围优化")
        print("="*60)
        
        for adx_min in [25, 28, 30, 32]:
            self.test_configuration(f"ADX {adx_min}-60", adx_min=adx_min, adx_max=60)
    
    def generate_report(self):
        """生成优化报告"""
        df = pd.DataFrame(self.results)
        df = df.sort_values('total_return', ascending=False)
        
        print("\n" + "="*60)
        print("📈 优化结果总结")
        print("="*60)
        
        print(f"\n🏆 最佳配置:")
        best = df.iloc[0]
        print(f"  配置: {best['config_name']}")
        print(f"  收益率: {best['total_return']:.2f}% (基准: {self.baseline['total_return']:.2f}%)")
        print(f"  提升: {best['total_return'] - self.baseline['total_return']:.2f}%")
        print(f"  胜率: {best['win_rate']:.1f}%")
        print(f"  参数: {best['params']}")
        
        print(f"\n📊 Top 5配置:")
        print(df[['config_name', 'total_return', 'win_rate', 'total_trades']].head())
        
        # 保存结果
        df.to_csv('optimization_results.csv', index=False)
        print(f"\n✅ 详细结果已保存至: optimization_results.csv")
        
        return df

if __name__ == "__main__":
    optimizer = ParameterOptimizer()
    
    # Phase 2: 单参数扫描
    optimizer.optimize_rsi_threshold()
    optimizer.optimize_volume_ratio()
    optimizer.optimize_adx_range()
    
    # 生成报告
    results_df = optimizer.generate_report()
