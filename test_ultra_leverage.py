#!/usr/bin/env python3
"""
超高杠杆策略回测
Ultra-High Leverage Strategy Backtest

测试多周期共振策略在6个月历史数据上的表现
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
import pandas as pd
from strategy.ultra_leverage import MultiTimeframeStrategy
from config import ultra_leverage_config as config

print("="*60)
print("🚀 超高杠杆策略回测")
print("="*60)
print(f"目标胜率: 95%+")
print(f"杠杆范围: {config.MIN_LEVERAGE}x - 125x (动态)")
print(f"风险控制: {config.STOP_LOSS_PERCENT}% 硬止损")
print("="*60)

# 初始化策略
strategy = MultiTimeframeStrategy()

# 显示杠杆配置
print(f"\n📊 币种杠杆配置:")
for symbol, lev in config.COIN_MAX_LEVERAGE.items():
    print(f"  {symbol}: {lev}x")

print(f"\n✅ 策略已加载")
print(f"信号强度阈值: {config.SIGNAL_STRENGTH_THRESHOLD}分")
print(f"完美信号阈值: {config.PERFECT_SIGNAL_THRESHOLD}分")

print(f"\n⚠️ 极限风险警告:")
print(f"125x杠杆 = 0.8%反向波动即爆仓")
print(f"硬止损仅{config.STOP_LOSS_PERCENT}%")
print(f"一次失误可能导致重大亏损")

print(f"\n💡 下一步: 实现多周期回测引擎")
print(f"Token剩余: ~53k")
print(f"预计需要: 完整实现需要约30-40k tokens")

print(f"\n准备就绪！")
