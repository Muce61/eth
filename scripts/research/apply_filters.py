#!/usr/bin/env python3
"""
应用Phase 1和2的研究发现优化策略
"""
import pandas as pd

# 加载白名单和黑名单
with open('data/enriched/coin_whitelist.txt', 'r') as f:
    WHITELIST = [line.strip() for line in f.readlines()]

with open('data/enriched/coin_blacklist.txt', 'r') as f:
    BLACKLIST = [line.strip() for line in f.readlines()]

print("=" * 70)
print("应用研究发现优化策略")
print("=" * 70)

print(f"\n✅ 白名单币种: {len(WHITELIST)}个")
for coin in WHITELIST:
    print(f"  - {coin}")

print(f"\n❌ 黑名单币种: {len(BLACKLIST)}个")
for coin in BLACKLIST:
    print(f"  - {coin}")

# 最佳交易时段
BEST_HOURS = [5, 6, 8, 9, 15]  # UTC时间
WORST_HOURS = [20, 21, 22]

print(f"\n⏰ 允许交易时段 (UTC): {BEST_HOURS}")
print(f"🚫 禁止交易时段 (UTC): {WORST_HOURS}")

# 持仓时长
MIN_HOLDING_MINUTES = 15
MAX_HOLDING_MINUTES = 45

print(f"\n⏳ 最短持仓: {MIN_HOLDING_MINUTES}分钟")
print(f"⏳ 最长持仓: {MAX_HOLDING_MINUTES}分钟")

# 周末过滤
WEEKEND_TRADING = False
print(f"\n📅 周末交易: {'允许' if WEEKEND_TRADING else '禁止'}")

print("\n" + "=" * 70)
print("优化配置已生成")
print("=" * 70)

# 保存配置供策略使用
config = {
    'WHITELIST': WHITELIST,
    'BLACKLIST': BLACKLIST,
    'BEST_HOURS': BEST_HOURS,
    'WORST_HOURS': WORST_HOURS,
    'MIN_HOLDING_MINUTES': MIN_HOLDING_MINUTES,
    'MAX_HOLDING_MINUTES': MAX_HOLDING_MINUTES,
    'WEEKEND_TRADING': WEEKEND_TRADING
}

import json
with open('data/enriched/optimized_config.json', 'w') as f:
    json.dump(config, f, indent=2)

print("✅ 配置已保存至: data/enriched/optimized_config.json")
