#!/usr/bin/env python3
"""
检查ccxt支持的Binance API方法
"""
import ccxt

exchange = ccxt.binance({
    'apiKey': 'test',
    'secret': 'test',
    'options': {'defaultType': 'future'}
})

# 列出所有API方法
methods = [m for m in dir(exchange) if 'leverage' in m.lower()]
print("Available leverage-related methods:")
for m in methods:
    print(f"  - {m}")

# 检查fapi相关方法
print("\nAll fapi methods:")
fapi_methods = [m for m in dir(exchange) if m.startswith('fapi')]
for m in fapi_methods[:20]:  # 只显示前20个
    print(f"  - {m}")
