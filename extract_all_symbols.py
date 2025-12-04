#!/usr/bin/env python3
"""
从leverage_brackets.csv读取所有币种并生成Python列表
"""
import pandas as pd

df = pd.read_csv('/Users/muce/PycharmProjects/github/eth/leverage_brackets.csv')
symbols = df['symbol'].tolist()

print(f"# 从leverage_brackets.csv提取的全部{len(symbols)}个币种")
print(f"ALL_SYMBOLS = {symbols}")
print(f"\n# 总计: {len(symbols)}个币种")
