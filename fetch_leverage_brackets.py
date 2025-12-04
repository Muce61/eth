#!/usr/bin/env python3
"""
获取所有Binance合约币种的最大杠杆倍数
"""
import ccxt
import pandas as pd
from pathlib import Path

def fetch_leverage_brackets():
    """获取所有合约的杠杆信息"""
    
    # 直接使用API密钥
    api_key = "rK0dL0f0w7KQheT8qv3Bt5OqYk2wA7tsRRpDs41kbHnITADixSwAWYmffEwn9uwu"
    secret = "98A8WFMD3AHlDeEGMSOQUdUkDZZI5NFyKYYomL7QkKls6sqfidACLafYTmQdIgUR"
    
    print("Connecting to Binance Futures...")
    exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': secret,
        'options': {'defaultType': 'future'},
        'enableRateLimit': True
    })

    try:
        print("Fetching leverage brackets from Binance...")
        
        # 使用私有API获取杠杆bracket信息
        response = exchange.fapiPrivateGetLeverageBracket()
        
        data = []
        for item in response:
            symbol = item['symbol']
            
            # 获取最大杠杆（通常是第一个bracket的initialLeverage）
            max_leverage = 0
            if 'brackets' in item and len(item['brackets']) > 0:
                # 第一个bracket通常是最大杠杆
                max_leverage = int(item['brackets'][0]['initialLeverage'])
            
            data.append({
                'symbol': symbol,
                'max_leverage': max_leverage
            })
        
        # 创建DataFrame并排序
        df = pd.DataFrame(data)
        df = df.sort_values('max_leverage', ascending=False)
        
        # 保存到CSV
        output_file = 'leverage_brackets.csv'
        df.to_csv(output_file, index=False)
        
        print(f"\n✅ Successfully saved leverage info for {len(df)} coins to {output_file}")
        print(f"\nTotal coins: {len(df)}")
        print(f"\nLeverage distribution:")
        print(df['max_leverage'].value_counts().sort_index(ascending=False))
        
        print(f"\nTop 20 High Leverage Coins:")
        print(df.head(20).to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching leverage brackets: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    fetch_leverage_brackets()
