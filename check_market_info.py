import ccxt
import json

def check_market_structure():
    exchange = ccxt.binance({'options': {'defaultType': 'future'}})
    markets = exchange.load_markets()
    
    # Check BTC/USDT structure
    btc = markets.get('BTC/USDT:USDT')
    if btc:
        print("BTC/USDT market info keys:", btc.keys())
        if 'limits' in btc:
            print("Limits:", btc['limits'])
        if 'info' in btc:
            # Check raw info
            print("Raw info keys:", btc['info'].keys())
            # Check if leverage is in raw info
            # Usually 'pair' in info
            
    # Check if we can deduce max leverage
    # Sometimes it's not explicitly there in public info

if __name__ == "__main__":
    check_market_structure()
