import ccxt
import json

def check_public_exchange_info():
    exchange = ccxt.binance({'options': {'defaultType': 'future'}})
    try:
        # fapiPublic_get_exchangeInfo
        info = exchange.fapiPublic_get_exchangeInfo()
        # Check first symbol
        if 'symbols' in info:
            first_sym = info['symbols'][0]
            print("First symbol keys:", first_sym.keys())
            # Check for leverage related keys
            print("First symbol:", json.dumps(first_sym, indent=2))
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    check_public_exchange_info()
