
import asyncio
import os
from binance import AsyncClient
from dotenv import load_dotenv

load_dotenv()

async def debug_futures_api():
    client = await AsyncClient.create()
    try:
        print("--- Testing futures_symbol_ticker ---")
        item1 = await client.futures_symbol_ticker(symbol='BTCUSDT')
        print(f"futures_symbol_ticker keys: {list(item1.keys())}")
        
        print("\n--- Testing futures_ticker (24hr) ---")
        # Try fetching 24hr stats
        # Note: python-binance 'futures_ticker' usually maps to 24hr stats
        item2_list = await client.futures_ticker()
        if item2_list:
            item2 = item2_list[0]
            print(f"futures_ticker keys: {list(item2.keys())}")
            print(f"Sample data: {item2}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close_connection()

if __name__ == "__main__":
    asyncio.run(debug_futures_api())
