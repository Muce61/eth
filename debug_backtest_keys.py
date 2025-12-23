
import sys
import os
from pathlib import Path

# Add project root
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backtest.real_engine import RealBacktestEngine

class DebugEngine(RealBacktestEngine):
    def run_check(self):
        self.load_data()
        print("\n--- CHECKING KEYS ---")
        tnsr_keys = [k for k in self.data_feed.keys() if "TNSR" in k]
        print(f"TNSR Keys: {tnsr_keys}")
        
        usdtusdt_keys = [k for k in self.data_feed.keys() if "USDTUSDT" in k]
        print(f"Total Double Suffix Keys: {len(usdtusdt_keys)}")
        if usdtusdt_keys:
             print(f"Examples: {usdtusdt_keys[:5]}")

data_dir = "/Users/muce/1m_data/new_backtest_data_1year_1m"
engine = DebugEngine(data_dir)
engine.run_check()
