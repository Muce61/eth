
import sys
import os
import pandas as pd
from pathlib import Path
from datetime import timedelta

# Add path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(str(Path(__file__).parent.parent))

from run_last_3months_backtest_1m import Last3MonthsBacktestEngine1m

def run_test(days, output_file):
    print(f"\n🚀 Running Consistency Test: Last {days} Days...")
    engine = Last3MonthsBacktestEngine1m(initial_balance=100) # Reset Engine
    
    # Configure similar to main script
    engine.config.TIMEFRAME = '15m'
    engine.config.TOP_GAINER_COUNT = 50
    
    # Run
    # engine.run handles data loading.
    # We rely on 'days' logic in RealBacktestEngine.run
    engine.run(days=days)
    
    # Export Trades
    trades = engine.trades
    print(f"✅ Finished {days}d run. Total Trades: {len(trades)}")
    
    df = pd.DataFrame(trades)
    if not df.empty:
        # Save minimal columns for comparison
        # We need Symbol, Entry Time, Exit Time, PnL
        df.to_csv(output_file, index=False)
        return df
    return pd.DataFrame()

def compare_runs():
    file_30 = "logs/consistency_30d.csv"
    file_15 = "logs/consistency_15d.csv"
    
    df_30 = run_test(30, file_30)
    df_15 = run_test(15, file_15)
    
    if df_15.empty:
        print("⚠️ 15d Run produced no trades. Cannot compare.")
        return

    print("\n⚔️ Comparing Intersection...")
    
    # Filter 30d df to only include trades that started in the range of 15d df
    # 15d range: Start of 15d run -> End
    min_time_15 = pd.to_datetime(df_15['entry_time']).min()
    print(f"15d Run Start Time: {min_time_15}")
    
    # Filter 30d
    df_30['entry_time'] = pd.to_datetime(df_30['entry_time'])
    df_15['entry_time'] = pd.to_datetime(df_15['entry_time'])
    
    df_30_subset = df_30[df_30['entry_time'] >= min_time_15].copy()
    
    print(f"Trades in 30d Run (filtered to last 15d): {len(df_30_subset)}")
    print(f"Trades in 15d Run: {len(df_15)}")
    
    # Comparison
    # Reset index
    df_30_subset.sort_values(by=['symbol', 'entry_time'], inplace=True)
    df_15.sort_values(by=['symbol', 'entry_time'], inplace=True)
    
    # Merge on Symbol + Entry Time
    merged = pd.merge(df_30_subset, df_15, on=['symbol', 'entry_time'], how='outer', suffixes=('_30', '_15'), indicator=True)
    
    perfect_matches = merged[merged['_merge'] == 'both']
    only_30 = merged[merged['_merge'] == 'left_only']
    only_15 = merged[merged['_merge'] == 'right_only']
    
    print(f"\n✅ Perfect Matches: {len(perfect_matches)}")
    print(f"❌ Only in 30d Run: {len(only_30)}")
    print(f"❌ Only in 15d Run: {len(only_15)}")
    
    if not only_30.empty:
         print("Sample Only 30d:", only_30[['symbol', 'entry_time']].head())
    if not only_15.empty:
         print("Sample Only 15d:", only_15[['symbol', 'entry_time']].head())

    # Check PnL Consistency for matches
    if not perfect_matches.empty:
        pnl_diff = (perfect_matches['pnl_30'] - perfect_matches['pnl_15']).abs()
        max_diff = pnl_diff.max()
        print(f"Max PnL Difference in matches: {max_diff:.6f}")
        
    print("\n🏁 Consistency Check Complete.")

if __name__ == "__main__":
    compare_runs()
