import pandas as pd
from datetime import datetime, timedelta

def compare_backtests():
    # Load the datasets
    file_30d = 'backtest_trades copy 4.csv'
    file_7d = 'backtest_trades.csv'
    
    try:
        df_30d = pd.read_csv(file_30d, parse_dates=['entry_time', 'exit_time'])
        df_7d = pd.read_csv(file_7d, parse_dates=['entry_time', 'exit_time'])
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # Determine overlapping period
    # 7-day test starts around Dec 4/5. 30-day starts Nov 11.
    # User request: "Exclude the first day of the warm-up period" for the 7-day test.
    # Let's find the start date of the 7-day test.
    start_7d = df_7d['entry_time'].min()
    
    # Define comparison start time: Start of 7d + 24 hours (warm-up exclusion)
    comparison_start = start_7d + timedelta(days=1)
    
    # Filter both dataframes
    df_30d_filtered = df_30d[df_30d['entry_time'] >= comparison_start].copy()
    df_7d_filtered = df_7d[df_7d['entry_time'] >= comparison_start].copy()
    
    print(f"Comparison Start Time (after 1 day warm-up): {comparison_start}")
    print(f"30-Day File Trades in Range: {len(df_30d_filtered)}")
    print(f"7-Day File Trades in Range: {len(df_7d_filtered)}")
    
    # Sort for detailed comparison
    df_30d_filtered.sort_values(by=['entry_time', 'symbol'], inplace=True)
    df_7d_filtered.sort_values(by=['entry_time', 'symbol'], inplace=True)
    
    # Align and Compare
    # We can merge on entry_time (closest match?) and symbol
    # Exact second matching might fail if there are slight diffs, but let's try exact first.
    
    merged = pd.merge_asof(
        df_7d_filtered.sort_values('entry_time'),
        df_30d_filtered.sort_values('entry_time'),
        on='entry_time',
        by='symbol',
        direction='nearest',
        tolerance=pd.Timedelta('1min'), # Allow 1 min difference
        suffixes=('_7d', '_30d')
    )
    
    # Check consistency
    print("\n--- Detailed Trade Comparison ---")
    consistent_count = 0
    inconsistent_count = 0
    missing_count = 0
    
    for index, row in merged.iterrows():
        symbol = row['symbol']
        time_7d = row['entry_time']
        pnl_7d = row['pnl_7d']
        pnl_30d = row['pnl_30d']
        
        if pd.isna(pnl_30d):
             print(f"[MISSING] {time_7d} {symbol}: Found in 7d but NOT in 30d")
             missing_count += 1
             continue
             
        # Check PnL closeness (allow some variance due to precision/state)
        # 10% tolerance or $5 diff?
        diff = abs(pnl_7d - pnl_30d)
        is_consistent = diff < 5.0 # Strict-ish check
        
        status = "MATCH" if is_consistent else "DIFF"
        if is_consistent:
            consistent_count += 1
        else:
            inconsistent_count += 1
            print(f"[{status}] {time_7d} {symbol}: 7d=${pnl_7d:.2f} vs 30d=${pnl_30d:.2f} (Diff: ${diff:.2f})")
            
    # Check for trades in 30d but not in 7d?
    # (Simplified: just strictly comparing what's in 7d against 30d for now is usually enough to reveal logic diffs)
    
    print("\n--- Summary ---")
    print(f"Total Compared Trades (from 7d source): {len(merged)}")
    print(f"Consistent Matches: {consistent_count}")
    print(f"Inconsistent PnL: {inconsistent_count}")
    print(f"Missing in 30d set: {missing_count}")
    
    if len(df_30d_filtered) != len(df_7d_filtered):
        print(f"\nWARNING: Count mismatch! 30d has {len(df_30d_filtered)} trades, 7d has {len(df_7d_filtered)} trades in the same period.")

if __name__ == "__main__":
    compare_backtests()
