
import pandas as pd
from pathlib import Path

def normalize_timestamp(df, col='timestamp'):
    # Detect format. Live log is "2025-12-27 10:00:00". Backtest log is likely similar.
    # Convert to datetime
    df[col] = pd.to_datetime(df[col])
    return df

def main():
    live_log_path = Path("logs/live_signals.csv")
    backtest_log_path = Path("logs/backtest_signals.csv")
    
    if not live_log_path.exists():
        print(f"Error: {live_log_path} not found.")
        return
        
    if not backtest_log_path.exists():
        print(f"Error: {backtest_log_path} not found.")
        return
        
    print("Loading logs...")
    # Live log has no header in the snippet I saw? 
    # Wait, the snippet showed:
    # 1: 2025-12-27 10:00:00,ZECUSDT,QUALITY,PASS,,489.2800,...
    # It MIGHT have a header row if I view line 1, but line 1 was data in the snippet view? 
    # Ah, let's re-verify line 1 of live_signals.csv.
    
    # Assuming standard header: timestamp,symbol,stage,status,reason,price,volume,rsi,adx,vol_ratio,wick_ratio,score
    headers = ['timestamp', 'symbol', 'stage', 'status', 'reason', 'price', 'volume', 'rsi', 'adx', 'vol_ratio', 'wick_ratio', 'score']
    
    # Try reading first line to check if it's header
    with open(live_log_path, 'r') as f:
        first_line = f.readline()
        if 'timestamp' in first_line.lower():
            df_live = pd.read_csv(live_log_path)
        else:
            df_live = pd.read_csv(live_log_path, names=headers)

    # Backtest log should have header as it uses SignalLogger which writes header on init
    df_backtest = pd.read_csv(backtest_log_path)
    
    print(f"Live Rows: {len(df_live)}")
    print(f"Backtest Rows: {len(df_backtest)}")
    
    # Normalize Timestamps
    df_live = normalize_timestamp(df_live)
    df_backtest = normalize_timestamp(df_backtest)
    
    # Filter Backtest to range of Live
    min_live = df_live['timestamp'].min()
    max_live = df_live['timestamp'].max()
    
    print(f"Live Range: {min_live} to {max_live}")
    
    df_backtest = df_backtest[(df_backtest['timestamp'] >= min_live) & (df_backtest['timestamp'] <= max_live)]
    print(f"Backtest Rows (Overlapping): {len(df_backtest)}")
    
    # Merge on Key: Timestamp, Symbol, Stage
    # We want to match decision for decision.
    
    merged = pd.merge(
        df_live, 
        df_backtest, 
        on=['timestamp', 'symbol', 'stage'], 
        how='inner', 
        suffixes=('_live', '_bt')
    )
    
    print(f"Matched Signals: {len(merged)}")
    
    # Compare Status
    merged['status_match'] = merged['status_live'] == merged['status_bt']
    
    # Mismatches
    mismatches = merged[~merged['status_match']]
    
    print(f"\nMatch Rate: {merged['status_match'].mean():.2%}")
    print(f"Total Mismatches: {len(mismatches)}")
    
    if len(mismatches) > 0:
        print("\n--- Top Mismatches ---")
        print(mismatches[['timestamp', 'symbol', 'stage', 'status_live', 'status_bt', 'reason_live', 'reason_bt']].head(20))
        
        # Breakdown by reason
        print("\n--- Mismatch Reason Breakdown ---")
        print(mismatches['reason_live'].value_counts().head(5))
        
    # Validation: Pass Rate
    pass_live = df_live[df_live['status'] == 'PASS']
    pass_bt = df_backtest[df_backtest['status'] == 'PASS']
    
    print(f"\nLive PASS Count: {len(pass_live)}")
    print(f"Backtest PASS Count: {len(pass_bt)}")
    
    # Signal Rate
    sig_live = df_live[df_live['stage'] == 'STRATEGY'][df_live['status'] == 'SIGNAL']
    sig_bt = df_backtest[df_backtest['stage'] == 'STRATEGY'][df_backtest['status'] == 'SIGNAL']
    
    print(f"\nLive SIGNALS: {len(sig_live)}")
    print(f"Backtest SIGNALS: {len(sig_bt)}")
    
    if len(sig_live) > 0 and len(sig_bt) > 0:
         # Check intersection of actual buy signals
         sig_merged = pd.merge(sig_live, sig_bt, on=['timestamp', 'symbol'], how='inner')
         print(f"Common Signals: {len(sig_merged)}")

if __name__ == "__main__":
    main()
