
import pandas as pd
from pathlib import Path

def normalize_reason(reason):
    if not isinstance(reason, str):
        return ""
    # Simplify reasons for comparison
    # Live: "Low Volume ($4.8M < $50M)"
    # Backtest: "Low Volume ($4.8M < $50M)"
    # Strip whitespace
    return reason.strip()

def main():
    live_path = Path("logs/live_signals.csv")
    backtest_path = Path("logs/backtest_signals.csv") # Assuming default output
    
    if not live_path.exists():
        print("❌ Live signals file not found.")
        return
        
    if not backtest_path.exists():
        print("⚠️ Backtest signals file not found (yet).")
        return

    print(f"Loading Live: {live_path}")
    live_df = pd.read_csv(live_path)
    live_df['timestamp'] = pd.to_datetime(live_df['timestamp'])
    # Filter for today/relevant period? 
    # Just take everything for now.
    
    print(f"Loading Backtest: {backtest_path}")
    backtest_df = pd.read_csv(backtest_path)
    backtest_df['timestamp'] = pd.to_datetime(backtest_df['timestamp'])
    
    # Merge on timestamp + symbol
    # Use inner join to find matches
    merged = pd.merge(
        live_df, 
        backtest_df, 
        on=['timestamp', 'symbol'], 
        how='inner', 
        suffixes=('_live', '_back')
    )
    
    print(f"Found {len(merged)} overlapping signals (Time+Symbol).")
    
    mismatches = []
    
    for idx, row in merged.iterrows():
        status_match = row['status_live'] == row['status_back']
        # Reason might differ slightly in formatting, check keyword
        # "Low Volume" vs "Low Volume"
        reason_live = normalize_reason(row['reason_live'])
        reason_back = normalize_reason(row['reason_back'])
        reason_match = reason_live == reason_back
        
        # Numeric Comparisons
        rsi_diff = abs(float(row.get('rsi_live', 0)) - float(row.get('rsi_back', 0)))
        adx_diff = abs(float(row.get('adx_live', 0)) - float(row.get('adx_back', 0)))
        
        if not status_match or not reason_match or rsi_diff > 0.5 or adx_diff > 0.5:
            mismatches.append({
                'timestamp': row['timestamp'],
                'symbol': row['symbol'],
                'live': f"{row['status_live']} ({row['reason_live']})",
                'backtest': f"{row['status_back']} ({row['reason_back']})",
                'metrics_live': f"RSI:{float(row.get('rsi_live',0)):.1f}, ADX:{float(row.get('adx_live',0)):.1f}, Vol:{float(row.get('volume_live',0)):.0f}",
                'metrics_back': f"RSI:{float(row.get('rsi_back',0)):.1f}, ADX:{float(row.get('adx_back',0)):.1f}, Vol:{float(row.get('volume_back',0)):.0f}",
            })
            
    if mismatches:
        print(f"\n❌ Found {len(mismatches)} mismatches (Status/Reason/Metrics):")
        for m in mismatches[:20]:
            print(f"⏰ {m['timestamp']} {m['symbol']}:")
            print(f"   Live: {m['live']} | {m['metrics_live']}")
            print(f"   Back: {m['backtest']} | {m['metrics_back']}")
            print("-" * 50)
    else:
        print("\n✅ All overlapping signals match perfectly (Status, Reason, RSI, ADX)!")
        
    # Check for Missing Signals (In Live but not Backtest, or vice versa)
    # This requires set difference
    live_keys = set(zip(live_df['timestamp'], live_df['symbol']))
    back_keys = set(zip(backtest_df['timestamp'], backtest_df['symbol']))
    
    only_live = live_keys - back_keys
    only_back = back_keys - live_keys
    
    if only_live:
        print(f"\n⚠️ {len(only_live)} signals in Live but NOT in Backtest:")
        for k in list(only_live)[:10]:
            print(f"   {k[0]} {k[1]}")
            
    if only_back:
        print(f"\n⚠️ {len(only_back)} signals in Backtest but NOT in Live:")
        # Filter purely for recent times to focus on relevant diffs
        recent_back = [k for k in only_back if k[0] >= live_df['timestamp'].min()]
        print(f"   (Showing {len(recent_back)} recent ones matching live range)")
        for k in recent_back[:10]:
            print(f"   {k[0]} {k[1]}")

if __name__ == "__main__":
    main()
