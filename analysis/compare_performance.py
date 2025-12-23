
import pandas as pd
from datetime import timedelta
import os

def load_trades(file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        # Standardize Columns
        # Live: Symbol,Open Time,Close Time,Investment,Final Amount,Leverage,Status,Entry Price,Exit Price,Qty
        # Clean timestamps
        df['Open Time'] = pd.to_datetime(df['Open Time'])
        df['Close Time'] = pd.to_datetime(df['Close Time'])
        return df
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return pd.DataFrame()

def compare_performance():
    live_path = "trades/history.csv"
    backtest_path = "logs/backtest_history.csv"
    
    print(f"📊 Loading Live Data: {live_path}")
    live_df = load_trades(live_path)
    
    print(f"📊 Loading Backtest Data: {backtest_path}")
    bt_df = load_trades(backtest_path)
    
    if live_df.empty or bt_df.empty:
        print("⚠️ One or both datasets are empty. Comparison aborted.")
        return

    # Filter Live Data to match Backtest Range (approx)
    if not bt_df.empty:
        min_time = bt_df['Open Time'].min() - timedelta(minutes=60)
        max_time = bt_df['Open Time'].max() + timedelta(minutes=60)
        live_df = live_df[(live_df['Open Time'] >= min_time) & (live_df['Open Time'] <= max_time)]

    print(f"\n🔍 Comparing {len(live_df)} Live Trades vs {len(bt_df)} Backtest Trades...")
    
    matches = []
    unmatched_live = []
    unmatched_bt = bt_df.copy()
    
    for idx, live_trade in live_df.iterrows():
        # Find match in Backtest
        # Criteria: Same Symbol, Open Time within 15 mins
        symbol = live_trade['Symbol']
        open_time = live_trade['Open Time']
        
        candidates = unmatched_bt[
            (unmatched_bt['Symbol'] == symbol) & 
            (abs(unmatched_bt['Open Time'] - open_time) <= timedelta(minutes=15))
        ]
        
        if not candidates.empty:
            # Pick best match (closest time)
            best_match = candidates.iloc[0] # Simplification
            
            # Remove from unmatched
            unmatched_bt = unmatched_bt.drop(best_match.name)
            
            matches.append({
                'Symbol': symbol,
                'Live_Time': open_time,
                'BT_Time': best_match['Open Time'],
                'Live_Entry': float(live_trade['Entry Price']),
                'BT_Entry': float(best_match['Entry Price']),
                'Live_Exit': float(live_trade['Exit Price']),
                'BT_Exit': float(best_match['Exit Price']),
                'Live_PnL': float(live_trade['Final Amount']) - float(live_trade['Investment']),
                'BT_PnL': float(best_match['Final Amount']) - float(best_match['Investment'])
            })
        else:
            unmatched_live.append(live_trade)
            
    # --- REPORT GENERATION ---
    print("\n" + "="*50)
    print("🔬 COMPARISON REPORT (MIRROR TEST)")
    print("="*50)
    
    total_live = len(live_df)
    match_count = len(matches)
    match_rate = (match_count / total_live * 100) if total_live > 0 else 0
    
    print(f"✅ Match Rate: {match_rate:.1f}% ({match_count}/{total_live})")
    print(f"⚠️ Unmatched Live: {len(unmatched_live)}")
    print(f"⚠️ Unmatched Backtest: {len(unmatched_bt)}")
    
    if matches:
        matches_df = pd.DataFrame(matches)
        
        # Slippage Analysis
        matches_df['Entry_Slippage_Pct'] = (matches_df['Live_Entry'] - matches_df['BT_Entry']) / matches_df['BT_Entry'] * 100
        matches_df['Exit_Slippage_Pct'] = (matches_df['Live_Exit'] - matches_df['BT_Exit']) / matches_df['BT_Exit'] * 100 # Note: Direction matters for PnL
        
        avg_entry_slip = matches_df['Entry_Slippage_Pct'].abs().mean()
        avg_exit_slip = matches_df['Exit_Slippage_Pct'].abs().mean()
        
        print(f"\n📉 Slippage Analysis:")
        print(f"   Avg Entry Deviation: {avg_entry_slip:.4f}%")
        print(f"   Avg Exit Deviation:  {avg_exit_slip:.4f}%")
        
        # PnL Analysis
        live_total_pnl = matches_df['Live_PnL'].sum()
        bt_total_pnl = matches_df['BT_PnL'].sum()
        pnl_diff = live_total_pnl - bt_total_pnl
        
        print(f"\n💰 PnL Comparison (Matched Trades Only):")
        print(f"   Live PnL: ${live_total_pnl:.2f}")
        print(f"   Backtest: ${bt_total_pnl:.2f}")
        print(f"   Diff:     ${pnl_diff:.2f}")
        
        # Export Matches
        matches_df.to_csv("logs/verification_report.csv", index=False)
        print(f"\n📄 Detailed Report Saved: logs/verification_report.csv")
    else:
        print("\n❌ No matches found to analyze.")

if __name__ == "__main__":
    compare_performance()
