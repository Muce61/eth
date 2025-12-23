
import pandas as pd
import numpy as np
from datetime import timedelta
import os

def load_live_positions(file_path):
    """
    Reconstructs "Positions" from raw trade logs.
    Assumes FIFO or simple "Open to Close" logic.
    Returns DataFrame: [symbol, entry_time, exit_time, pnl, roi, side]
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return pd.DataFrame()

    df = pd.read_csv(file_path)
    df['time'] = pd.to_datetime(df['time'])
    df.sort_values(['symbol', 'time'], inplace=True)
    
    positions = []
    
    # Process per symbol
    for symbol, group in df.groupby('symbol'):
        # Simple Logic: Accumulate Buys until Sell closes it.
        # This works for the bot's "One Position Per Coin" logic.
        
        current_qty = 0
        entry_time = None
        entry_cost = 0 # sum(price * qty)
        total_pnl = 0
        
        for idx, row in group.iterrows():
            qty = row['qty']
            price = row['price']
            pnl = row['realizedPnl']
            side = row['side']
            
            if side == 'BUY':
                if current_qty == 0:
                    entry_time = row['time'] # First Buy
                
                current_qty += qty
                entry_cost += (price * qty)
                
            elif side == 'SELL':
                if current_qty > 0:
                    # Closing
                    # PnL is already in 'realizedPnl'
                    total_pnl += pnl
                    current_qty -= qty
                    
                    if current_qty <= 0.0000001: # Closed (Float tolerance)
                        # Position Cycle Complete
                        avg_entry = entry_cost / (qty + current_qty + 0.0000001) # Approx
                        # Better: Entry Cost was for the FULL amount.
                        # Wait, we need accurate ROI.
                        # ROI = Total Pnl / Total Invested
                        # Invested = The entry cost of the closed quantity.
                        
                        # Simplified: ROI = PnL / (Average Entry Price * Closed Qty / Leverage) -> But we don't know leverage here easily unless fixed.
                        # Let's just use Raw PnL match first.
                        
                        positions.append({
                            'symbol': symbol,
                            'entry_time': entry_time,
                            'exit_time': row['time'],
                            'pnl': total_pnl,
                            'status': 'CLOSED'
                        })
                        
                        # Reset
                        current_qty = 0
                        entry_cost = 0
                        total_pnl = 0
                        entry_time = None
                else:
                    # Short or error (Assume Bot is Long Only)
                    pass
                    
    return pd.DataFrame(positions)

def load_backtest_positions(file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return pd.DataFrame()
        
    df = pd.read_csv(file_path)
    if 'entry_time' not in df.columns:
        return pd.DataFrame()
        
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    return df

def compare_positions():
    live_file = "logs/historical_trades_smart.csv"
    # Backtest file name might vary, check CWD
    backtest_file = "backtest_trades.csv" 
    
    print(f"📂 Loading Live: {live_file}")
    print(f"📂 Loading Backtest: {backtest_file}")
    
    live_pos = load_live_positions(live_file)
    bt_pos = load_backtest_positions(backtest_file)
    
    if live_pos.empty:
        print("⚠️ No Live Positions found (or file missing).")
        return
    if bt_pos.empty:
        print("⚠️ No Backtest Positions found (or file missing).")
        return

    print(f"📊 Live Positions: {len(live_pos)}")
    print(f"📊 Backtest Positions: {len(bt_pos)}")
    
    # Matching Logic
    # We want to find if a Live Position exists in Backtest
    
    matches = []
    live_only = []
    
    # Tolerance for Time Match (e.g., 15 minutes since backtest is 15m candles)
    time_tolerance = timedelta(minutes=16) 
    
    for _, l_row in live_pos.iterrows():
        l_sym = l_row['symbol']
        l_entry = l_row['entry_time']
        
        # Filter candidates
        candidates = bt_pos[bt_pos['symbol'] == l_sym]
        
        match = None
        for _, b_row in candidates.iterrows():
            b_entry = b_row['entry_time']
            # print(f"Checking {l_sym}: Live {l_entry} vs Backtest {b_entry}")
            
            if abs(l_entry - b_entry) <= time_tolerance:
                match = b_row
                break
        
        if match is not None:
            matches.append({
                'symbol': l_sym,
                'live_entry': l_entry,
                'bt_entry': match['entry_time'],
                'live_exit': l_row['exit_time'],
                'bt_exit': match['exit_time'],
                'live_pnl': l_row['pnl'],
                'bt_pnl': match['pnl'],
                'pnl_diff': l_row['pnl'] - match['pnl']
            })
        else:
            live_only.append(l_row)
            
    # Output Report
    results = pd.DataFrame(matches)
    
    print("\n" + "="*60)
    print("MATCHED POSITIONS (Live vs Backtest)")
    print("="*60)
    
    if not results.empty:
        # Format for readability
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print(results[['symbol', 'live_entry', 'bt_entry', 'live_pnl', 'bt_pnl', 'pnl_diff']])
        
        # Stats
        matched_count = len(results)
        print(f"\n✅ Matched Count: {matched_count} / {len(live_pos)} Live Positions")
        
        # Calculate Correlation
        if matched_count > 1:
            corr = results['live_pnl'].corr(results['bt_pnl'])
            print(f"📈 PnL Correlation: {corr:.4f}")
    else:
        print("❌ No matches found!")

    if live_only:
        print("\n" + "="*30)
        print("⚠️ LIVE ONLY (Missing in Backtest)")
        print("="*30)
        print(pd.DataFrame(live_only)[['symbol', 'entry_time', 'pnl']])

if __name__ == "__main__":
    compare_positions()
