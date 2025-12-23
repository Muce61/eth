import re
import pandas as pd
from datetime import datetime
import sys

LOG_FILE = "logs/backtest_3m.log"

def parse_log_roi(file_path):
    # Track open trades: symbol -> entry_price
    open_trades = {}
    data = []
    
    # Regexes
    # [2024-11-06 00:45:00+00:00] OPEN LONG BTCUSDTUSDT @ 71218.2913 ...
    open_pattern = re.compile(r"\[(\d{4}-\d{2}-\d{2}) .*?\] OPEN LONG (\S+) @ ([\d\.]+)")
    
    # [2024-11-06 02:15:00+00:00] CLOSE BTCUSDTUSDT @ 72464.0166 | PnL: ...
    close_pattern = re.compile(r"\[(\d{4}-\d{2}-\d{2}) .*?\] CLOSE (\S+) @ ([\d\.]+)")
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            # Check OPEN
            match_open = open_pattern.search(line)
            if match_open:
                sym = match_open.group(2)
                price = float(match_open.group(3))
                open_trades[sym] = price
                continue
                
            # Check CLOSE
            match_close = close_pattern.search(line)
            if match_close:
                date_str = match_close.group(1)
                sym = match_close.group(2)
                close_price = float(match_close.group(3))
                
                if sym in open_trades:
                    entry_price = open_trades[sym]
                    
                    if entry_price <= 0:
                        continue

                    # Calculate ROI
                    # Assuming 20x Leverage as per logs showing "0.75% move = 15% ROE"
                    leverage = 20 
                    
                    price_change_pct = (close_price - entry_price) / entry_price
                    roi_pct = price_change_pct * leverage * 100
                    
                    data.append({
                        'Date': date_str,
                        'ROI': roi_pct,
                        'Win': 1 if roi_pct > 0 else 0
                    })
                    
                    # FIFO / Clear (Simple assumption: 1 concurrent trade per symbol)
                    # Ideally we wouldn't delete if pyramiding, but logs imply single pos.
                    # del open_trades[sym] 
                    # Actually, dont delete immediately if multiple opens? 
                    # But the log usually shows "OPEN ... CLOSE". 
                    # Let's overwrite on Open, use on Close.
                    # Safety: Keep it simple.
                    pass 

    if not data:
        print("No paired trade data found.")
        return

    df = pd.DataFrame(data)
    
    # Aggregate
    daily = df.groupby('Date').agg({
        'ROI': 'sum',
        'Win': 'count' # Just count trades for now
    }).reset_index()
    
    daily.rename(columns={'Win': 'Trades'}, inplace=True)
    daily['CumROI'] = daily['ROI'].cumsum()
    
    print(f"{'Date':<12} | {'Trades':<6} | {'Daily ROI %':<12} | {'Cum ROI %':<12}")
    print("-" * 50)
    for _, row in daily.iterrows():
        print(f"{row['Date']:<12} | {row['Trades']:<6} | {row['ROI']:>9.1f}% | {row['CumROI']:>9.1f}%")
    print("-" * 50)
    print(f"Total ROI Sum: {daily['ROI'].sum():.1f}%")

if __name__ == "__main__":
    parse_log_roi(LOG_FILE)
