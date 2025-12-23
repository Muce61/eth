import re
import pandas as pd
from datetime import datetime
import sys

# Log File Path
LOG_FILE = "logs/backtest_3m.log"

def parse_log(file_path):
    data = []
    
    # Regex for identifying Close lines
    # Example: [2024-11-05 16:30:00+00:00] CLOSE BTCUSDTUSDT @ 70122.3384 | PnL: $0.01 | Reason: Smart Break-even
    # Regex needs to handle the timezone offset mostly by ignoring it or parsing generically
    pattern = re.compile(r"\[(\d{4}-\d{2}-\d{2}) .*?\] CLOSE .*? PnL: \$([-]?\d+\.\d+)")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    date_str = match.group(1)
                    pnl_str = match.group(2)
                    
                    try:
                        pnl = float(pnl_str)
                        data.append({
                            'Date': date_str,
                            'PnL': pnl,
                            'Win': 1 if pnl > 0 else 0,
                            'Loss': 1 if pnl <= 0 else 0 
                        })
                    except ValueError:
                        continue
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return

    if not data:
        print("No trade data found in log.")
        return

    df = pd.DataFrame(data)
    
    # Aggregate by Date
    daily_stats = df.groupby('Date').agg({
        'PnL': 'sum',
        'Win': 'sum',
        'Loss': 'sum'
    }).reset_index()
    
    # Calculate derived metrics
    daily_stats['Trades'] = daily_stats['Win'] + daily_stats['Loss']
    # Win Rate
    daily_stats['WinRate'] = (daily_stats['Win'] / daily_stats['Trades'] * 100).round(1)
    # Cumulative PnL
    daily_stats['CumPnL'] = daily_stats['PnL'].cumsum()
    
    # Format for output
    print(f"{'Date':<12} | {'Trades':<6} | {'Win':<4} | {'Loss':<5} | {'WinRate%':<8} | {'Daily PnL':<10} | {'Total PnL':<10}")
    print("-" * 80)
    
    for _, row in daily_stats.iterrows():
        print(f"{row['Date']:<12} | {row['Trades']:<6} | {row['Win']:<4} | {row['Loss']:<5} | {row['WinRate']:<8} | ${row['PnL']:<9.2f} | ${row['CumPnL']:<9.2f}")
        
    print("-" * 80)
    print(f"Total Trades: {daily_stats['Trades'].sum()}")
    print(f"Total PnL:    ${daily_stats['PnL'].sum():.2f}")
    if daily_stats['Trades'].sum() > 0:
        overall_wr = (daily_stats['Win'].sum() / daily_stats['Trades'].sum() * 100)
        print(f"Overall Win Rate: {overall_wr:.1f}%")

if __name__ == "__main__":
    parse_log(LOG_FILE)
