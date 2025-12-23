
import pandas as pd
import sys
import os

def generate_daily_summary(csv_file_path):
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at {csv_file_path}")
        return

    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # cleanup whitespace in columns just in case
    df.columns = df.columns.str.strip()

    if 'exit_time' not in df.columns:
        print("Error: 'exit_time' column missing in CSV.")
        return
    
    if 'pnl' not in df.columns:
        print("Error: 'pnl' column missing in CSV.")
        return

    # Convert exit_time to datetime
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    # Localize to UTC if naive, or convert to date
    # Assuming the format in CSV is like "2025-10-26 00:18:17+00:00" which pandas handles well
    
    df['date'] = df['exit_time'].dt.date

    # Setup aggregation
    daily_stats = df.groupby('date').agg(
        total_trades=('pnl', 'count'),
        wins=('pnl', lambda x: (x > 0).sum()),
        losses=('pnl', lambda x: (x <= 0).sum()),
        net_pnl=('pnl', 'sum'),
        end_balance=('balance_after', 'last')
    ).reset_index()

    # Calculate derived metrics
    daily_stats['win_rate'] = (daily_stats['wins'] / daily_stats['total_trades']) * 100
    
    # Calculate Daily ROI
    # We need the start balance for each day.
    # The start balance of a day is the end balance of the previous day.
    # For the very first day, we can infer start balance = end_balance - net_pnl
    
    daily_stats['prev_balance'] = daily_stats['end_balance'].shift(1)
    
    # Fill first day's previous balance
    # First day start balance = First day end balance - First day Net PnL
    first_day_idx = daily_stats.index[0]
    # Ideally find the simplistic start balance: 
    daily_stats.loc[first_day_idx, 'prev_balance'] = daily_stats.loc[first_day_idx, 'end_balance'] - daily_stats.loc[first_day_idx, 'net_pnl']

    daily_stats['daily_roi_pct'] = (daily_stats['net_pnl'] / daily_stats['prev_balance']) * 100

    # formatting
    pd.options.display.float_format = '{:,.2f}'.format
    
    print("\n" + "="*80)
    print(f"Daily Trading Summary for: {csv_file_path}")
    print("="*80)
    
    # Print table
    print(daily_stats[['date', 'total_trades', 'wins', 'losses', 'win_rate', 'net_pnl', 'daily_roi_pct', 'end_balance']].to_string(index=False))
    
    print("\n" + "="*80)
    print("Overall Statistics:")
    total_pnl = daily_stats['net_pnl'].sum()
    total_wins = daily_stats['wins'].sum()
    total_count = daily_stats['total_trades'].sum()
    avg_win_rate = (total_wins / total_count * 100) if total_count else 0
    
    print(f"Total Days:       {len(daily_stats)}")
    print(f"Total Trades:     {total_count}")
    print(f"Overall Win Rate: {avg_win_rate:.2f}%")
    print(f"Total PnL:        {total_pnl:.2f} USDT")
    print(f"Final Balance:    {daily_stats['end_balance'].iloc[-1]:.2f} USDT")
    print("="*80 + "\n")

    # Optional: Save to CSV
    output_path = csv_file_path.replace(".csv", "_daily_summary.csv")
    daily_stats.to_csv(output_path, index=False)
    print(f"Saved daily summary to: {output_path}")

if __name__ == "__main__":
    target_file = "/Users/muce/PycharmProjects/20251223/eth/backtest_trades.csv"
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    
    generate_daily_summary(target_file)
