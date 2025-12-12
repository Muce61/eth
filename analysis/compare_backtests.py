import pandas as pd
import matplotlib.pyplot as plt
import os

def analyze_csv(path, label):
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None, {}
        
        # Ensure dates
        df['exit_time'] = pd.to_datetime(df['exit_time'])
        df = df.sort_values('exit_time')
        
        # Stats
        initial_bal = 1000 # hardcoded base
        final_bal = df['balance_after'].iloc[-1]
        total_ret = (final_bal - initial_bal) / initial_bal * 100
        trade_count = len(df)
        win_count = len(df[df['pnl'] > 0])
        loss_count = len(df[df['pnl'] <= 0])
        win_rate = win_count / trade_count * 100 if trade_count > 0 else 0
        avg_pnl = df['pnl'].mean()
        
        stats = {
            'Label': label,
            'Total Return (%)': total_ret,
            'Final Balance ($)': final_bal,
            'Trade Count': trade_count,
            'Win Rate (%)': win_rate,
            'Avg PnL ($)': avg_pnl
        }
        
        return df, stats
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None, {}

def main():
    path_1m = "backtest_trades_1m_3months.csv"
    path_15m = "backtest_trades_15m_3months.csv"
    
    df_1m, stats_1m = analyze_csv(path_1m, "1m Logic")
    df_15m, stats_15m = analyze_csv(path_15m, "15m Logic")
    
    print("=== Comparative Analysis Report ===")
    
    # Print Stats Table
    all_stats = [stats_1m, stats_15m]
    res_df = pd.DataFrame(all_stats)
    print(res_df.to_string(index=False))
    
    # Plotting
    plt.figure(figsize=(14, 7))
    
    if df_1m is not None:
        # Reconstruct Equity Curve
        # Assuming balance_after is accurate. 
        # We need to prepend Start Balance for a nicer chart, but usually balance_after is fine.
        plt.plot(df_1m['exit_time'], df_1m['balance_after'], label='1m Logic', color='blue', alpha=0.7)
        
    if df_15m is not None:
        plt.plot(df_15m['exit_time'], df_15m['balance_after'], label='15m Logic', color='red', alpha=0.7)
        
    plt.title('Equity Curve Comparison: 1m vs 15m Logic (3 Months)')
    plt.xlabel('Date')
    plt.ylabel('Balance (USDT)')
    plt.legend()
    plt.grid(True)
    
    out_path = "analysis/charts/comparison_1m_vs_15m.png"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path)
    print(f"\nChart saved to {out_path}")

if __name__ == "__main__":
    main()
