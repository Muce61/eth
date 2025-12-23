
import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_monthly_stats(csv_file='backtest_trades.csv', output_img='backtest_monthly_stats.png'):
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found.")
        return

    # Load trades
    df = pd.read_csv(csv_file)
    df['exit_time'] = pd.to_datetime(df['exit_time'])
    df['month'] = df['exit_time'].dt.to_period('M')

    # Monthly Stats Calculation
    stats = []
    
    # Track beginning balance
    initial_balance = 1000.0
    current_balance = initial_balance

    for month, group in df.groupby('month'):
        total_trades = len(group)
        winning_trades = len(group[group['pnl'] > 0])
        losing_trades = len(group[group['pnl'] <= 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        monthly_pnl = group['pnl'].sum()
        start_bal = current_balance
        end_bal = start_bal + monthly_pnl
        monthly_return_pct = (monthly_pnl / start_bal * 100) if start_bal > 0 else 0
        
        stats.append({
            'Month': str(month),
            'Trades': total_trades,
            'Wins': winning_trades,
            'Losses': losing_trades,
            'WinRate': f"{win_rate:.1f}%",
            'Profit': f"${monthly_pnl:,.2f}",
            'Return': f"{monthly_return_pct:,.1f}%"
        })
        
        current_balance = end_bal

    # Convert to DataFrame for display
    stats_df = pd.DataFrame(stats)
    print("\n" + "="*80)
    print("MONTHLY BACKTEST SUMMARY")
    print("="*80)
    print(stats_df.to_string(index=False))
    print("="*80)

    # Plotting Monthly Returns (Bar Chart)
    plt.figure(figsize=(12, 6))
    
    # Extract numerical return for plotting
    plot_data = []
    for s in stats:
        pct_str = s['Return'].replace(',', '').replace('%', '')
        plot_data.append(float(pct_str))
    
    months = [s['Month'] for s in stats]
    colors = ['green' if x >= 0 else 'red' for x in plot_data]
    
    # Use log scale for Y if returns are extreme
    plt.bar(months, plot_data, color=colors, alpha=0.7)
    
    # If returns are astronomical, we might need log scale or just Cap for visualization
    # But for now let's try linear.
    plt.title("Monthly Return %", fontsize=14, fontweight='bold')
    plt.ylabel("Return %", fontsize=12)
    plt.grid(True, axis='y', linestyle='--', alpha=0.6)
    plt.xticks(rotation=45)
    
    # Add labels on bars
    for i, v in enumerate(plot_data):
        label = f"{v:,.0f}%" if abs(v) > 100 else f"{v:.1f}%"
        plt.text(i, v, label, ha='center', va='bottom' if v >= 0 else 'top', fontsize=9)

    plt.tight_layout()
    plt.savefig(output_img, dpi=300)
    print(f"\n✅ Monthly stats chart saved to {output_img}")

if __name__ == "__main__":
    generate_monthly_stats()
