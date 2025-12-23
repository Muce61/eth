
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

def plot_equity_curve(csv_file='backtest_trades.csv', output_file='backtest_equity_curve.png'):
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found.")
        return

    # Load trades
    df = pd.read_csv(csv_file)
    if 'exit_time' not in df.columns or 'pnl' not in df.columns:
        print("Error: Required columns 'exit_time' and 'pnl' missing in CSV.")
        return

    df['exit_time'] = pd.to_datetime(df['exit_time'])
    df = df.sort_values('exit_time')

    # Use existing balance_after or calculate it
    initial_balance = 1000.0
    if 'balance_after' in df.columns:
        df['balance'] = df['balance_after']
    else:
        df['balance'] = initial_balance + df['pnl'].cumsum()

    # Calculate Drawdown
    df['peak'] = df['balance'].cummax()
    df['drawdown'] = (df['balance'] - df['peak']) / df['peak'] * 100
    max_dd = df['drawdown'].min()

    # Create Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]}, sharex=True)

    # 1. Equity Curve
    ax1.plot(df['exit_time'], df['balance'], label='Portfolio Balance (USDT)', color='#00aaff', linewidth=2)
    ax1.fill_between(df['exit_time'], df['balance'], initial_balance, color='#00aaff', alpha=0.1)
    ax1.set_title(f"Backtest Equity Curve (Sep 23 - Dec 22)\nFinal Balance: ${df['balance'].iloc[-1]:,.2f}", fontsize=16, fontweight='bold', pad=20)
    ax1.set_ylabel("Balance (USDT)", fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.6)
    ax1.legend(loc='upper left')

    # Add Max Balance annotation
    final_balance = df['balance'].iloc[-1]
    ax1.annotate(f'Final: ${final_balance:,.0f}', 
                 xy=(df['exit_time'].iloc[-1], final_balance),
                 xytext=(df['exit_time'].iloc[-1], final_balance * 1.05),
                 arrowprops=dict(facecolor='black', shrink=0.05, width=1, headwidth=5))

    # 2. Drawdown Plot
    ax2.fill_between(df['exit_time'], df['drawdown'], 0, color='red', alpha=0.3, label='Drawdown %')
    ax2.set_ylabel("Drawdown %", fontsize=12)
    ax2.set_ylim(min(max_dd * 1.2, -10), 2)
    ax2.grid(True, linestyle='--', alpha=0.4)
    ax2.legend(loc='lower left')

    # Formatting X-axis dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"✅ Success: Equity curve saved to {output_file}")
    print(f"📊 Final Balance: ${final_balance:,.2f}")
    print(f"📉 Max Drawdown: {max_dd:.2f}%")

if __name__ == "__main__":
    plot_equity_curve()
