"""
June 2025 Backtest Script
Focus: Evaluate performance during the "crash" month with optimized strategy (Shorts + Stricter Filters).
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backtest.real_engine import RealBacktestEngine
from utils.backtest_metrics import calculate_comprehensive_metrics

def main():
    print("="*80)
    print("📉 June 2025 Crash Test (Optimized Strategy)")
    print("="*80)
    
    # Initialize Engine
    engine = RealBacktestEngine(initial_balance=100)
    
    # Run Backtest for June
    print("Running backtest for 2025-06-01 to 2025-06-30...")
    engine.run(start_date='2025-06-01', end_date='2025-06-30')
    
    # Calculate Metrics
    metrics = calculate_comprehensive_metrics(
        engine.trades, 
        engine.initial_balance, 
        engine.balance
    )
    
    # Save Trades
    trades_df = pd.DataFrame(engine.trades)
    if not trades_df.empty:
        output_path = "backtest_trades_june_optimized.csv"
        trades_df.to_csv(output_path, index=False)
        print(f"\n✅ Trades saved to {output_path}")
        
        # Quick Stats
        longs = trades_df[trades_df['side'] == 'LONG']
        shorts = trades_df[trades_df['side'] == 'SHORT']
        
        print("\nStrategy Breakdown:")
        print(f"Longs: {len(longs)} trades, PnL: ${longs['pnl'].sum() if 'pnl' in longs else 0:.2f}")
        print(f"Shorts: {len(shorts)} trades, PnL: ${shorts['pnl'].sum() if 'pnl' in shorts else 0:.2f}")
    else:
        print("\n⚠️ No trades generated.")

    # Print Summary
    print("\n" + "="*60)
    print("June 2025 Results")
    print("="*60)
    print(f"📊 Total Return: {metrics['total_return_pct']:.2f}%")
    print(f"💰 Final Balance: ${engine.balance:.2f}")
    print(f"📈 Total Trades: {metrics['total_trades']}")
    print(f"✅ Win Rate: {metrics['win_rate']:.2f}%")
    print(f"📉 Max Drawdown: {metrics['max_drawdown']:.2f}%")
    print("="*60)

if __name__ == "__main__":
    main()
