
import sys
import os
import csv
from datetime import datetime
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.real_engine import RealBacktestEngine
from config.settings import Config

class VerificationEngine(RealBacktestEngine):
    """
    Extended Backtest Engine that outputs trades in Live Bot format.
    """
    def __init__(self, data_dir, start_date, end_date):
        super().__init__(data_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.csv_file = Path("logs/backtest_history.csv")
        self.setup_csv()
        
    def setup_csv(self):
        """Initialize the CSV file with headers matching Live Bot"""
        self.csv_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            # Live Bot Format: Symbol,Open Time,Close Time,Investment,Final Amount,Leverage,Status,Entry Price,Exit Price,Qty
            writer.writerow(['Symbol', 'Open Time', 'Close Time', 'Investment', 'Final Amount', 'Leverage', 'Status', 'Entry Price', 'Exit Price', 'Qty'])

    def _log_trade_csv(self, symbol, pos, exit_price, pnl, status="Closed"):
        """Log trade to CSV in strict Live Bot format"""
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                
                # Format Data
                open_time = pos['entry_time'].strftime("%Y-%m-%d %H:%M:%S")
                # Backtest assumes exit at current timestamp of the engine
                # We need to pass the exit time. In _close_position we have it.
                # But _close_position in real_engine doesn't call this method.
                # We will override _close_position.
                
                # In real_engine._close_position, we don't readily have 'current_time' passed easily 
                # unless we grab it from the loop context or modify _close_position signature.
                # Actually _close_position signature is: _close_position(self, symbol, exit_price, timestamp, reason)
                # So we have timestamp!
                
                close_time = self.current_exit_time.strftime("%Y-%m-%d %H:%M:%S")
                
                investment = (pos['entry_price'] * pos['quantity']) / pos['leverage']
                final_amount = investment + pnl # Approx margin + pnl
                
                # Status mapping
                # Live Bot uses: "普通止损", "移动止盈", "保本止损", "信号平仓"
                # Backtest reasons: 'Stop Loss', 'Trailing Stop', etc.
                status_map = {
                    'Stop Loss': '普通止损',
                    'Stop Loss (1s Trigger)': '普通止损', 
                    'Stop Loss (1m Fallback)': '普通止损',
                    'Trailing Stop': '移动止盈',
                    'Smart Exit': '移动止盈',
                    'Signal Reversal': '信号平仓'
                }
                status_cn = status_map.get(status, status)
                
                writer.writerow([
                    symbol,
                    open_time,
                    close_time,
                    f"{investment:.2f}",
                    f"{final_amount:.2f}",
                    pos['leverage'],
                    status_cn,
                    f"{pos['entry_price']:.6f}",
                    f"{exit_price:.6f}",
                    f"{pos['quantity']:.6f}"
                ])
        except Exception as e:
            print(f"Error logging to CSV: {e}")

    def _close_position(self, symbol, exit_price, timestamp, reason):
        """Override to hook into CSV logging"""
        # We need to capture the position data BEFORE it is deleted by super()._close_position
        # But super() method does the PnL calculation and deletion.
        # So we should copy the data first.
        
        pos = None
        if symbol in self.positions:
            pos = self.positions[symbol]
        elif symbol in self.paper_positions:
            pos = self.paper_positions[symbol]
            
        if pos:
            # Save for logging
            self.current_exit_time = timestamp
            quantity = pos['quantity']
            leverage = pos['leverage']
            entry_price = pos['entry_price']
            
            # Calculate PnL locally for logging (super will do it again but that's fine)
            slippage = 0.0005
            exit_price_with_slippage = exit_price * (1 - slippage)
            pnl = (exit_price_with_slippage - entry_price) * quantity
            
            # Log it
            self._log_trade_csv(symbol, pos, exit_price_with_slippage, pnl, reason)
            
        # Call original method to handle state cleanup
        super()._close_position(symbol, exit_price, timestamp, reason)


def run_verification():
    # CONFIGURATION
    # NEED TO MATCH LIVE BOT EXACTLY
    
    # User said: "12月24日18点00分 - 12月24日23点59分" in previous context as example.
    # But let's run for the FULL DAY of Dec 24 to cover everything, or make it configurable.
    # Let's hardcode Dec 24 for now as requested in task.md
    
    start_date = "2025-11-23 00:00:00"
    end_date = "2025-11-23 23:59:59"
    
    data_dir = Config.DATA_DIR
    # Ensure correct data dir
    # From previous context: /Users/muce/1m_data/new_backtest_data_1year_1m
    # Config.DATA_DIR might be "data/storage". 
    # Check settings.py?
    # Actually, let's hardcode the data path known to work from "run_last_3months_backtest_1m.py"
    data_dir = "/Users/muce/1m_data/new_backtest_data_1year_1m"
    
    print(f"🧪 Running Verification Backtest: {start_date} -> {end_date}")
    print(f"📂 Data Dir: {data_dir}")
    
    engine = VerificationEngine(data_dir, start_date, end_date)
    engine.run(start_date, end_date)
    
    print(f"\n✅ Verification Run Complete. Output: logs/backtest_history.csv")

if __name__ == "__main__":
    run_verification()
