
import logging
import os
from datetime import datetime
from pathlib import Path

class LiveTradeLogger:
    """
    Dedicated Logger to mimic Backtest Output Format exactly.
    Writes to logs/live_trading.log
    """
    def __init__(self, log_path="logs/live_trading.log"):
        self.log_path = Path(log_path)
        self._setup_logger()
        
    def _setup_logger(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        # We use a custom file handler that just appends messages
        # No standard logging formatting (date/level) because we want to control the exact string
        pass # We will just use 'open' in append mode for simplicity and thread safety is low concern for this scale
        
    def _write(self, msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.log_path, "a", encoding='utf-8') as f:
                # If message already has timestamp, use it, else prepend?
                # Backtest format often includes timestamp in the message string constructed by f-string
                # But actually, looking at the log: "[2024-11-05 15:00:00+00:00] OPEN LONG ..."
                # We should allow raw writing.
                f.write(f"{msg}\n")
                # Also print to stdout? Maybe not to avoid cluttering main logs
        except Exception as e:
            print(f"FAILED TO WRITE LIVE LOG: {e}")

    def log_open_position(self, symbol, entry_price, stop_loss, quantity, roe_pct="-15%"):
        # Format: [Timestamp] OPEN LONG {symbol} @ {price} | SL: {sl} (-0.75%/-15%ROE) | Size: {qty}
        # Note: Backtest uses UTC usually. Live is usually local or UTC. We should verify.
        # User current time is UTC+8. Backtest log shows "+00:00".
        # We should use UTC to match backtest? Or Keep Local?
        # User context says "Local Time". Backtest says "UTC".
        # Let's use UTC to align with "Backtest Style".
        
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+00:00")
        msg = f"[{now_utc}] OPEN LONG {symbol} @ {entry_price:.4f} | SL: {stop_loss:.4f} (-0.75%/{roe_pct}ROE) | Size: {quantity:.2f}"
        self._write(msg)

    def log_close_position(self, symbol, exit_price, pnl, reason):
        # Format: [Timestamp] CLOSE {symbol} @ {price} | PnL: ${pnl} | Reason: {reason}
        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+00:00")
        msg = f"[{now_utc}] CLOSE {symbol} @ {exit_price:.4f} | PnL: ${pnl:.2f} | Reason: {reason}"
        self._write(msg)
        
    def log_rejection(self, symbol, reason_detail):
         # Format: [Timestamp] {symbol} Rejected: {reason}
         now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+00:00")
         msg = f"[{now_utc}] {symbol} Rejected: {reason_detail}"
         self._write(msg)
         
    def log_position_calc(self, balance, risk_pct, risk_amt, quantity):
        # Format: 仓位计算: 余额={bal}, 风险等级={pct}%, 风险金额=${amt}, 数量={qty}
        # No timestamp prefix usually in backtest for this line
        msg = f"仓位计算: 余额={balance:.2f}, 风险等级={risk_pct*100}%, 风险金额=${risk_amt:.2f}, 数量={quantity:.6f}"
        self._write(msg)
        
    def log_processing(self, timestamp_str, balance):
        # Format: Processing {time}... Balance: ${bal}
        msg = f"Processing {timestamp_str}... Balance: ${balance:.2f}"
        self._write(msg)
