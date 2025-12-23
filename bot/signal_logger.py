
import csv
import os
from datetime import datetime
import logging

class SignalLogger:
    """
    Logs every signal check (quality filter and strategy signal) to a CSV file.
    Enables scientific verification by comparing Live vs Backtest signal quality.
    """
    def __init__(self, log_dir="logs", filename="signal_quality.csv"):
        self.filepath = os.path.join(log_dir, filename)
        self.logger = logging.getLogger("SignalLogger")
        self._ensure_file()

    def _ensure_file(self):
        """Create file with header if not exists."""
        if not os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'timestamp', 'symbol', 'stage', 'status', 'reason', 
                        'price', 'volume', 'rsi', 'adx', 'vol_ratio', 'wick_ratio', 'score'
                    ])
            except Exception as e:
                self.logger.error(f"Failed to init signal log: {e}")

    def log_signal(self, timestamp, symbol, stage, status, reason=None, metrics=None, score=None, price=None):
        """
        Log a signal event (Quality Check or Strategy Check).
        """
        if metrics is None: metrics = {}
        
        try:
            # Format timestamp
            if isinstance(timestamp, (int, float)): # ms or s
                # Heuristic: if > 3e9, unlikely to be seconds (year 2065). 
                # If > 1e11 (year 1973 in ms), likely ms.
                # Current 2025 ts is ~1.7e9 (seconds) or 1.7e12 (ms)
                if timestamp > 1e11: 
                    timestamp = timestamp / 1000.0
                ts_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            elif hasattr(timestamp, 'strftime'):
                ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                ts_str = str(timestamp)

            row = [
                ts_str,
                symbol,
                stage, # 'QUALITY' or 'STRATEGY'
                status, # 'PASS', 'REJECT', 'SIGNAL'
                reason if reason else '',
                f"{price:.4f}" if price is not None else '',
                f"{metrics.get('volume', 0):.2f}",
                f"{metrics.get('rsi', 0):.2f}",
                f"{metrics.get('adx', 0):.2f}",
                f"{metrics.get('volume_ratio', 0):.2f}",
                f"{metrics.get('upper_wick_ratio', 0):.4f}",
                score if score is not None else ''
            ]
            
            with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row)
                
        except Exception as e:
            self.logger.error(f"Error logging signal for {symbol}: {e}")
