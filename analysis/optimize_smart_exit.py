
import re
import pandas as pd
import numpy as np

log_file = "logs/backtest_3m.log"
print(f"Parsing {log_file}...")

data = []

# Regex to match CLOSE lines
# [2025-05-02 12:45:00+00:00] CLOSE VINEUSDT @ 0.0560 | PnL: $5993.54 (39.03%) | Reason: Smart Trailing (Max 48%)
pattern = re.compile(r"CLOSE (\w+) @ .*? \| PnL: .*? \(([\d\.-]+)%\) \| Reason: (.*)")

try:
    with open(log_file, 'r') as f:
        for line in f:
            if "CLOSE" in line and "Smart" in line:
                match = pattern.search(line)
                if match:
                    symbol = match.group(1)
                    roe = float(match.group(2))
                    reason = match.group(3)
                    
                    max_roe = 0
                    if "Max" in reason:
                        # Extract Max ROE
                        # Smart Trailing (Max 48%)
                        # Smart Trailing (Max 48.2%)
                        m = re.search(r"Max ([\d\.]+)%", reason)
                        if m:
                            max_roe = float(m.group(1))
                            
                    data.append({
                        'Symbol': symbol,
                        'ROE': roe,
                        'Max_ROE': max_roe,
                        'Reason': reason
                    })

    df = pd.DataFrame(data)
    print(f"Found {len(df)} Smart Exit trades.")

    if not df.empty:
        print("\n--- Smart Exit Statistics ---")
        print(f"Avg Realized ROE: {df['ROE'].mean():.2f}%")
        print(f"Avg Max Potential ROE: {df['Max_ROE'].mean():.2f}%")
        
        df['Retracement'] = df['Max_ROE'] - df['ROE']
        print(f"Avg Retracement (Giveback): {df['Retracement'].mean():.2f}%")
        
        print("\n--- Distribution ---")
        print("Realized ROE Percentiles:")
        print(df['ROE'].quantile([0.25, 0.5, 0.75, 0.9]))
        
        print("\nMax Potential ROE Percentiles:")
        print(df['Max_ROE'].quantile([0.25, 0.5, 0.75, 0.9]))
        
        # Suggestion
        print("\n--- Optimization Suggestion ---")
        # Check scatter of Max vs Realized
        # Best Trailing Callback?
        # If Average Giveback is high, maybe tighten callback?
        if df['Retracement'].mean() > 10:
             print("Giveback is high (>10%). Consider tightening callback (e.g. 5%).")
        else:
             print(f"Giveback is reasonable ({df['Retracement'].mean():.1f}%). Logic is working.")

except Exception as e:
    print(f"Error: {e}")
