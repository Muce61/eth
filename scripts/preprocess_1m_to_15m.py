"""
Preprocess 1m Data to 15m
==========================
This script reads all 1m CSV files from the source directory,
resamples them to 15m intervals, and saves them to the output directory.

Usage:
    python scripts/preprocess_1m_to_15m.py
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# === CONFIGURATION ===
SOURCE_DIR = Path("E:/ALIXZ/new_backtest_data_1year_1m")
OUTPUT_DIR = Path("e:/dikong/eth/data/15m_preprocessed")

def resample_1m_to_15m(input_path: Path, output_path: Path) -> bool:
    """
    Resample a single 1m CSV file to 15m intervals.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Read CSV with error handling for malformed rows
        try:
            df = pd.read_csv(input_path, on_bad_lines='skip')
        except TypeError:
            # Older pandas version
            df = pd.read_csv(input_path, error_bad_lines=False, warn_bad_lines=False)
        
        # Ensure column names are lowercase
        df.columns = [c.lower() for c in df.columns]
        
        # Find timestamp column (could be 'timestamp', 'time', 'date', etc.)
        ts_col = None
        for col in ['timestamp', 'time', 'date', 'datetime']:
            if col in df.columns:
                ts_col = col
                break
        
        if ts_col is None:
            # Try first column as timestamp
            ts_col = df.columns[0]
        
        # Convert to datetime and set as index
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        df = df.dropna(subset=[ts_col])  # Drop rows with invalid timestamps
        df = df.set_index(ts_col)
        
        # Skip if no valid data
        if len(df) == 0:
            print(f"  Warning: No valid data after timestamp parsing")
            return False
        
        # Resample to 15m using OHLCV aggregation
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        
        df_15m = df.resample('15min').agg(agg_dict).dropna()
        
        # Skip if too few rows after resampling
        if len(df_15m) < 10:
            print(f"  Warning: Only {len(df_15m)} rows after resampling")
            return False
        
        # Normalize timezone to UTC
        if df_15m.index.tz is None:
            df_15m.index = df_15m.index.tz_localize('UTC')
        else:
            df_15m.index = df_15m.index.tz_convert('UTC')
        
        # Save to output
        df_15m.to_csv(output_path)
        
        return True
        
    except Exception as e:
        print(f"  Error: {e}")
        return False


def main():
    print("=" * 60)
    print("1m to 15m Data Preprocessing Script")
    print("=" * 60)
    print(f"Source: {SOURCE_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print()
    
    # Verify source directory exists
    if not SOURCE_DIR.exists():
        print(f"ERROR: Source directory does not exist: {SOURCE_DIR}")
        sys.exit(1)
    
    # Create output directory if needed
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files in source directory
    csv_files = list(SOURCE_DIR.glob("*.csv"))
    total_files = len(csv_files)
    
    if total_files == 0:
        print("ERROR: No CSV files found in source directory")
        sys.exit(1)
    
    print(f"Found {total_files} CSV files to process")
    print("-" * 60)
    
    success_count = 0
    error_count = 0
    
    for i, input_path in enumerate(csv_files, 1):
        symbol = input_path.stem
        output_path = OUTPUT_DIR / f"{symbol}.csv"
        
        # Progress indicator
        if i % 50 == 0 or i == total_files:
            print(f"Processing {i}/{total_files}: {symbol}...")
        
        if resample_1m_to_15m(input_path, output_path):
            success_count += 1
        else:
            error_count += 1
            print(f"  FAILED: {symbol}")
    
    print("-" * 60)
    print(f"Completed!")
    print(f"  Success: {success_count}")
    print(f"  Errors:  {error_count}")
    print(f"  Output:  {OUTPUT_DIR}")
    
    # Show sample of first file
    sample_files = list(OUTPUT_DIR.glob("*.csv"))[:1]
    if sample_files:
        print()
        print("Sample output (first file):")
        df_sample = pd.read_csv(sample_files[0], nrows=5)
        print(df_sample.to_string())


if __name__ == "__main__":
    main()
