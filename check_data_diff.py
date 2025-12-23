
import os
from pathlib import Path

def get_symbols(directory):
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"Error: Directory not found: {directory}")
        return set()
    files = list(dir_path.glob("*.csv"))
    if not files:
        print(f"Warning: No .csv files found in {directory}")
        try:
            all_files = os.listdir(directory)
            # Filter distinct types
            valid_dirs = [d for d in all_files if os.path.isdir(os.path.join(directory, d))]
            print(f"Directory contains {len(valid_dirs)} subdirectories.")
            
            if valid_dirs:
                first_dir = valid_dirs[0]
                sample_path = os.path.join(directory, first_dir)
                print(f"Inspecting sample subdir: {first_dir}")
                sample_files = os.listdir(sample_path)
                print(f"Contents (first 5): {sample_files[:5]}")
                # Print a few sorted to see date pattern
                sorted_files = sorted(sample_files)
                print(f"Sorted sample: {sorted_files[:5]}")
                
            # Extract symbols from folder names
            # Logic: Remove '_1s_agg' or '_1s_mark' suffix
            symbols = set()
            for name in valid_dirs:
                clean_name = name.replace('_1s_agg', '').replace('_1s_mark', '')
                symbols.add(clean_name)
            return symbols
            
        except Exception as e:
            print(f"Error listing directory: {e}")
        return set()
    return {f.stem for f in files}

def main():
    dir_1m = "/Users/muce/1m_data/new_backtest_data_1year_1m"
    dir_1s = "/Users/muce/1m_data/klines_data_usdm_1s_agg"
    
    print(f"Checking 1m data in: {dir_1m}")
    print(f"Checking 1s data in: {dir_1s}")
    
    symbols_1m = {s.replace('USDTUSDT', 'USDT') for s in get_symbols(dir_1m)}
    symbols_1s = get_symbols(dir_1s)
    
    print(f"Found {len(symbols_1m)} unique symbols in 1m data (Normalized).")
    print(f"Found {len(symbols_1s)} unique symbols in 1s data.")
    
    common = symbols_1m.intersection(symbols_1s)
    missing_in_1s = symbols_1m - symbols_1s
    missing_in_1m = symbols_1s - symbols_1m
    
    print(f"Common symbols: {len(common)}")
    
    if missing_in_1s:
        print(f"\n[WARNING] {len(missing_in_1s)} symbols found in 1m but MISSING in 1s:")
        print(", ".join(sorted(list(missing_in_1s))[:50]))
        if len(missing_in_1s) > 50:
            print("... and more.")

    if missing_in_1m:
        print(f"\n[INFO] {len(missing_in_1m)} symbols found in 1s but MISSING in 1m (Extra data):")
        print(", ".join(sorted(list(missing_in_1m))[:50]))
        if len(missing_in_1m) > 50:
            print("... and more.")
            
    if not missing_in_1s and not missing_in_1m:
        print("\n✅ PERFECT MATCH! All symbols correspond one-to-one.")

if __name__ == "__main__":
    main()
