
import os
from pathlib import Path
import pandas as pd

def check_files():
    data_dir_1s = Path("/Users/muce/1m_data/klines_data_usdm_1s_agg")
    
    # Symbols seen in Fallback logs
    check_list = [
        ('TWTUSDT', '20250920'),
        ('OPENUSDT', '20250920'),
        ('NEARUSDT', '20250920'),
        ('BBUSDT', '20250920'),
        ('LDOUSDT', '20250920'),
        ('AIAUSDT', '20250920'),
        ('IPUSDT', '20250920')
    ]
    
    print(f"Checking 1s data availability in {data_dir_1s}...")
    
    for symbol, date_str in check_list:
        folder_name = f"{symbol}_1s_agg"
        folder_path = data_dir_1s / folder_name
        
        if not folder_path.exists():
            print(f"❌ Folder NOT FOUND: {folder_path}")
            continue
            
        # Check specific file
        # Format: {symbol}_1s_{YYYYMMDD}.parquet
        file_name = f"{symbol}_1s_{date_str}.parquet"
        file_path = folder_path / file_name
        
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024*1024)
            print(f"✅ Found {file_name} ({size_mb:.2f} MB)")
        else:
            print(f"⚠️ File MISSING: {file_name}")
            # List mostly what IS there
            files = sorted(list(folder_path.glob("*.parquet")))
            if files:
                print(f"   (Folder has {len(files)} files. First: {files[0].name}, Last: {files[-1].name})")
            else:
                print("   (Folder is empty)")

if __name__ == "__main__":
    check_files()
