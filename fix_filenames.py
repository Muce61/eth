
import os
from pathlib import Path

target_dir = Path("/Users/muce/1m_data/old/new_backtest_data_1year_1m")

if not target_dir.exists():
    print(f"Error: Directory {target_dir} not found.")
    exit(1)

print(f"Scanning {target_dir} for *USDTUSDT.csv ...")

files = list(target_dir.glob("*USDTUSDT.csv"))
print(f"Found {len(files)} files with double suffix.")

renamed_count = 0
for file_path in files:
    old_name = file_path.name
    new_name = old_name.replace("USDTUSDT.csv", "USDT.csv")
    new_path = file_path.parent / new_name
    
    # Check if target exists (collision?)
    if new_path.exists():
        print(f"⚠️ Safety Skipping {old_name} -> {new_name} (Target already exists)")
        continue
        
    try:
        os.rename(file_path, new_path)
        renamed_count += 1
        if renamed_count % 50 == 0:
            print(f"Renamed {renamed_count} files...")
    except Exception as e:
        print(f"Error renaming {old_name}: {e}")

print(f"✅ Completed. Renamed {renamed_count}/{len(files)} files.")
