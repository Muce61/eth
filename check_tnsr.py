
import glob
from pathlib import Path

data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
pattern = str(data_dir / "*TNSR*.csv")
print(f"Searching pattern: {pattern}")
files = glob.glob(pattern)
print(f"Found {len(files)} files:")
for f in files:
    print(Path(f).stem)
