
import requests
import zipfile
import io
import pandas as pd

url = "https://data.binance.vision/data/futures/um/daily/klines/BLZUSDT/1m/BLZUSDT-1m-2025-12-06.zip"

print(f"Downloading {url}...")
r = requests.get(url, timeout=10)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_name = z.namelist()[0]
        print(f"Found CSV: {csv_name}")
        with z.open(csv_name) as f:
            # Read first few lines as bytes then decode
            head = [f.readline().decode('utf-8').strip() for _ in range(5)]
            print("\n--- First 5 lines ---")
            for line in head:
                print(line)
