# 量化策略 - 数据获取阶段

本仓库提供基于 `ccxt` 的历史 K 线数据抓取与存储工具（CSV/Parquet）。

## 快速开始

1. 安装依赖
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt
```

2. 获取历史 K 线（示例：Binance，ETH/USDT，1m，从 2023-01-01 到现在）
```bash
python -m src.cli fetch-historical \
  --exchange binance \
  --symbol "ETH/USDT" \
  --timeframe 1m \
  --since 2023-01-01 \
  --until now \
  --out data/raw \
  --format parquet
```

3. 输出位置
- Parquet/CSV 文件默认写入 `data/raw/{exchange}/{symbol_no_slash}/{timeframe}.{ext}`
- 若文件已存在，将进行增量合并、去重并按时间排序。

## 常见参数
- `--exchange`: 交易所（支持 ccxt 中的大多数交易所，如 binance、okx 等）
- `--symbol`: 交易对（如 `ETH/USDT`）
- `--timeframe`: K 线周期（如 `1m`, `5m`, `15m`, `1h`, `4h`, `1d`）
- `--since`: 起始时间（如 `2023-01-01` 或毫秒时间戳）
- `--until`: 结束时间（`now` 或具体时间）
- `--format`: `parquet` 或 `csv`

## 注意
- 若频繁请求，请遵守交易所限速规则；本工具默认启用 `enableRateLimit`。
- 部分交易所历史数据范围有限；如需全量回补，建议从更早的时间点启动并耐心等待。 