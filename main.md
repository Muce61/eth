# 数据获取清单与对应脚本/命令

## 必须获取的数据
- K线OHLCV（多周期：1m/5m/15m/1h/4h/1d，可扩展1s）
  - 字段：timestamp, open, high, low, close, volume
  - 对应脚本：`src/data/ohlcv_fetcher.py`
  - CLI入口：`src/cli.py` 的 `fetch-historical`
  - 运行命令（示例）：
    ```bash
    # 抓取币安 ETH/USDT 1分钟K线，自 2023-01-01 至今，写入 Parquet
    python -m src.cli fetch-historical \
      --exchange binance \
      --symbol ETH/USDT \
      --timeframe 1m \
      --since 2023-01-01 \
      --until now \
      --out data/raw \
      --format parquet \
      --limit 1000
    ```

- 资金费率（Funding Rate）历史序列（永续合约）
  - 字段示例：fundingTime, fundingRate
  - 对应脚本：待新增（建议 `src/data/funding_fetcher.py`）
  - 运行命令：待在 CLI 中新增命令（建议 `funding-fetch`）

- Taker 主动买/卖量与不平衡（按周期聚合）
  - 字段示例：taker_buy_vol, taker_sell_vol, taker_imbalance
  - 对应脚本：待新增（建议 `src/data/trades_aggregator.py` 负责从逐笔成交聚合）
  - 运行命令：待在 CLI 中新增命令（建议 `aggregate-trades`）

## 实盘/低延迟数据（可选扩展）
- 实时K线/成交（WebSocket）
  - 对应脚本：待新增（建议 `src/data/realtime_ws.py`）
  - 运行命令：待在 CLI 中新增命令（建议 `ws-run`）

- 盘口深度（Order Book / L2）
  - 对应脚本：待新增（建议 `src/data/orderbook_ws.py`）
  - 运行命令：待在 CLI 中新增命令（建议 `orderbook-ws`）

## 本地派生特征（由行情计算）
- 布林带、%B、带宽；RSI、MACD；移动均线/斜率；收益率/波动率、ATR 等
  - 对应脚本：待新增（建议 `src/features/indicators.py`）
  - 运行命令：待在 CLI 中新增命令（建议 `build-features`）

## 存储与文件组织
- 写入：`src/storage/file_storage.py`
- 输出目录结构：`{out}/{exchange}/{symbol}/{timeframe}.parquet|csv`，例如：`data/raw/binance/ETH-USDT/1m.parquet`

## 时间参数说明
- 时间解析：`src/utils/time_utils.py` 的 `parse_time_to_ms`
- `--since` 支持日期（如 `2023-01-01`）或毫秒时间戳；`--until` 支持 `now`/日期/毫秒时间戳
