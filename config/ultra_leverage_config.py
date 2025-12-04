"""
配置文件 - 超高杠杆策略
"""

# === 基础配置 ===
INITIAL_BALANCE = 100  # 起始资金 (USDT)
RISK_PER_TRADE = 5.0  # 每笔交易风险 (%) - 高杠杆下实际仓位很小

# === 杠杆配置 ===
USE_DYNAMIC_LEVERAGE = True  # 使用动态杠杆
MIN_LEVERAGE = 20
MAX_LEVERAGE_DEFAULT = 50

# 特定币种最大杠杆 (根据币安实际规则)
COIN_MAX_LEVERAGE = {
    'BTCUSDT': 125,
    'ETHUSDT': 125,
    'BNBUSDT': 100,
    'SOLUSDT': 75,
    'ADAUSDT': 50,
    'XRPUSDT': 50,
    'DOGEUSDT': 50,
    'MATICUSDT': 50,
    'LINKUSDT': 50,
    'AVAXUSDT': 50,
}

# === 策略参数 ===
SIGNAL_STRENGTH_THRESHOLD = 90  # 最低信号强度 (90分)
PERFECT_SIGNAL_THRESHOLD = 95   # 完美信号阈值 (95分)

# === 仓位管理 ===
MAX_OPEN_POSITIONS = 3  # 最多同时3个仓位
MAX_LONG_POSITIONS = 2   # 最多2个多头
MAX_SHORT_POSITIONS = 2  # 最多2个空头

# === 止损配置 (高杠杆下必须极严格) ===
STOP_LOSS_PERCENT = 0.3  # 硬止损 0.3% (50x杠杆下 = 15% ROI损失)
# 时间止损（分钟）- Phase 1研究优化
TIME_STOP_MINUTES = 45  # 从30增加到45，数据显示>15分钟胜率更高
VOLATILITY_STOP_MULTIPLIER = 2.0  # 波动止损倍数

# === 止盈配置 ===
TAKE_PROFIT_QUICK = 0.5  # 快速止盈 0.5% (50x = 25% ROI)
TAKE_PROFIT_TARGET = 1.0  # 目标止盈 1.0% (50x = 50% ROI)
USE_TRAILING_STOP = True
TRAILING_ACTIVATION_PERCENT = 0.5  # 0.5%激活追踪
TRAILING_CALLBACK_PERCENT = 0.15   # 回撤0.15%止盈

# === 风控限制 ===
MAX_DAILY_LOSS_PERCENT = 15  # 单日最大亏损15%
MAX_CONSECUTIVE_LOSSES = 3    # 连续亏损3次停止交易
DAILY_PROFIT_TARGET = 50      # 单日盈利目标50% (达到后降低仓位)

# === 选币过滤 ===
MIN_VOLUME_24H = 50_000_000  # 最小24h成交量 5000万USDT
TOP_N_COINS = 50              # 只交易Top 50
EXCLUDED_COINS = [
    # 排除稳定币和特殊代币
    'USDCUSDT',
    'BUSDUSDT',
    'TUSDUSDT',
]

# === 时间过滤 ===
TRADING_HOURS_UTC = {
    'BEST': [15, 16, 17, 21, 22, 23],  # 欧洲和美国开盘
    'AVOID': [5, 6, 7, 8],              # 亚洲早盘
}
TRADE_ON_WEEKENDS = True  # 允许周末交易以收集数据

# === 数据配置 ===
DATA_DIR = "/Users/muce/1m_data/processed_15m_data"
TIMEFRAMES = ['15m', '1h', '4h']  # 多周期

# === 回测配置 ===
BACKTEST_SLIPPAGE = 0.0005  # 0.05% 滑点
BACKTEST_FEE = 0.0004       # 0.04% 手续费 (VIP用户)

# === API配置 (实盘用) ===
EXCHANGE = 'binance'
TESTNET = True  # 先用测试网
API_KEY = ''
API_SECRET = ''
