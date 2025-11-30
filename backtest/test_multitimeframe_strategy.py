import sys
from pathlib import Path
import pandas as pd
import pandas_ta as ta

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backtest.real_engine import RealBacktestEngine
from strategy.momentum import MomentumStrategy

class MultiTimeframeStrategy(MomentumStrategy):
    """
    多时间框架策略
    1. 先在1小时级别确认趋势
    2. 再在15分钟级别寻找入场点
    """
    
    def check_signal_1h(self, symbol, df_1h):
        """
        1小时级别信号确认 (仅作参考，不拦截)
        """
        if len(df_1h) < 50:
            return False
            
        # 只要数据足够，就返回True，不进行硬性过滤
        # 具体的1小时指标可以在15分钟信号中作为参考因子
        return True
    
    def check_signal(self, symbol, df):
        """
        15分钟级别入场信号 (主信号源)
        """
        if df.empty or len(df) < 20:
            return None
        
        current = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 获取timestamp
        timestamp = df['timestamp'].iloc[-1] if 'timestamp' in df.columns else df.index[-1]
        
        # 核心条件1: 突破确认 (Close > Previous High)
        if current['close'] <= prev['high']:
            return None
        
        # 核心条件2: 成交量确认 (放宽至 2x average)
        avg_vol = df['volume'].iloc[-21:-1].mean()
        if current['volume'] < 2.0 * avg_vol:  # 从3x降低到2x
            return None
        
        # 核心条件3: RSI范围 (55-90，放宽)
        rsi = ta.rsi(df['close'], length=14).iloc[-1]
        if not (55 <= rsi <= 90):  # 从60-85放宽至55-90
            return None
        
        # 计算metrics用于记录
        vol_ratio = current['volume'] / avg_vol
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)['ADX_14'].iloc[-1]
        
        return {
            'symbol': symbol,
            'side': 'LONG',
            'entry_price': current['close'],
            'timestamp': timestamp,
            'metrics': {
                'rsi': rsi,
                'adx': adx,
                'volume_ratio': vol_ratio
            }
        }


class MultiTimeframeEngine(RealBacktestEngine):
    """
    多时间框架回测引擎
    涨幅范围：5%-20%（更窄，减少极端币种）
    """
    def __init__(self, initial_balance=100):
        super().__init__(initial_balance)
        
        # 使用多时间框架策略
        self.strategy = MultiTimeframeStrategy()
        
        # 修改涨幅范围 (迭代3 - 进一步扩大)
        self.config.CHANGE_THRESHOLD_MIN = 0.0   # 0% (捕捉底部启动)
        self.config.CHANGE_THRESHOLD_MAX = 50.0  # 50% (捕捉强势延续)
        self.config.TOP_GAINER_COUNT = 100       # 扫描前100名
        
        # 20x杠杆 + 分批止盈
        self.config.LEVERAGE = 20
        self.risk_manager.config.LEVERAGE = 20
        
        # 存储1小时数据
        self.data_feed_1h = {}
        
        # 存储待入场的信号
        self.pending_entries = {}
        
        print(f"⚙️  策略: 多时间框架 (15M主导 + 1H参考)")
        print(f"📊 涨幅范围: 不限 (扫描全市场)")
        print(f"📈 1H条件: 仅作参考 (不拦截)")
        print(f"📉 15M条件: Vol>2x, RSI 55-90")
        print(f"⚙️  杠杆: 20x")
        print(f"💰 分批止盈: 15% (40%), 25% (30%), 40% (all)")
    
    def _open_position(self, symbol, price, timestamp, history_slice, metrics=None):
        """
        Override to force 20x leverage
        """
        leverage = 20
        self.risk_manager.config.LEVERAGE = 20
        
        if history_slice is not None and len(history_slice) >= 14:
            import pandas_ta as ta
            atr = ta.atr(history_slice['high'], history_slice['low'], history_slice['close'], length=14).iloc[-1]
            sl_distance = atr * 2.5
            max_stop_distance = price * 0.035
            sl_distance = min(sl_distance, max_stop_distance)
            stop_loss_pct = sl_distance / price
        else:
            stop_loss_pct = 0.035
        
        slippage = 0.0005
        entry_price_with_slippage = price * (1 + slippage)
        stop_loss = entry_price_with_slippage * (1 - stop_loss_pct)
        
        quantity = self.risk_manager.calculate_position_size(self.balance, entry_price_with_slippage, stop_loss)
        
        if quantity <= 0:
            return
        
        notional = price * quantity
        fee = notional * 0.0005
        self.balance -= fee
        
        self.positions[symbol] = {
            'entry_price': entry_price_with_slippage,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'highest_price': entry_price_with_slippage,
            'entry_time': timestamp,
            'leverage': leverage,
            'metrics': metrics or {}
        }
        
        print(f"[{timestamp}] OPEN LONG {symbol} @ {entry_price_with_slippage:.4f} | SL: {stop_loss:.4f} | Size: {quantity:.2f}")
    
    def load_data(self):
        """
        加载全市场597个币种的15分钟和1小时数据
        """
        data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed = {}
        self.data_feed_1h = {}
        
        print(f"Loading ALL 597 coins from {data_dir}...")
        
        if not data_dir.exists():
            print(f"Error: {data_dir} does not exist!")
            return
        
        files = list(data_dir.glob("*.csv"))
        loaded_count = 0
        
        for file_path in files:
            try:
                symbol = file_path.stem
                
                # 读取1分钟数据
                df_1m = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
                
                # 重采样到15分钟
                df_15m = df_1m.resample('15min').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                })
                df_15m.dropna(inplace=True)
                
                # 重采样到1小时
                df_1h = df_1m.resample('1h').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                })
                df_1h.dropna(inplace=True)
                
                if len(df_15m) > 50 and len(df_1h) > 50:
                    self.data_feed[symbol] = df_15m
                    self.data_feed_1h[symbol] = df_1h
                    loaded_count += 1
                    
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
        
        print(f"✅ Loaded {loaded_count} coins (15M + 1H data)")
    
    def _scan_market(self, current_time):
        """
        扫描市场，使用多时间框架确认
        """
        candidates = []
        
        for symbol, df_15m in self.data_feed.items():
            if symbol in self.positions:
                continue
            
            if symbol not in self.data_feed_1h:
                continue
            
            df_1h = self.data_feed_1h[symbol]
            
            # 确保有足够数据
            available_15m = df_15m[df_15m.index <= current_time]
            available_1h = df_1h[df_1h.index <= current_time]
            
            if len(available_15m) < 50 or len(available_1h) < 50:
                continue
            
            # 计算24小时涨幅（使用时间索引，避免数据缺失导致的计算错误）
            time_24h_ago = current_time - pd.Timedelta(hours=24)
            
            # 查找24小时前的价格（如果找不到精确时间，找最近的一个）
            # 使用 searchsorted 找到位置
            idx = available_15m.index.searchsorted(time_24h_ago)
            
            # 如果位置超出范围或太远，跳过
            if idx >= len(available_15m):
                continue
                
            # 获取该位置的时间戳
            found_time = available_15m.index[idx]
            
            # 如果找到的时间与目标时间相差超过4小时，说明数据缺失太严重，跳过
            if abs((found_time - time_24h_ago).total_seconds()) > 4 * 3600:
                continue
                
            current_price = available_15m.iloc[-1]['close']
            price_24h_ago = available_15m.iloc[idx]['close']
            change_pct = ((current_price - price_24h_ago) / price_24h_ago) * 100
            
            # 涨幅筛选：已移除，直接扫描所有币种
            # if self.config.CHANGE_THRESHOLD_MIN <= change_pct <= self.config.CHANGE_THRESHOLD_MAX:
            
            # 直接检查1小时级别确认 (仅作参考)
            confirmed_1h = self.strategy.check_signal_1h(symbol, available_1h)
            
            if confirmed_1h:
                candidates.append({
                    'symbol': symbol,
                    'change_pct': change_pct,
                    'confirmed_1h': True
                })
        
        # 按涨幅排序
        candidates.sort(key=lambda x: x['change_pct'], reverse=True)
        # candidates = candidates[:self.config.TOP_GAINER_COUNT] # 移除Top N限制，扫描所有符合涨幅条件的币种
        
        # 对筛选出的候选进行15分钟信号检查
        for candidate in candidates:
            symbol = candidate['symbol']
            df_15m = self.data_feed[symbol]
            available = df_15m[df_15m.index <= current_time]
            
            if len(available) < 50:
                continue
            
            # 检查15分钟信号
            signal = self.strategy.check_signal(symbol, available)
            
            if signal and signal['side'] == 'LONG':
                # 添加1小时确认标记到metrics
                if 'metrics' not in signal:
                    signal['metrics'] = {}
                signal['metrics']['confirmed_1h'] = True
                signal['metrics']['change_24h'] = candidate['change_pct']
                
                # 标记下一根K线开盘入场
                next_candle_time = current_time + pd.Timedelta(minutes=15)
                if next_candle_time not in self.pending_entries:
                    self.pending_entries[next_candle_time] = []
                self.pending_entries[next_candle_time].append({
                    'symbol': symbol,
                    'signal': signal
                })

def main():
    print("="*60)
    print("多时间框架策略回测 (30天)")
    print("涨幅范围: 5% - 20%")
    print("确认: 1小时 + 15分钟")
    print("="*60)
    
    engine = MultiTimeframeEngine(initial_balance=100)
    engine.run(days=30)

if __name__ == "__main__":
    main()
