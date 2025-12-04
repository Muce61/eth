#!/usr/bin/env python3
"""
超高杠杆回测引擎
Ultra-High Leverage Backtest Engine

支持:
- 多周期数据 (15m, 1h, 4h)
- 动态杠杆 50x-125x
- 极严格止损 0.3%
- 信号强度评分
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategy.ultra_leverage import MultiTimeframeStrategy
from config import ultra_leverage_config as config

class UltraLeverageBacktest:
    """超高杠杆回测引擎"""
    
    def __init__(self):
        # 加载杠杆信息
        self.leverage_map = self.load_leverage_brackets()
        
        self.strategy = MultiTimeframeStrategy(leverage_map=self.leverage_map)
        self.balance = config.INITIAL_BALANCE
        self.initial_balance = config.INITIAL_BALANCE
        self.positions = {}  # {symbol: position_dict}
        self.trades = []
        self.daily_stats = []
        
        # 加载多周期数据
        self.data_1m = {}   # 新增: 1分钟数据用于精确入场
        self.data_15m = {}
        self.data_1h = {}
        self.data_4h = {}
        
        max_lev = max(self.leverage_map.values()) if self.leverage_map else 0
        print("初始化超高杠杆回测引擎...")
        print(f"起始资金: ${self.balance}")
        print(f"最大杠杆: {max_lev}x (BTC/ETH)")
        print(f"硬止损: {config.STOP_LOSS_PERCENT}%")
        print(f"可用币种: {len(self.leverage_map)}个")
        
    def load_multiframe_data(self, symbols=None):
        """加载多周期数据 + 1分钟数据 (用于精确入场)"""
        print("\n加载多周期数据 (1m/15m/1h/4h)...")
        
        dir_1m = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")  # 新增
        dir_15m = Path(config.DATA_DIR)
        dir_1h = Path("/Users/muce/1m_data/processed_1h_data")
        dir_4h = Path("/Users/muce/1m_data/processed_4h_data")
        
        if not dir_1m.exists() or not dir_15m.exists() or not dir_1h.exists() or not dir_4h.exists():
            print(f"❌ 数据目录不完整")
            print(f"1m: {dir_1m.exists()}, 15m: {dir_15m.exists()}, 1h: {dir_1h.exists()}, 4h: {dir_4h.exists()}")
            return
        
        # 获取所有可用币种
        files = list(dir_15m.glob("*USDT.csv"))
        temp_data = []
        
        print(f"扫描 {len(files)} 个币种并计算成交量...")
        
        for file in files:
            symbol = file.stem
            if symbol in config.EXCLUDED_COINS:
                continue
            
            # 仅加载有杠杆信息的币种
            if symbol not in self.leverage_map:
                continue
                
            try:
                # 预读计算成交量 (只读最后1000行以加速)
                df_preview = pd.read_csv(file)
                if len(df_preview) < 1000:
                    continue
                    
                avg_vol = (df_preview['close'] * df_preview['volume']).mean() # 美元成交量
                
                if avg_vol > 100000: # 最小日均成交量过滤
                    temp_data.append({
                        'symbol': symbol,
                        'file': file,
                        'avg_vol': avg_vol
                    })
                
            except Exception:
                continue
        
        # 按成交量排序，取Top 50
        temp_data.sort(key=lambda x: x['avg_vol'], reverse=True)
        top_coins = temp_data[:50]
        
        print(f"选取 Top {len(top_coins)} 活跃币种 (Vol > $100k)")
        
        loaded = 0
        for coin in top_coins:
            symbol = coin['symbol']
            
            try:
                # 加载四个周期数据 (新增1m)
                df_1m = pd.read_csv(dir_1m / f"{symbol}.csv")
                df_15m = pd.read_csv(coin['file'])
                df_1h = pd.read_csv(dir_1h / f"{symbol}.csv")
                df_4h = pd.read_csv(dir_4h / f"{symbol}.csv")
                
                # 转换时间索引
                for df in [df_1m, df_15m, df_1h, df_4h]:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    df.sort_index(inplace=True)
                
                self.data_1m[symbol] = df_1m
                self.data_15m[symbol] = df_15m
                self.data_1h[symbol] = df_1h
                self.data_4h[symbol] = df_4h
                
                loaded += 1
                print(f"已加载: {symbol} (Vol: ${coin['avg_vol']/1000:.0f}k)")
                
            except Exception as e:
                print(f"加载失败 {symbol}: {e}")
                continue
        
        print(f"✅ 成功加载 {loaded} 个活跃币种")
    
    def load_leverage_brackets(self):
        """加载杠杆信息 from leverage_brackets.csv"""
        leverage_file = Path('leverage_brackets.csv')
        if not leverage_file.exists():
            print("⚠️ leverage_brackets.csv not found, using default leverage")
            return {}
        
        df = pd.read_csv(leverage_file)
        leverage_map = {}
        
        # Symbol格式转换: BTCUSDT -> BTCUSDTUSDT
        for _, row in df.iterrows():
            symbol_raw = row['symbol']  # e.g., "BTCUSDT"
            symbol_data = symbol_raw + "USDT"  # e.g., "BTCUSDTUSDT" (匹配数据文件)
            leverage_map[symbol_data] = int(row['max_leverage'])
        
        print(f"\n💪 杠杆信息加载完成:")
        print(f"  总币种: {len(leverage_map)}")
        top_lev = sorted(leverage_map.items(), key=lambda x: x[1], reverse=True)[:3]
        for sym, lev in top_lev:
            print(f"  {sym}: {lev}x")
        
        return leverage_map
        
    def calculate_position_size(self, symbol, entry_price, stop_loss, leverage):
        """
        计算仓位大小 (高杠杆下)
        
        风险固定模型: 每笔风险 = 账户的 RISK_PER_TRADE%
        """
        risk_amount = self.balance * (config.RISK_PER_TRADE / 100)
        price_risk = abs(entry_price - stop_loss)
        
        if price_risk == 0:
            return 0
        
        # 计算合约数量
        quantity = risk_amount / price_risk
        
        # 考虑杠杆，实际占用保证金很小
        margin_required = (entry_price * quantity) / leverage
        
        # 确保不超过余额
        if margin_required > self.balance * 0.9:  # 最多用90%保证金
            quantity = (self.balance * 0.9 * leverage) / entry_price
        
        return quantity
    
    def open_position(self, symbol, side, entry_price, stop_loss, leverage, timestamp, strength, breakdown):
        """开仓"""
        quantity = self.calculate_position_size(symbol, entry_price, stop_loss, leverage)
        
        if quantity <= 0:
            return False
        
        # 计算需要的保证金
        notional = entry_price * quantity
        margin = notional / leverage
        fee = notional * config.BACKTEST_FEE
        
        # 检查余额
        if margin + fee > self.balance:
            return False
        
        # 扣除保证金和手续费
        self.balance -= (margin + fee)
        
        # 记录仓位
        self.positions[symbol] = {
            'side': side,
            'entry_price': entry_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'leverage': leverage,
            'entry_time': timestamp,
            'margin': margin,
            'highest_price': entry_price if side == 'LONG' else entry_price,
            'lowest_price': entry_price if side == 'SHORT' else entry_price,
            'strength': strength,
            'breakdown': breakdown
        }
        
        print(f"[{timestamp}] 🚀 OPEN {side} {symbol} @ {entry_price:.4f}")
        print(f"  杠杆: {leverage}x | 数量: {quantity:.2f} | 止损: {stop_loss:.4f}")
        print(f"  信号强度: {strength}分 | 保证金: ${margin:.2f}")
        
        return True
    
    def close_position(self, symbol, exit_price, timestamp, reason):
        """平仓"""
        if symbol not in self.positions:
            return
        
        pos = self.positions[symbol]
        
        # 计算PnL (考虑杠杆)
        if pos['side'] == 'LONG':
            price_change = exit_price - pos['entry_price']
        else:  # SHORT
            price_change = pos['entry_price'] - exit_price
        
        pnl_before_fee = price_change * pos['quantity']
        
        # 手续费
        notional = exit_price * pos['quantity']
        fee = notional * config.BACKTEST_FEE
        
        # 净PnL
        net_pnl = pnl_before_fee - fee
        
        # 更新余额 (释放保证金 + PnL)
        self.balance += pos['margin'] + net_pnl
        
        # ROI = PnL / Margin
        roi = (net_pnl / pos['margin']) * 100
        
        # 记录交易
        self.trades.append({
            'symbol': symbol,
            'side': pos['side'],
            'entry_price': pos['entry_price'],
            'exit_price': exit_price,
            'entry_time': pos['entry_time'],
            'exit_time': timestamp,
            'quantity': pos['quantity'],
            'leverage': pos['leverage'],
            'pnl': net_pnl,
            'roi': roi,
            'reason': reason,
            'duration': (timestamp - pos['entry_time']),
            'signal_strength': pos['strength'],
            'balance_after': self.balance
        })
        
        print(f"[{timestamp}] ❌ CLOSE {symbol} @ {exit_price:.4f}")
        print(f"  PnL: ${net_pnl:.2f} | ROI: {roi:.1f}% | 原因: {reason}")
        print(f"  余额: ${self.balance:.2f}")
        
        # 实时保存交易记录
        pd.DataFrame(self.trades).to_csv('ultra_leverage_backtest.csv', index=False)
        
        # 删除仓位
        del self.positions[symbol]
    
    def manage_positions(self, current_time):
        """管理持仓 - 极严格止损"""
        to_close = []
        
        for symbol, pos in self.positions.items():
            # 获取当前价格 (使用1分钟数据)
            if symbol not in self.data_1m:
                print(f"DEBUG: {symbol} not in data_1m")
                continue
            
            df = self.data_1m[symbol]
            if current_time not in df.index:
                # 尝试容错：找最近的前一个时间点
                idx = df.index.asof(current_time)
                if pd.isna(idx):
                    continue
                candle = df.loc[idx]
            else:
                candle = df.loc[current_time]
            
            high = candle['high']
            low = candle['low']
            close = candle['close']
            
            # print(f"DEBUG: Managing {symbol} at {current_time} | Close: {close}")
            
            # === Layer 1: 硬止损 (0.3%) ===
            if pos['side'] == 'LONG':
                if low <= pos['stop_loss']:
                    to_close.append((symbol, pos['stop_loss'], 'Hard Stop Loss'))
                    continue
            else:  # SHORT
                if high >= pos['stop_loss']:
                    to_close.append((symbol, pos['stop_loss'], 'Hard Stop Loss'))
                    continue
            
            # === Layer 2: 时间止损 (15分钟) ===
            holding_minutes = (current_time - pos['entry_time']).total_seconds() / 60
            if holding_minutes > config.TIME_STOP_MINUTES:
                # 计算当前PnL
                if pos['side'] == 'LONG':
                    pnl_pct = (close - pos['entry_price']) / pos['entry_price'] * 100
                else:
                    pnl_pct = (pos['entry_price'] - close) / pos['entry_price'] * 100
                
                if pnl_pct < 0:
                    to_close.append((symbol, close, f'Time Stop ({int(holding_minutes)}min)'))
                    continue
            
            # === 追踪止盈 ===
            if pos['side'] == 'LONG':
                if high > pos['highest_price']:
                    pos['highest_price'] = high
                
                # 计算最高点PnL
                max_pnl_pct = (pos['highest_price'] - pos['entry_price']) / pos['entry_price'] * 100
                
                # 如果达到快速止盈目标
                if max_pnl_pct >= config.TAKE_PROFIT_QUICK:
                    # 回撤超过阈值就止盈
                    current_pnl_pct = (close - pos['entry_price']) / pos['entry_price'] * 100
                    drawdown = max_pnl_pct - current_pnl_pct
                    
                    if drawdown >= config.TRAILING_CALLBACK_PERCENT:
                        to_close.append((symbol, close, f'Trailing TP (Max {max_pnl_pct:.1f}%)'))
                        continue
            
            else:  # SHORT
                if low < pos['lowest_price']:
                    pos['lowest_price'] = low
                
                max_pnl_pct = (pos['entry_price'] - pos['lowest_price']) / pos['entry_price'] * 100
                
                if max_pnl_pct >= config.TAKE_PROFIT_QUICK:
                    current_pnl_pct = (pos['entry_price'] - close) / pos['entry_price'] * 100
                    drawdown = max_pnl_pct - current_pnl_pct
                    
                    if drawdown >= config.TRAILING_CALLBACK_PERCENT:
                        to_close.append((symbol, close, f'Trailing TP (Max {max_pnl_pct:.1f}%)'))
                        continue
        
        # 执行平仓
        for symbol, price, reason in to_close:
            self.close_position(symbol, price, current_time, reason)
    
    def run(self, start_date, end_date):
        """运行回测 (1分钟粒度)"""
        if not self.data_1m or not self.data_15m:
            print("❌ 请先加载数据")
            return
        
        # 1. 获取时间戳序列 (使用1分钟数据的时间戳!!)
        print("\n⚡ 构建1分钟级别时间轴...")
        all_timestamps = set()
        for df in self.data_1m.values():
            all_timestamps.update(df.index)
        
        sorted_timestamps = sorted(list(all_timestamps))
        print(f"总时间点: {len(sorted_timestamps)} (1分钟粒度)")
        
        # 2. 过滤日期范围
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        
        filtered_timestamps = [ts for ts in sorted_timestamps if start_ts <= ts <= end_ts]
        
        print(f"回测时间范围: {filtered_timestamps[0]} 到 {filtered_timestamps[-1]}")
        print("="*60)
        
        iterations = 0
        
        for current_time in filtered_timestamps:
            iterations += 1
            
            # 优化: 只在有持仓或15分钟边界时才处理
            has_positions = len(self.positions) > 0
            is_15m_boundary = current_time.minute % 15 == 0
            
            if not has_positions and not is_15m_boundary:
                continue  # 跳过：无持仓且非扫描时机
            
            # 1. 管理现有持仓（只在有持仓时）
            if has_positions:
                self.manage_positions(current_time)
            
            # 2. 检查新信号（只在15分钟边界且未满仓时）
            if is_15m_boundary and len(self.positions) < config.MAX_OPEN_POSITIONS:
                self.scan_and_open(current_time)
            
            # 进度显示（降低频率）
            if iterations % (60 * 24 * 5) == 0:  # 每5天打印一次
                print(f"⏳ {current_time} | 余额: ${self.balance:.2f} | 持仓: {len(self.positions)}")
        
        # 强制平掉所有剩余仓位
        for symbol in list(self.positions.keys()):
            if symbol in self.data_1m:
                # 使用回测结束时的1m数据作为最终价格
                final_price = self.data_1m[symbol].loc[filtered_timestamps[-1]]['close']
                self.close_position(symbol, final_price, filtered_timestamps[-1], 'End of Backtest')
        
        self.generate_report()
    
    def scan_and_open(self, current_time):
        """扫描并开仓 (1分钟级别精确入场)"""
        # 达到最大持仓数
        if len(self.positions) >= config.MAX_OPEN_POSITIONS:
            return
        
        # 扫描所有币种
        for symbol in list(self.data_1m.keys()):  # 改为1m数据字典
            if len(self.positions) >= config.MAX_OPEN_POSITIONS:
                break
            
            if symbol in self.positions:
                continue  # 已有持仓
            
            # 检查当前时间是否在1m数据范围内
            if current_time not in self.data_1m[symbol].index:
                continue
            
            # 检查信号 (仍然用15m/1h/4h判断)
            # 确保有足够的历史数据来计算指标
            df_15m_slice = self.data_15m[symbol].loc[:current_time]
            df_1h_slice = self.data_1h[symbol].loc[:current_time]
            df_4h_slice = self.data_4h[symbol].loc[:current_time]
            
            if len(df_15m_slice) < 300 or len(df_1h_slice) < 300 or len(df_4h_slice) < 300:
                continue
            
            signal = self.strategy.check_signal(
                symbol,
                {'15m': df_15m_slice.tail(300), '1h': df_1h_slice.tail(300), '4h': df_4h_slice.tail(300)},
                current_time
            )
            
            if signal is None:
                continue
            
            # 开仓 - 使用1分钟数据的精确价格！
            entry_price = self.data_1m[symbol].loc[current_time, 'close']
            
            # 计算止损
            if signal['side'] == 'LONG':
                stop_loss = entry_price * (1 - config.STOP_LOSS_PERCENT / 100)
            else:
                stop_loss = entry_price * (1 + config.STOP_LOSS_PERCENT / 100)
            
            # 执行开仓
            success = self.open_position(
                symbol,
                signal['side'],
                entry_price,
                stop_loss,
                signal['leverage'],
                current_time,
                signal['strength'],
                signal['breakdown']
            )
            
            if success:
                break  # 每次只开一个新仓
    
    def generate_report(self):
        """生成报告"""
        print("\n" + "="*60)
        print("📊 超高杠杆回测报告")
        print("="*60)
        
        total_return = (self.balance - self.initial_balance) / self.initial_balance * 100
        
        print(f"\n💰 资金表现:")
        print(f"  起始: ${self.initial_balance:.2f}")
        print(f"  最终: ${self.balance:.2f}")
        print(f"  收益率: {total_return:.2f}%")
        
        if not self.trades:
            print("\n⚠️ 无交易记录")
            return
        
        wins = [t for t in self.trades if t['pnl'] > 0]
        losses = [t for t in self.trades if t['pnl'] <= 0]
        
        print(f"\n📈 交易统计:")
        print(f"  总交易: {len(self.trades)}")
        print(f"  盈利: {len(wins)} ({len(wins)/len(self.trades)*100:.1f}%)")
        print(f"  亏损: {len(losses)} ({len(losses)/len(self.trades)*100:.1f}%)")
        print(f"  **胜率: {len(wins)/len(self.trades)*100:.1f}%**")
        
        if wins:
            avg_win_roi = np.mean([t['roi'] for t in wins])
            print(f"  平均盈利ROI: {avg_win_roi:.1f}%")
        if losses:
            avg_loss_roi = np.mean([t['roi'] for t in losses])
            print(f"  平均亏损ROI: {avg_loss_roi:.1f}%")
        
        # 按杠杆分组
        print(f"\n⚡ 杠杆使用分布:")
        df_trades = pd.DataFrame(self.trades)
        for lev in sorted(df_trades['leverage'].unique()):
            lev_trades = df_trades[df_trades['leverage'] == lev]
            lev_wins = len(lev_trades[lev_trades['pnl'] > 0])
            print(f"  {lev}x: {len(lev_trades)}笔 (胜率{lev_wins/len(lev_trades)*100:.0f}%)")
        
        # 保存结果
        df_trades.to_csv('ultra_leverage_backtest.csv', index=False)
        print(f"\n✅ 详细结果已保存至: ultra_leverage_backtest.csv")

if __name__ == "__main__":
    backtest = UltraLeverageBacktest()
    backtest.load_multiframe_data()
    
    # 运行11月回测 (最近一个月)
    backtest.run(
        start_date=datetime(2025, 11, 1),
        end_date=datetime(2025, 11, 30)
    )
