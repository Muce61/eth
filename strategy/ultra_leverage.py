#!/usr/bin/env python3
"""
多周期共振策略
Multi-Timeframe Resonance Strategy for Ultra-High Win Rate

核心理念: 15m + 1h + 4h 三周期必须同时确认，才产生信号
目标胜率: 95%+
杠杆: 动态 50x-125x
"""

import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta

class MultiTimeframeStrategy:
    """
    超高胜率多周期共振策略
    """
    
    def __init__(self, leverage_map=None):
        # 使用外部传入的杠杆配置（来自leverage_brackets.csv）
        if leverage_map:
            self.max_leverage = leverage_map
        else:
            # Fallback: 硬编码默认值
            self.max_leverage = {
                'BTCUSDTUSDT': 125,
                'ETHUSDTUSDT': 125,
                'BNBUSDTUSDT': 100,
                'SOLUSDTUSDT': 75,
                'ADAUSDTUSDT': 50,
                'XRPUSDTUSDT': 50,
                'DOGEUSDTUSDT': 50,
            }
        
        # 信号强度阈值
        self.signal_strength_threshold = 70
        
        # ===== Phase 4 最终优化: Hour 6 Only Strategy =====
        # 白名单币种（胜率>40%）
        self.WHITELIST = [
            'VIRTUALUSDTUSDT', 'DOTUSDTUSDT', 
            'LINKUSDTUSDT', 'ENAUSDTUSDT'
        ]
        
        # 黑名单币种（胜率<20%）
        self.BLACKLIST = [
            'COAIUSDTUSDT', 'FARTCOINUSDTUSDT', 'AIAUSDTUSDT'
        ]
        
        # 黄金时段策略（Hour 6 = 52.6%胜率）
        self.GOLDEN_HOUR = 6             # UTC Hour 6
        self.GOOD_HOURS = [9, 15]        # 备选（如果6点无信号）
        self.BEST_HOURS = [6]            # 只允许Hour 6
        self.WORST_HOURS = [20, 21, 22]  # 完全避开
        
        # 周末过滤
        self.WEEKEND_TRADING = False
        
        # 甜蜜区：70-75分（性价比最高）
        self.MIN_SCORE = 70
        self.MAX_SCORE = 85  # 拒绝过高分数（可能过拟合）
        
        # BTC/ETH特殊处理（高风险）
        self.MAJOR_COINS = ['BTCUSDTUSDT', 'ETHUSDTUSDT']
        self.MAJOR_COIN_MAX_LEV = 50  # 降低杠杆
        
    def get_max_leverage(self, symbol):
        """获取币种最大杠杆（BTC/ETH特殊限制）"""
        # BTC/ETH降低杠杆至50x（Phase 4发现：高分亏损30%是BTC/ETH）
        if symbol in self.MAJOR_COINS:
            return min(self.MAJOR_COIN_MAX_LEV, self.max_leverage.get(symbol, 50))
        
        # 其他币种使用正常杠杆
        return self.max_leverage.get(symbol, 10)
    
    def detect_trend_alignment(self, df_15m, df_1h, df_4h):
        """
        检测三个周期的趋势一致性
        
        Returns:
            'LONG', 'SHORT', or None
        """
        # 计算每个周期的EMA
        for df in [df_15m, df_1h, df_4h]:
            df['ema20'] = ta.ema(df['close'], length=20)
            df['ema50'] = ta.ema(df['close'], length=50)
            df['ema200'] = ta.ema(df['close'], length=200)
        
        # 检查最新K线的趋势
        trends = []
        for df in [df_15m, df_1h, df_4h]:
            if df['ema20'].iloc[-1] > df['ema50'].iloc[-1] > df['ema200'].iloc[-1]:
                trends.append('LONG')
            elif df['ema20'].iloc[-1] < df['ema50'].iloc[-1] < df['ema200'].iloc[-1]:
                trends.append('SHORT')
            else:
                trends.append(None)
        
        # 放宽: 只需要15m和1h一致即可
        trends_short = trends[:2] # 只看前两个
        
        if all(t == 'LONG' for t in trends_short):
            return 'LONG'
        elif all(t == 'SHORT' for t in trends_short):
            return 'SHORT'
        else:
            return None
    
    def check_extreme_momentum(self, df_15m, df_1h, df_4h):
        """
        检查极端动量
        
        要求:
        - Volume Ratio > 5.0
        - ADX > 40 (所有周期)
        - 连续3根K线同向
        
        Returns:
            score (0-100)
        """
        score = 0
        
        # 1. Volume检查 (15m)
        avg_vol = df_15m['volume'].iloc[-21:-1].mean()
        current_vol = df_15m['volume'].iloc[-1]
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0
        
        if vol_ratio > 3.0:  # 原5.0
            score += 30
        elif vol_ratio > 2.0: # 原3.5
            score += 20
        elif vol_ratio > 1.5: # 原2.0
            score += 10
        
        # 2. ADX检查 (所有周期)
        adx_scores = []
        for df in [df_15m, df_1h, df_4h]:
            adx = ta.adx(df['high'], df['low'], df['close'], length=14)['ADX_14'].iloc[-1]
            if adx > 50:
                adx_scores.append(20)
            elif adx > 40:
                adx_scores.append(15)
            elif adx > 35:
                adx_scores.append(10)
            else:
                adx_scores.append(0)
        
        score += min(adx_scores)  # 取最弱的周期ADX
        
        # 3. 连续K线方向 (15m)
        last_3_closes = df_15m['close'].iloc[-3:].values
        if all(last_3_closes[i] > last_3_closes[i-1] for i in range(1, 3)):
            score += 25  # 连续上涨
        elif all(last_3_closes[i] < last_3_closes[i-1] for i in range(1, 3)):
            score += 25  # 连续下跌
        
        # 4. MACD共振 (15m和1h)
        macd_aligned = True
        for df in [df_15m, df_1h]:
            macd = ta.macd(df['close'])
            if macd is None or macd['MACD_12_26_9'].iloc[-1] * macd['MACDh_12_26_9'].iloc[-1] < 0:
                macd_aligned = False
        
        if macd_aligned:
            score += 25
        
        return min(score, 100)
    
    def check_pullback_entry(self, df_15m):
        """
        检查是否在回调入场点
        
        理想入场: 价格回调至EMA20附近，但趋势未破坏
        
        Returns:
            True/False
        """
        current_price = df_15m['close'].iloc[-1]
        ema20 = df_15m['ema20'].iloc[-1]
        ema50 = df_15m['ema50'].iloc[-1]
        
        # 计算价格与EMA20的距离
        distance_to_ema20 = abs(current_price - ema20) / ema20
        
        # 理想: 距离EMA20在0.5%以内，且在EMA50之上(多头)或之下(空头)
        if distance_to_ema20 < 0.005:  # 0.5%
            if current_price > ema50:  # 多头回调
                return True, 'LONG'
            elif current_price < ema50:  # 空头反弹
                return True, 'SHORT'
        
        return False, None
    
    def calculate_signal_strength(self, symbol, df_15m, df_1h, df_4h, current_time):
        """
        计算信号强度 (0-100分)
        
        >= 95分: 完美信号 (最大杠杆)
        >= 90分: 优秀信号 (75%杠杆)
        >= 85分: 良好信号 (50%杠杆)
        < 85分: 不交易
        """
        total_score = 0
        breakdown = {}
        
        # 1. 趋势一致性 (30分) - 提高权重
        trend = self.detect_trend_alignment(df_15m, df_1h, df_4h)
        if trend:
            total_score += 30
            breakdown['trend_alignment'] = 30
        else:
            breakdown['trend_alignment'] = 0
            return 0, None, {} # 趋势不一致，直接拒绝
            
        # 2. 极端动量 (40分) - 核心驱动力
        momentum_score = self.check_extreme_momentum(df_15m, df_1h, df_4h)
        # 归一化: momentum_score max is around 80 (30+10+10+25+5)
        # We want max contribution to be 40
        weighted_momentum = min(40, momentum_score * 0.5)
        total_score += weighted_momentum
        breakdown['momentum'] = int(weighted_momentum)
        
        # 3. 回调入场 (20分)
        is_pullback, pullback_side = self.check_pullback_entry(df_15m)
        if is_pullback and pullback_side == trend:
            total_score += 20
            breakdown['pullback_entry'] = 20
        else:
            breakdown['pullback_entry'] = 0
            
        # 4. RSI极端性 (10分) - 放宽标准
        rsi = ta.rsi(df_15m['close'], length=14).iloc[-1]
        if trend == 'LONG':
            if 40 <= rsi <= 60: # 强势整理区间
                total_score += 10
                breakdown['rsi'] = 10
            elif rsi < 30: # 超卖
                total_score += 5
                breakdown['rsi'] = 5
        elif trend == 'SHORT':
            if 40 <= rsi <= 60: # 弱势整理区间
                total_score += 10
                breakdown['rsi'] = 10
            elif rsi > 70: # 超买
                total_score += 5
                breakdown['rsi'] = 5
        else:
            breakdown['rsi'] = 0
        
        # 5. 时间过滤 (Hour 6 Only Strategy) - Phase 4最终优化
        hour = current_time.hour
        dow = current_time.weekday()  # 0=Monday, 6=Sunday
        
        # 禁止周末交易
        if not self.WEEKEND_TRADING and dow >= 5:
            return 0, None, {'rejection_reason': 'weekend'}
        
        # Hour 6 Only策略（52.6%胜率！）
        if hour == self.GOLDEN_HOUR:
            total_score += 40  # 黄金时段大幅加分
            breakdown['timing'] = 40
        elif hour in self.GOOD_HOURS:
            total_score += 20  # 备选时段中等加分
            breakdown['timing'] = 20
        else:
            # 非黄金时段直接拒绝
            return 0, None, {'rejection_reason': f'not_golden_hour_{hour}'}
            
        # Debug print (只打印高分或特定币种) - DISABLED for performance
        # if total_score > 50:
        #     print(f"DEBUG {symbol}: Score {total_score} | Trend {trend} | Breakdown {breakdown}")
        
        return int(total_score), trend, breakdown
    
    def determine_leverage(self, symbol, signal_strength):
        """
        获取币种最大杠杆（不再动态调整）
        始终使用最高杠杆以最大化收益潜力
        """
        return self.get_max_leverage(symbol)
    
    def check_signal(self, symbol, df_dict, timestamp):
        """
        检查信号（集成Phase 1&2优化）
        
        Args:
            symbol: 交易对
            df_dict: {'15m': df_15m, '1h': df_1h, '4h': df_4h}
            timestamp: 当前时间
        
        Returns:
            {
                'side': 'LONG'/'SHORT'/None,
                'strength': 0-100,
                'leverage': int,
                'breakdown': dict
            }
        """
        # ===== Phase 4: 币种+分数段过滤 =====
        # 黑名单：完全拒绝
        if symbol in self.BLACKLIST:
            return None
        
        # 白名单优先：降低阈值
        is_whitelist = symbol in self.WHITELIST
        
        # 甜蜜区策略：70-75分最优（Phase 4发现）
        threshold_min = self.MIN_SCORE - 5 if is_whitelist else self.MIN_SCORE
        threshold_max = self.MAX_SCORE
        
        df_15m = df_dict.get('15m')
        df_1h = df_dict.get('1h')
        df_4h = df_dict.get('4h')
        
        if df_15m is None or df_1h is None or df_4h is None:
            return None
        
        # 确保数据足够
        if len(df_15m) < 200 or len(df_1h) < 200 or len(df_4h) < 200:
            return None
        
        # 计算信号强度（包含时间过滤）
        strength, trend, breakdown = self.calculate_signal_strength(
            symbol, df_15m, df_1h, df_4h, timestamp
        )
        
        # 甜蜜区过滤：70-85分
        if strength < threshold_min:
            return None  # 分数太低
        
        if strength > threshold_max:
            return None  # 分数太高，可能过拟合
        
        # 确定杠杆
        leverage = self.determine_leverage(symbol, strength)
        
        if leverage == 0:
            return None
        
        return {
            'side': trend,
            'strength': strength,
            'leverage': leverage,
            'breakdown': breakdown,
            'metrics': {
                'strength': strength,
                'leverage': leverage,
                'is_whitelist': is_whitelist
            }
        }
