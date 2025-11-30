import pandas_ta as ta
from config.settings import Config

class RiskManager:
    def __init__(self):
        self.config = Config()

    def calculate_position_size(self, account_balance, entry_price, stop_loss_price):
        """
        Calculate position size based on percentage of account balance.
        Margin = Account Balance * TRADE_MARGIN_PERCENT
        Quantity = (Margin * Leverage) / Entry Price
        """
        if entry_price == 0 or account_balance == 0:
            return 0
            
        # Calculate margin as percentage of balance
        margin = account_balance * self.config.TRADE_MARGIN_PERCENT
        
        # Check if balance is sufficient
        if margin <= 0:
            print(f"余额不足: 账户余额 {account_balance}, 计算保证金 {margin}")
            return 0
            
        position_value = margin * self.config.LEVERAGE
        quantity = position_value / entry_price
        
        print(f"仓位计算: 余额={account_balance:.2f}, 保证金({self.config.TRADE_MARGIN_PERCENT*100}%)={margin:.2f}, 杠杆={self.config.LEVERAGE}x, 仓位价值={position_value:.2f}, 数量={quantity:.6f}")
        
        return quantity

    def calculate_stop_loss(self, df, entry_price, side='LONG'):
        """
        Calculate ATR-based Stop Loss with hard cap for 50x leverage safety.
        """
        if len(df) < self.config.ATR_PERIOD:
            return entry_price * 0.98 # Fallback 2%
            
        # Calculate ATR
        atr = ta.atr(df['high'], df['low'], df['close'], length=self.config.ATR_PERIOD)
        current_atr = atr.iloc[-1]
        
        sl_distance = current_atr * self.config.ATR_MULTIPLIER
        
        # CRITICAL: Cap stop loss at 1.4% for 50x leverage (liquidation at ~1.5%)
        max_stop_distance = entry_price * self.config.STOP_LOSS_CAP_PERCENT
        sl_distance = min(sl_distance, max_stop_distance)
        
        if side == 'LONG':
            return entry_price - sl_distance
        else:
            return entry_price + sl_distance

    def check_trailing_stop(self, current_price, highest_price, entry_price, side='LONG'):
        """
        Check if trailing stop is triggered.
        """
        if side == 'LONG':
            # 1. Check Activation
            if highest_price >= entry_price * (1 + self.config.TRAILING_ACTIVATION):
                # 2. Check Callback
                stop_price = highest_price * (1 - self.config.TRAILING_CALLBACK)
                if current_price <= stop_price:
                    return True, stop_price
        
        return False, 0.0
