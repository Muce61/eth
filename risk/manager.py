import pandas_ta as ta
from config.settings import Config

class RiskManager:
    def __init__(self):
        self.config = Config()

    def calculate_position_size(self, account_balance, entry_price, stop_loss_price):
        """
        Calculate position size based on Fixed Risk Model.
        Risk Amount = Account Balance * RISK_PER_TRADE (e.g. 2%)
        Quantity = Risk Amount / (Entry Price - Stop Loss Price)
        """
        if entry_price == 0 or account_balance == 0 or entry_price <= stop_loss_price:
            return 0
            
        # 1. Calculate Risk Amount (How much we are willing to lose)
        risk_amount = account_balance * self.config.RISK_PER_TRADE
        
        # 2. Calculate Risk Per Unit
        risk_per_unit = entry_price - stop_loss_price
        
        # 3. Calculate Quantity
        quantity = risk_amount / risk_per_unit
        
        # 4. Check Leverage Limit
        # Max Position Value = Balance * Max Leverage
        max_position_value = account_balance * self.config.LEVERAGE
        current_position_value = quantity * entry_price
        
        if current_position_value > max_position_value:
            # Cap quantity to max leverage
            quantity = max_position_value / entry_price
            print(f"⚠️ Position capped by leverage {self.config.LEVERAGE}x")
            
        print(f"仓位计算: 余额={account_balance:.2f}, 风险金额({self.config.RISK_PER_TRADE*100}%)={risk_amount:.2f}, 止损距离={risk_per_unit:.4f}, 数量={quantity:.6f}")
        
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
