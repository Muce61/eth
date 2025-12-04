"""
6个月回测脚本 (6-Month Backtest)

目标: 验证策略在 2025年5月 - 10月 (6个月) 的长期表现
时间范围: 2025-05-01 ~ 2025-10-31
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from backtest.real_engine import RealBacktestEngine
from utils.backtest_metrics import calculate_comprehensive_metrics

def main():
    print("="*80)
    print("长期压力测试: 2025年5月 - 10月 (6个月)")
    print("策略配置: SmartExit + QualityFilter + LossPatternFilter + FixedRisk(2%)")
    print("="*80)
    print()
    
    # 初始化回测引擎
    engine = RealBacktestEngine(initial_balance=100)
    
    # 运行回测 (指定日期范围)
    print("正在运行6个月回测 (可能需要几分钟)...")
    engine.run(start_date='2025-05-01', end_date='2025-10-31')
    
    # 计算指标
    metrics = calculate_comprehensive_metrics(
        engine.trades, 
        engine.initial_balance, 
        engine.balance
    )
    
    # 打印结果
    print("\n" + "="*60)
    print("6个月回测结果 (6-Month Results)")
    print("="*60)
    print(f"📊 总收益率: {metrics['total_return_pct']:.2f}%")
    print(f"📈 交易数量: {metrics['total_trades']}")
    print(f"✅ 盈利笔数: {metrics['winning_trades']}")
    print(f"❌ 亏损笔数: {metrics['losing_trades']}")
    print(f"🎯 胜率: {metrics['win_rate']:.2f}%")
    print(f"💰 平均利润率: {metrics['avg_profit_pct']:.2f}%")
    print(f"⚖️  盈亏比: {metrics['profit_factor']:.2f}")
    print(f"📉 最大回撤: {metrics['max_drawdown']:.2f}%")
    print("="*60)
    
    # 保存交易记录
    trades_df = pd.DataFrame(engine.trades)
    if not trades_df.empty:
        output_path = 'backtest_results/csv/backtest_trades_6months.csv'
        # Ensure directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        trades_df.to_csv(output_path, index=False)
        print(f"交易记录已保存至 {output_path}")

if __name__ == "__main__":
    main()
