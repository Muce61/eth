# 基于Phase 4分析的推荐修复方案

THRESHOLD = 80  # 下限
THRESHOLD_MAX = 85  # 上限，拒绝过高分数
BEST_HOURS = [6, 9]  # 只保留最优时段
# 降低趋势权重
trend_weight = 20  # 从30降至20
momentum_weight = 30  # 从40降至30
# 增加实证有效指标权重
time_weight = 25  # 时间最重要