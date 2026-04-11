# Changelog: Sub-A 添加波动率缩放 (v5.8)

## 日期: 2026-03-20

## 变更摘要
为 Sub-A (A股乖离动量轮动) 策略添加波动率缩放机制，与 ADK 策略的 vol-scaling 一致。

## 参数
| 参数 | 值 | 说明 |
|------|--------|------|
| CN_TARGET_VOL | 0.15 (15%) | 目标年化波动率 |
| CN_VOL_WINDOW | 60 | 波动率计算窗口（与MA60一致）|
| CN_MAX_LEV | 1.5 | 最大杠杆 |
| CN_MIN_LEV | 0.1 | 最小杠杆 |
| CN_SCALE_THRESHOLD | 0.10 | scale变动阈值 |

## 参数来源
- 5个参数全部通过过拟合测试（横向平原 + 纵向无衰减）
- VOL_WINDOW=60 是全局最优（后半段/近8年/近4年都最优），与策略本身MA60慢速特性一致
- MIN_LEV 和 MAX_LEV 为钝感参数（实际很少触及边界）

## 预期效果
- **4年 MDD**: -25.50% → -15.14% (改善40%)
- **12年 Sharpe**: 1.07 → 1.09 (略提升)
- **2024年9-10月**: 10月MDD从-25.5%降至-7.1%（波动率暴涨时自动降杠杆）
- **纵向稳定性**: 前半段Sharpe衰减从30%降至11%

## 关键设计
- **Cash日 scale=1.0**: 持现金时不缩放（无风险利率不应被杠杆影响）
- **权益日 scale=target_vol/realized_vol**: 低波时加杠杆、高波时减杠杆
- **shift(1)**: 用T-1信息计算T的杠杆，避免未来函数
- **阈值平滑**: |Δscale| < 0.10 时不调整，减少微调

## 修改文件

### strategy_signal_v5.8.py
1. **常量区** (line ~35): 新增 CN_TARGET_VOL, CN_VOL_WINDOW, CN_MAX_LEV, CN_MIN_LEV, CN_SCALE_THRESHOLD
2. **run_cn_strategy()**: 在DataFrame构建后、NAV计算前插入vol-scaling逻辑
3. **短报告显示**: 排名表前添加"波动率缩放"显示块
4. **详细报告显示**: 同上
5. **长报告显示**: ⑤防接刀后添加"⑥波动率缩放"表格
6. **参数文档**: Sub-A参数表增加5行vol-scaling参数，计算过程增加step 5

### trade_journal_v5.8.py
1. **常量区** (line ~33): 新增 CN_TARGET_VOL, CN_VOL_WINDOW, CN_MAX_LEV, CN_MIN_LEV, CN_SCALE_THRESHOLD
2. **run_cn_strategy()**: 同strategy_signal，插入vol-scaling逻辑
3. **持仓对比表**: Sub-A行下方增加"Sub-A杠杆"行显示当前scale和已实现波动率
