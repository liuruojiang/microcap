# V5.8 资产池优化修订说明

**修订日期**: 2026-03-21
**涉及文件**: `strategy_signal_v5.8.py` (信号机器人) / `trade_journal_v5.8.py` (交易日志机器人)

---

## 一、修订总览

基于回测数据的系统性分析（动量排名相关性、资产池冗余度、因子暴露测试），对 Sub-A 和 Sub-B 两个子策略的资产池进行精简优化。Sub-A-DK 多空策略和 Sub-C 组合策略保持不变。

| 子策略 | 变更内容 | 预期影响 |
|--------|---------|---------|
| **Sub-A** | 沪深300 → 上证50 | Sharpe +0.06~0.09 |
| **Sub-B** | 移除 VOO(SPY)、SCHH(VNQ) | Sharpe +0.08~0.10 |
| Sub-A-DK | 无变更 | — |
| Sub-C | 无变更 | — |

---

## 二、Sub-A：A股轮动 — 沪深300替换为上证50

### 2.1 变更理由

| 指标 | 沪深300 | 上证50 |
|------|---------|--------|
| 与中证1000排名相关性 | -0.546 | **-0.626** |
| Top-1被选频率 | 0.3% | **5.7%** |
| 收益率相关性（vs 沪深300） | — | 0.942（高度相似） |

- **上证50** 与中证1000的**动量排名负相关**更强 (-0.626 vs -0.546)，大小盘轮动信号更清晰
- Top-1 选频 5.7% vs 沪深300 的 0.3%，实际参与轮动的机会显著增加
- 4/8/12 年回测期 Sharpe 均稳定提升 +0.06~0.09

### 2.2 代码变更

**轮动池定义** — `CN_EQUITY_CODES`

```
之前: ["1.515100", "0.159915", "1.000300", "1.000852", "1.000905"]
之后: ["1.515100", "0.159915", "1.000016", "1.000852", "1.000905"]
                                 ^^^^^^^^
                                 上证50替换沪深300
```

**数据获取列表** — `CN_STOCK_CODES`
```python
CN_STOCK_CODES = CN_EQUITY_CODES + ["1.000300"]
# 沪深300保留在fetch列表中，供Sub-A-DK多空策略使用
```

**名称映射** — `CN_NAMES`
```python
新增: "1.000016": "上证50" / "SZ50"
保留: "1.000300": "沪深300" / "HS300"  # DK策略仍使用
```

**持仓归一化** — `_CN_HOLDING_NORM`
```python
新增: "sz50": "1.000016", "上证50": "1.000016", "50": "1.000016"
```

### 2.3 未变更项

- **Sub-A-DK 多空策略**：仍使用全部 C(5,2)=10 个价差对（含 500/1000、50/300），这些对虽然被选频率极低（合计1.6%），但移除后 Sharpe 下降 0.04~0.06
- **沪深300数据**：仍保留在 `CN_STOCK_CODES` 获取列表中，因 DK 策略中 50/300 价差对需要沪深300数据

---

## 三、Sub-B：美股轮动 — 移除 VOO 和 SCHH

### 3.1 变更理由

| 移除标的 | 代理 | 移除原因 |
|---------|------|---------|
| **VOO** | SPY | 与QQQM(QQQ)排名相关性 0.818，高度冗余；QQQ在回测中表现更优 |
| **SCHH** | VNQ | REITs长期Sharpe仅0.36，且与SPY排名相关性0.484，提供的分散化收益有限 |

- 移除后 Sharpe 提升 +0.08~0.10（4/8/12年回测一致）
- 资产池从 9 ETF 精简至 **7 ETF**，降低了冗余度

### 3.2 资产池对比

```
之前 (9 ETF):                      之后 (7 ETF):
  VOO  (SPY)   - S&P 500           ❌ 移除
  QQQM (QQQ)   - Nasdaq 100        ✅ 保留
  EMXC         - 新兴市场(除中国)    ✅ 保留
  VEA  (EFA)   - 发达市场           ✅ 保留
  GLDM (GLD)   - 黄金              ✅ 保留
  VGLT (TLT)   - 长期国债           ✅ 保留
  SCHH (VNQ)   - REITs             ❌ 移除
  PDBC (DBC)   - 大宗商品           ✅ 保留
  IBIT (BTC)   - 比特币             ✅ 保留
```

### 3.3 代码变更

**轮动资产** — `US_ROT_ASSETS`
```python
# 移除两行:
#   "VOO":  {"proxy": "SPY",  "label": "S&P 500"},
#   "SCHH": {"proxy": "VNQ",  "label": "REITs"},
```

**期货对冲集合** — `US_ROT_FUTURES`
```
之前: {"SPY", "QQQ", "GLD", "TLT"}
之后: {"QQQ", "GLD", "TLT"}
```

**SPY 数据保留 (VolReg 风控)** — `US_ALL_TICKERS` & `_fetch_data`
```python
# US_ALL_TICKERS 中显式保留 SPY:
US_ALL_TICKERS = sorted(set(
    US_ROT_POOL + ["BIL", "SPY", ...] + ...  # SPY: VolReg风控仍需要
))

# _fetch_data 中注入SPY列到 us_rot_close:
if "SPY" not in us_rot_close.columns and "SPY" in us_raw:
    us_rot_close["SPY"] = us_raw["SPY"]["close"].reindex(us_rot_close.index)
```
> VolReg 风控通过 SPY 短期/长期波动率比判断风险状态，即使 SPY 不在轮动池中，其数据仍必须可用。

**界面文本更新**
- 所有 "美股9ETF" → "美股7ETF"
- 示例文本 "VOO 100股" → "QQQM 100股"
- 期货标的说明 "VOO/QQQM/GLDM/VGLT" → "QQQM/GLDM/VGLT"

---

## 四、未变更的子策略

### 4.1 Sub-A-DK (A股多空)
- 测试了移除 500/1000 和 50/300 两个价差对：被选频率仅 0.9% 和 0.7%，但移除后 Sharpe 下降 0.04~0.06
- **结论**：保留全部 10 个价差对不变

### 4.2 Sub-C (美股组合)
- 测试了将 VTI 30% + QQQM 10% 替换为因子ETF（VUG成长/VTV价值/IWM&IWN小盘）
- 8 种配置全部表现更差（Sharpe -0.05 ~ -0.18）
- IWM/IWN Sharpe 仅 0.28，远逊于 QQQ 的 0.74；VTI 已通过市值加权隐式覆盖各因子
- **结论**：维持 VTI + QQQM 组合不变

---

## 五、验证清单

- [x] `strategy_signal_v5.8.py` — Sub-A 轮动池: 上证50 替换沪深300
- [x] `strategy_signal_v5.8.py` — Sub-B 轮动池: 移除 VOO、SCHH
- [x] `strategy_signal_v5.8.py` — SPY 保留供 VolReg 使用
- [x] `strategy_signal_v5.8.py` — 所有界面文本更新
- [x] `trade_journal_v5.8.py` — Sub-A 定义同步 (CN_EQUITY_CODES / CN_STOCK_CODES / CN_NAMES)
- [x] `trade_journal_v5.8.py` — Sub-B 定义同步 (US_ROT_ASSETS / US_ROT_FUTURES)
- [x] `trade_journal_v5.8.py` — SPY 保留供 VolReg 使用
- [x] `trade_journal_v5.8.py` — _CN_HOLDING_NORM 新增上证50条目
- [x] `trade_journal_v5.8.py` — 所有界面文本更新
- [x] 全局搜索确认无残留 VOO/SCHH/VNQ/9ETF 引用
