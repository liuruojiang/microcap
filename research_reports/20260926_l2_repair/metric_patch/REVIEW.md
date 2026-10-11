# L2 指标口径候选修复：精确文件身份与验证

**状态：候选补丁已实跑验证，正式源码与 `outputs/` 均保持原状。** 本目录的 [`metric_fix_candidate.patch`](metric_fix_candidate.patch) SHA-256 为 `4c555c8c966d1017c90eeabbe0906b4841f554ae47a502370d86c167c34bbf3f`；当前工作区 `git apply --check` 通过。变更仅针对三版年化收益日分母、v2.5 初始权益回撤锚点及三个 v2.5 扫描脚本的相同指标公式，含一处既有回归测试的稀疏输入改为逐交易日输入。新增针对性测试备份见 [`metric_contract_cases.py`](metric_contract_cases.py)。

| 源文件 | 原 SHA-256 | 候选修改后 SHA-256 |
| --- | --- | --- |
| `microcap_top100_mom16_biweekly_live_v2_0.py` | `4e20dea7fb79ccf1658e549ec881ff931d26fb9c1156badc5bf9f7aa5ea5bfb3` | `62e59375ff2bf773b5d244ed9a09bfb850a776b816234b3cc0e28689921977ba` |
| `microcap_top100_mom16_biweekly_live_v2_3.py` | `323fef0c3843007762fe650a8f0942b879c549fc8ae0a43c472b609bd306ce13` | `ce49dee8cc9d42f63ff4cca4f3bd7744c1331dfa62b54974756af1d0f63345aa` |
| `microcap_top100_mom16_biweekly_live_v2_5.py` | `68c109ac268779f2d0afb4350e109e596063c9e6c11093922ef6105ab4c2636e` | `b5b90ed35dc06427050c6ba0b56535a7b9ace62242d3d8e9f139f25b754d9acc` |
| `scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py` | `0d79c4483a6c691c993e84e18421b4dc74f55173c125f2cdd47922444d08e96a` | `a2fb7e1b08703e83fa3613924e2d7d00a08be68bdf193a18ff805f13cd737457` |
| `scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py` | `2624f151b5921c9263eadd3926626e16a5b5dad2ea722c2d8392f7b9500dbad3` | `9c089cc376bfdaef924c4b351fcf4acbf35d75b55965f16a83c1a7a2c2ed9f2d` |
| `scripts/run_microcap_v2_5_bias_overheat_scan.py` | `7d71aee87453b7adcb70d87198065233da98f51e24a049d45febb4a97c1e83bb` | `66591bd14f0884a3e75c152c14526dadad16c01d9b8dde04db08fa720321ba60` |

原件备份在 [`../backups`](../backups)，逐一复制后哈希相符；测试完成后，以上六个正式文件和修改过的既有测试文件又从该备份恢复，当前 `git status` 对这些文件为空。补丁尚未用于正式输出或历史迁移。

## 为什么需要这项修复

- 正式 `summarize_returns()` / `summarize_yearly()` 把首末自然日差除以 `365.25`，而正式日收益流从初始权益 1 连乘；v2.0 的首个收益日已扣入场费。手册的该口径应按收益日数 `N/244` 年化。旧冻结 v2.0 流上，两种展示年化差约 **0.07737 个百分点**；这不是修复后的策略绩效。
- v2.5 的回撤公式未将初始权益 1 作为高水位。冻结旧收益流的 **2016 年度** MDD 由旧式 `-14.76873257%` 变为锚定后的 `-15.88288293%`，相差 **-1.11415037 个百分点**；全样本和 10Y/5Y/3Y/1Y 窗在该旧流中不变。独立原始收益复算见 [`../other_versions/REPLAY_REVIEW.md`](../other_versions/REPLAY_REVIEW.md)。
- 三个 v2.5 扫描脚本有独立复制的年化和回撤公式。若只改正式函数，扫描的 `ann_return` / `max_dd` 会与正式口径不一致；候选补丁同步修改这三个消费者。既有测试用跨年三行的稀疏序列验证“自然日口径”，不符合逐交易日日收益输入合同，已在候选中改为三个连续工作日。对真实大缺口的输入仍应另行标记不可比较，本补丁没有用稀疏序列产生任何绩效。

## 验证和边界

补丁临时应用在本地代码上时，运行：

```powershell
python -X utf8 -m pytest -q tests/test_top100_l2_metric_contract.py tests/test_adversarial_v20_plain_20260904.py tests/test_adversarial_v23_plain_20260904.py tests/test_v2_5_scan_integrity.py
```

结果 **59 passed**；`git diff --check` 通过。`test_top100_l2_metric_contract.py` 现保存在本目录的 `metric_contract_cases.py`，补丁本身没有添加该新测试文件，正式采纳时应一并放入 `tests/`。这些检查只证明指标函数及相邻扫描消费者的定义修复；旧收益流的成员、公司权益、时钟与账户仍未通过 L1/L2。根据项目精确哈希迁移门槛，尚未刷新正式报告、NAV 或交付清单；也不得将修正后的旧流指标当作可发布正式业绩。
