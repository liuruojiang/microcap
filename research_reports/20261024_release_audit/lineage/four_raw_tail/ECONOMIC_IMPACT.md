# 四只实际持仓缺失 2026-10-23 收盘价：真实经济差异

结论：这是实际输入完整性问题。旧 full-pool 候选确实把四只持仓的缺失当日收益计为 0，补齐真实 raw 后会改变批准的当日 proxy、base costed、v2.0 和 v2.3 NAV。不能以旧输出字节守恒或 base-state-only 校验通过认定经济结果正确。本报告及候选文件均 `approved:false`，未修改主目录正式数据或 authority。

实际导入了 `D:/t/stcand20261024_a/r/microcap_top100_mom16_biweekly_live_v2_0.py`，其 full-pool 入口调用 `freq_mod.load_cache_panels → load_symbol_cache`，取该根 `.microcap_index_cache/prices_raw`；共享 raw/qfq 路径均为空，不使用 `.autobuild_top100_cache/prices`。四只均无 qfq。使用已重放的实际有效 100 成员、原生 loader，逐只读取 10/23 return：恰好四只为 NaN；原生模拟器 `fillna(0.0).mean()` 得到 **0.01897559571469154**，精确复现原候选 proxy。故不是仅 transport 成员清单的推测。

| 股票 | 实际成员 rank | 10/22 raw | 新 10/23 raw | 原计入收益 | 真实新收益 |
|---|---:|---:|---:|---:|---:|
| 301512 | 16 | 27.89 | 28.89 | 0 | 3.5855145213% |
| 688067 | 17 | 20.99 | 21.64 | 0 | 3.0967127203% |
| 688096 | 68 | 7.32 | 6.94 | 0 | -5.1912568306% |
| 688251 | 59 | 17.12 | 17.40 | 0 | 1.6355140187% |

新 raw 来自另一 Agent 已真实抓取的 `D:/t/m24release`。本审计逐文件验证旧全部行与新文件前缀逐值完全相同，只有新增 10/23 行；记录新旧 SHA。来源网络抓取证据由该 Agent 保存，本脚本禁止联网、禁止写入证据目录以外的文件。

100 成员新平均收益 **0.019288244157668274**，增加 **0.000312648442976734**，即 **3.12648443 bp**。proxy close 从 **210707.8492821961** 变为 **210772.49997773592**。

## 真实下游重算

先用原批准 proxy、turnover、panel 和当前真实源链做控制：base costed 与三版结果全部精确复现原批准原始文件 SHA。然后只替换 proxy 的最后一行，保留截至 10/22 的全部原始字节，再执行真实 base 成本模型及双 warm-up 的三版原生计算。

| 流 | 10/23 return_net 原值 → 候选 | 最终 nav_net 原值 → 候选 |
|---|---|---|
| base costed | -0.0002700091023803 → 0.00004263934059634 | 30.770363122718614 → 30.77998602711054 |
| v2.0 | -0.0002700091023803 → 0.00004263934059634 | 29.733344700139845 → 29.742643294770023 |
| v2.3 | -0.0032691990750732 → -0.0029574885774255 | 30.08269755241326 → 30.09210540116836 |
| v2.5 | 0 → 0 | 99.66965441268805 → 99.66965441268805 |

四流截至 10/22 **逐值完全一致且原始行字节完全一致**。仅 10/23 行改变；持仓、动作、规模列均不变。v2.5 当日现金收益/NAV 不变，但其市场特征列仍改变，因此不能说 v2.5 整个 CSV 不变。逐列变化、四新 CSV 身份见 `native_stream_impact.json`；正式候选文件在 `native_streams_exact_prefix/`。早期 `native_streams/` 是定位阶段重序列化 proxy 的中间产物，正式精确前缀结论只引用 `native_streams_exact_prefix/`。

## 原生门禁与最小更正路径

三版真实原生 rewrite guard 分别按 95/60/40 行默认尾窗及逐列例外执行，均直接通过，属于允许的普通尾部修正，不触发 exact-hash migration report 分支。base 成本重算 helper 本身不含该历史 rewrite guard。**这不等于现有 authority 可以原样沿用或自行改 hash**：现有六 seed 绑定了 10/23 的 proxy/base costed，二者确实改变，仍需要新精确审查及批准绑定。

现有 no-new-rebalance tail extension 在 `current_end >= target` 且非全平价 placeholder 时直接拒绝；本次是部分成员缺价，整行 proxy 有非零收益，所以单纯补缓存再走追加不会自动纠正。最小范围是：冻结当前有效 100 成员和未变 turnover，用原生收益逻辑重算 10/23 单行，严格保留此前 proxy 字节，再运行原生成本重算和三版正式生成；不需要全股票池重排、ST 元数据更改或放宽 guard。

## 阻止同类 whole-delivery 假阳性

既有 `state.validate_member_cache_transport` 已能发现此缺口：旧真实候选精确报四条 raw 停在 10/22，新真实 100 缓存返回空错误。旧 whole-delivery 在完整发布路径仅用 `state.validate_state` 的覆盖警告，直到 pack 才执行 hard gate，因而存在遗漏。

按授权，在 `validate_base_state_for_session` 和 `inspect_outputs` 两处复用现成 hard gate：前者拦截 refresh-all 生成前/后及 check；后者避免直接 whole-workspace inspect 和短期日期证明复用报假成功。`state.validate_state` 的 base-only 可读语义、历史停牌 `fillna(0)` 规则均未改变。修门禁前的完整回归成功不能充当新完整性合同下的旧缓存验收。

该 gate 证明所需缓存当时齐备，并不重新验证缓存价格与既有 proxy/NAV 的经济同一性。先补 raw 却保留旧 proxy/NAV，单独 transport 校验仍会通过；因此本次必须完成新经济候选、精确批准绑定和正式生成，不能把新缓存正控误报为旧 NAV 正确或新 whole-delivery 已完成。
