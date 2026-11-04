# 共享打包与跨午夜证明独立审计

审计对象：`193d9309fcfa0ac365d3016b5c804ff07190ecd7` 共享直接打包，以及 `a70ee78e` 跨午夜 close-confirmed proof。只读生产输入；反例只使用 pytest 临时目录和现有100成员隔离夹具。不刷新行情、不执行整包全历史、不发送邮件、不修改生产源码。

## P2：成功归档未将所有缓存绑定到已通过语义验证的内容

- 位置：`scripts/top100_cloud_delivery.py:129`、`:136`、`:146`；`scripts/realtime_state_bundle.py:1005-1034`。
- 触发：共享根目录的独立缓存写入者，在 `pack_state` 的语义验证之后、归档文件清单和哈希生成之前短暂改变原始价格缓存，并在最后 `inspect_outputs` 前恢复。共享 delivery 锁只约束遵守它的入口；例如价格/复权存储并不读取 `top100_delivery.lock`。这是并发变更故障注入，不是宣称当前正式包已损坏。
- 原因：语义验证早于归档清单哈希。归档后检查只把 `report.inputs/artifacts` 同 ZIP 比较；其中没有 `.microcap_index_cache/prices_raw/000001.csv` 等完整缓存。最后检查观察的是已恢复的源目录。
- 已运行反例：`python -B -m pytest -p no:cacheprovider research_reports/20261103_adversarial_audit/test_pack_cache_race.py -q -s`，`1 passed in 4.48s`。该测试先证明真实成员缓存运输检查通过，随后在文件枚举时将000001缓存改为2020结尾，在归档完成后恢复原值。`cloud.pack` 返回 `ok=True`，ZIP自身哈希校验通过，但读取ZIP中该缓存再执行真实 `validate_member_cache_transport`，得到 `before anchor 2026-09-03`。
- 对照：首次故障注入让损坏持续保留，最后 `inspect_outputs` 正确拒绝。这确认漏洞只在瞬时替换/恢复场景，不能扩大为任意损坏都会放行。
- 影响：可能把无法通过接收端完整语义检查的包标为成功，影响后续本地/云端恢复可靠性。接收端另做语义验证，仍会 fail-closed；本反例没有证明错误持仓或历史收益被正式发布。正常单写者云端流程降低触发概率，故评级P2。
- 修复方向：以同一个不可变归档输入身份绑定语义验证与归档，并确保所有实际缓存写入口遵守同一写入协议。不要直接追加一轮全历史重建来掩盖竞态；可以复用本次归档清单/哈希，但需使其覆盖实际通过语义验证的输入，而不是验证结束后才生成未绑定清单。

## 验证有效的边界

- 共享路径用 exclusive lock；已存在锁时拒绝；正常异常路径释放自己创建的锁。
- 临时 ZIP 完成后才 `os.replace`，已发布目标包在失败时保留。既有最终CSV瞬时变化测试覆盖 report 中的 artifacts。
- 原有云端回归1073项通过是软件基线证据，不等于上面缓存竞态已覆盖。
- 跨午夜 proof 八项测试独立运行：`8 passed in 0.49s`。源代码会独立核对最新完成交易日、真实 proof 日期/UTC时间、收盘时间和未来时间；未发现该修订额外放宽实时锚点或把旧证明改写成今日证明。
- 完整历史经济口径、实际当前包恢复及邮件发送不属于本子审计，不作通过声明。

## 修复完成（待主 Agent 汇总提交）

- 共享整包路径在语义验证之前捕获既有 ZIP 清单及 payload 哈希；语义验证仍只执行一次，写归档时复用这份清单。归档后原有完整性检查将实际写入的缓存字节与验证前身份比较，覆盖不在 `delivery.input_hashes` 中的原始价格、股本、复权及其来源缓存。
- 未追加全历史语义验证、未增加 payload 全量哈希遍历次数，未加入跳过门槛选项。普通基础包路径保持原验证顺序，避免改变缺失输入的诊断接口。
- 新增 `test_shared_pack_rejects_transient_cache_change_after_validation`，分别对原始价格/股本缓存做相同长度的瞬时替换；归档结束即恢复源，仍必须拒绝，并断言旧目标包不变、锁释放。
- 检查：`python -B -m pytest -p no:cacheprovider tests/test_top100_cloud_delivery.py tests/test_close_confirmed_midnight_proof.py tests/test_full_rebalance_cache_bundle.py tests/test_whole_delivery_member_cache_gate_20261024.py -q`，**43 passed in 31.90s**。唯一 warning 是故意生成重复 ZIP member 的原有篡改测试。
- `git diff --check` 通过。修复只涉及 `scripts/realtime_state_bundle.py`、`scripts/top100_cloud_delivery.py`、`tests/test_top100_cloud_delivery.py`；未提交、推送、改生产输出、刷新行情或运行实包测速。完整修复前源码位于现有 Git 提交，可回退。
