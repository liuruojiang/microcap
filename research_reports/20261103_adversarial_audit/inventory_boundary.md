# 预捕获清单的新增路径边界复核

结论：主审提出的遗漏场景成立。未修改运行时代码，生产刷新未触碰。

运行 `python -B -m pytest -p no:cacheprovider research_reports/20261103_adversarial_audit/test_pack_inventory_race.py -q -s`：**1 passed in 6.25s**。

隔离100成员夹具中，在cloud首次inspect通过后、state.pack_state预捕获库存前移除raw000001；进入语义验证前恢复。真实member-cache检查通过、源文件完整；cloud.pack返回ok，ZIP自身manifest校验通过，但raw000001未在ZIP中。这证明只把payload哈希前移不足以证明库存完整性。

最小修复建议（未实施）：共享归档完成后，在原archive验证附近用只读路径枚举重新核对当前应运输的路径是否都在捕获清单。至少覆盖 `REQUIRED_FILES`、`OPTIONAL_GLOBS`、当前effective-members文件和 `_iter_current_member_cache_files(root)`（包含全历史universe）。此枚举只读目录、成员表和is_file，不重读payload哈希，不重跑全历史语义。凡新出现的应运输路径不在captured names中即拒绝；已有捕获文件内容仍由归档前哈希及ZIP实际字节校验处理。

不要直接再次调用当前 `_iter_bundle_files` 并称其“纯库存”：它还调用 `verify_share_cache`、检查share来源payload SHA、收集ST来源闭包。若采用统一names函数，需明确拆出纯库存收集与来源语义验证，或仅对必需/可枚举候选做缺项检查；不能暗中增加一轮来源校验。

正式回归可直接使用本反例，改为要求cloud.pack拒绝、旧目标包保留、锁释放。生产refresh结束后再由主审决定并实施；此子任务已停止写入源码。

## 最小修复已实现

按主审后续授权，改动限制在pack路径：在 `top100_cloud_delivery.py` 新增 `_shared_pack_inventory`，仅用已定义required/glob/effective-member以及成员+ranking-universe缓存路径生成库存，采用现有Windows extended-root转换。归档后要求库存未变化且所有当前应运输路径均在ZIP中。它不调用带来源验证的 `_iter_bundle_files`，不新增payload哈希或全历史语义校验。

正式回归加入raw/share两类缓存，各覆盖当前成员000001和仅ranking-universe的999999，共4种缺失后恢复场景，要求拒绝且旧包不变、锁释放。

验证：完整cloud测试（前2种新场景）21 passed in26.86s；新增universe两例2 passed in4.61s；最终Windows路径正规化后仅复核全部4项库存反例，4 passed in7.98s。日志 `pack-inventory-repair-tests.log`、`pack-inventory-universe-tests.log`、`pack-inventory-final-tests.log`。没有重跑行情或修改生产输出，没有commit/push。最终23项cloud测试均有适用通过证据，未为最后一行路径正规化额外全组重跑。
