# Return-basis完整指纹I/O等价优化（2026-11-04）

仅调整 `scripts/realtime_state_bundle.py::_return_basis_input_fingerprint` 的文件访问方式。原生完整语义重建、股票18文件、BASE、策略参数、成员/ST、authority、经济数据、日历与15:00边界均未改变。

- root只规范为extended path一次；使用 `os.scandir` 的原生lstat/find-data识别条目。固定文件全部祖先仍检查，待进入子目录再次lstat；symlink/junction及未知reparse/special条目均拒绝复用。
- 保留完整前后目录库存及原sorted(relative,kind)顺序；每个实际文件仍逐字节SHA-256，不用mtime/size替代内容。
- 树文件直接stream，不逐文件重复Path.resolve；固定文件与5个模块继续原hash helper。目标、完整symbols、正式calendar、模块字节和实际函数对象ID、锁状态与所有失败语义保持。
- 使用最多4线程、8个在途文件任务；严格按原库存顺序收摘要，不按完成次序。异常退出等待全部自有读取任务，不遗留后台reader。不生成磁盘成功证书、不序列化memo。

## 真实只读对照

对实际生产状态2026-11-03、完整5438符号，只执行指纹函数，未执行semantic verifier、历史builder、刷新或写入。每次对照均在同一个Python进程、同一批模块/函数对象内比较，起止6份固定文件与候选源码字节哈希一致。

| 独立进程内的对照 | 原/串行耗时 | 优化耗时 | 热盘控制/结论 |
|---|---:|---:|---|
| 原30a → scandir串行 → 原30a | 170.971秒 | 64.478秒 | 末次原函数191.474秒；三次digest及unlocked完全相同 |
| scandir串行c58 → bounded4reader | 107.748秒 | 46.904秒 | 两次digest及unlocked完全相同 |

函数ID本来就参与指纹，因此两个独立进程的digest不可跨进程直接比较；上述相等结论分别只适用于各自同进程对照。运行中主机另有正式验证负载，耗时不是稳定性能保证。原始记录在本次任务 `audit_gates/fingerprint_io_actual_root_benchmark.json` 与 `fingerprint_io_parallel_actual_root_benchmark.json`，旧串行候选commit为 `c58c1cf0da825b8f904c6f85a0d109df52fd7980`。

## 验证范围

原每日core30已包含的 `tests/test_realtime_state_guard_20260915.py` 追加17例，保留原94例及原断言：原函数可执行oracle同进程精确比较、真实长路径/Unicode/symlink、固定祖先、目录重定向、未知reparse、空目录、锁、同size恢复mtime后字节变化、函数ID、读取中新增文件、每文件恰一次/4线程8任务上界、乱序完成和异常join。完整文件111项在180秒硬限内通过，零失败/错误/跳过，源码测试起止字节一致。

这里只证明完整指纹I/O等价及该子阶段耗时改善。metadata/ST原文闭包仍按原合同每次真实检查；是否足以让14:30当次完整软件门禁、热readiness、三版正式查询及实际发布在15:00前完成，必须由后续真实全流程计时验收，不能据本页先宣称盘中交付就绪。
