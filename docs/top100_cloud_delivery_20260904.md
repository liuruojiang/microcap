# 三版本收盘交付的跨日恢复

本改动不改变策略参数、历史谱系、执行时间或费用。合并既有本地 preflight/整体交付门禁，并让 GitHub 三个隔离目录的最终产物接受同一组校验。

云端在三个正式入口成功后调用：

```text
python scripts/top100_cloud_delivery.py pack --root ROOT20 --v23-root ROOT23 --v25-root ROOT25 --expected-date YYYY-MM-DD --bundle microcap-whole-delivery-state.zip
```

只接收相同核心代码、authority、底层输入，以及同日、正确版本、历史审计 clean 的三个最终成本净值、NAV 别名、信号与绩效产物。先在临时目录合并验证，再以现有逐文件 SHA-256 包格式保存；不复制或批准新代码/authority。GitHub 工作流的日期来自当天交付门禁，不能以旧日数据自我证明为今日。

本地下一交易日可调用：

```text
python scripts/top100_cloud_delivery.py sync --expected-date 上一已完成交易日
```

优先复用已通过整组清单验证的本地同日状态；缺失时仅从该仓库 main 的成功 GitHub 日报运行下载 `microcap-whole-delivery-state`。恢复前先在临时目录校验整个包、全部最终产物、日期与本地代码/authority 哈希；拒绝回退更晚本地状态，拒绝与整组刷新并发。每个覆盖文件先备份，备份路径在 JSON 结果中给出。恢复中断后清单为 blocked；旧日证明不改成今日证明。

恢复不代表当日盘中信号已获准发布。必须随后执行原有 `realtime_state_bundle.py preflight`（当前 100 成员当日报价名称/ST 检查和独立历史日期）及 validate，再执行 `top100_delivery.py check`，全部通过后才能运行官方实时入口。这里不得运行完整 refresh-all；完整刷新保留在收盘维护/GitHub路径。

失败包或旧日期不得生成“同步成功”或正常交付标记。缓存包不包含 Python 源码或冻结 authority，不自动部署代码，不接触交易接口。

回归：整包往返、三版本输入错位、日期错误、代码身份差异、回退更新状态、并发锁、任意附加字节篡改、跨日证明不被改写。首次全仓 251 passed，2 条警告来自故意构造重复 ZIP 成员的测试。实际云端运行与真实数据往返结果另存 automation 验收记录，不以本文件替代上线证明。
