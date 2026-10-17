# 实现说明（M2）

本文记录 M2 实现与 `DESIGN.md`（含 §0.5 v0.2 修订）的偏差、补充的细节和原因，以及已知缺口。没有列出的部分按设计实现。

## 1. 与设计的偏差

| # | 设计原文 | 实现 | 原因 |
|---|---|---|---|
| 1 | §3 L1Record 的 `ParkedAt`、`Reason` 在记录级 | `Reason` 和 `FirstAt`（首次 Park 时间）放在每个 `DestSlot` 里；记录级保留 `ParkedAt`，另存事件的 `ReceivedAt` | 同一事件不同目的地的 Park 原因不同；孤儿 TTL 要按目的地计时；`max_age` 要按事件接收时间计算，不用回读 WAL |
| 2 | §4.2 L1 writer "≤5ms 或 1000 条" 组提交 | 单 writer 不设定时器：取到第一个操作后，把队列里已有的操作（最多 1000 个）一起放进一个 `db.Update` | 低负载时不增加延迟，高负载时自然成批；同样满足"单 writer、不用 `db.Batch`" |
| 3 | §4.4 redeliver 每轮预算 = `L2Free × redeliver_share` | **预留式份额**：目的地有 L1 积压时（L1 中有它的 pending，或 L2 里有重投项），新事件（live/replay）最多占 `⌊(1−share)·max_events⌋` 条和 `(1−share)·max_bytes` 字节；redeliver 可以把 L2 填满。无积压时新事件可以用满 L2。`share` 默认 0.5，配置校验要求 ≥ 0.2 | 每轮预算的做法在新事件持续涌入时仍可能让 redeliver 拿不到空间；预留式保证 redeliver 至少有 `share` 的容量，同时新事件也不会被饿死（redeliver 只在 tick 时补充） |
| 4 | §4.4 half-open 时"只放 1 条探测" | half-open 时 redeliver 和新事件都放行（§0.5 item 9）；每个目的地只有一个消费者，所以下一次 `Write` 天然就是探测，成功则关闭，失败立刻重新打开 | 与 §0.5 一致，且不需要额外的探测配额逻辑 |
| 5 | 熔断按"失败次数"计 | 按每次 `Sink.Write` 的结果计：批内有成功即算成功；无成功且有 Retryable 失败算失败；只有 NonRetryable/Throttled 算中性 | 一个批次对应一次网络往返，按批计数才对应"连续失败的请求" |
| 6 | §5 Draining "把 L2 处理完，到 deadline 后剩余的也 Park" | 按任务要求：在途批次走正常结果流程，L2 里剩余的**直接** Park（Unregister 用 `NotActive`，优雅关闭用 `Shutdown`）；deadline 到了会取消在途 `Write`，这批事件不计 attempt、改为 Park | §0.5 item 5 与 §4.10 的语义；避免关闭/注销被一个慢 sink 拖住 |
| 7 | 优雅关闭验收"dup=0" | 已实现：关闭后剩余 L2 Park、L1 flush、写最终 checkpoint；重启后 `replayed=0`，Park 的事件由 redeliver 精确投一次（`TestGracefulRestartNoDuplicates`） | — |
| 8 | §4.6 水位 | 实现为 `min(队头未解决 −1, dispatchCursor)`；**先读 cursor 再锁 tracker**，保证读到的 cursor 以下的 seq 都已 Add。checkpoint 写入 `max(旧值, 新值)`，永不回退 | 恢复时 cursor 从 `min(ckpt)` 起步，若直接写会让 ckpt 较大的目的地回退 |
| 9 | 设计未提 | **Claims**：`(seq, dest)` 级别的内存占用标记。redeliver 选中时 claim，worker 在 L1 Ack/Fail/Dead 提交后释放；sweeper 同样先 claim | 防止 sweeper 和 redeliver 同时处理同一对，造成"已投递又进 DLQ"或重复投递 |
| 10 | §4.8 GC 用 `maxSeq ≤ min(ckpt)` | 用**已持久化**的 ckpt（不是内存水位）；段的 maxSeq 取"下一段 base − 1"（保守上界）；只删非最新段 | 崩溃后恢复只看持久化 ckpt；段内可能有烧掉的 seq，用下一段 base 判断更安全 |
| 11 | registry `name → DestID / 版本 / 状态 / tombstone` | 持久化状态只有 `configured / absent / tombstone` 三种；生命周期状态（Registered/Active/Draining/Unregistered）只在内存。新 ID 按配置顺序分配（先 destinations，再仅出现在路由里的名字）；新分配 ID 的 ckpt 初始化为当前 WAL 最大 seq；tombstone 超过 `l1.tombstone_ttl` 且 L1 无 pending 时才释放 ID | 新 ID 的 ckpt 初始化保证旧 WAL 记录里同号的位（只可能来自很久以前被释放的同号目的地）不会被重放给它 |
| 12 | 路由 match（class_uid 等） | 先做"首个匹配生效"，match 为入站 JSON 顶层字段的等值比较（按规范化字符串比，YAML `3002` 能匹配 JSON `3002` 和 `"3002"`） | OCSF 归一化在 M4；任务允许先做简单相等 |
| 13 | 配置里路由引用未知目的地 = 错误（M1） | 允许，加载时告警，按名字预留 ID（registry 状态 `absent`），事件 Park（`NoDest`，不计 attempt） | §5 的统一策略 |
| 14 | §4.7 恢复顺序 | 1) 开 bbolt，载入 registry/ckpt，扫 l1 重建 segRef、各目的地 pending 数、L1 最大 seq；2) 开 WAL，`SeqFloor = max(全部 ckpt, L1 最大 seq)`，`ScanFrom = min(ckpt)`；3) reconcile registry；4) 启动各目的地消费者；5) 从 `min(ckpt)+1` 重放（每 1024 条批量查一次 L1 Owned）；6) 启动 redeliver、checkpoint、sweeper、GC；7) 开始消费 live 流；8) 打开 HTTP | redeliver 在重放**之后**启动，这样重放期间 L1 记录只会被重放本身新增，按块查询的 Owned 不会过期；仍满足"先启动 redeliver 再开放 ingest" |
| 15 | WAL 恢复扫描（M1 TODO） | `wal.Options.ScanFrom`：所有 seq 都 ≤ `ScanFrom` 的段不做 CRC 扫描，只取文件大小；最后一段总是扫描 | 这些记录不会被重放；被 L1 引用的记录每次 `ReadAt` 都会校验 CRC |
| 16 | 设计未提 | WAL 段读者引用计数：`ReadAt` 和迭代器打开段文件前先 pin，`RemoveSegment` 遇到仍被 pin 的段只做标记（新读者立即不可见），最后一个读者释放时再删除 | Windows 不能删除打开中的文件（REVIEW P2） |
| 17 | 设计未提 | WAL 里无法解码的记录：对每个目标写 DLQ（reason `undecodable`，body 为 base64 字符串），再 Resolve | 不能静默丢，也不能永远卡住水位 |
| 18 | §4.9 反压 | 只实现 L1 配额：`on_quota=block_all` 时任一目的地 pending ≥ `max_records_per_dest`，HTTP 返回 429 + `Retry-After: 1`；此时已 ack 的事件仍照常 Park（配额可能小幅超出）。`dlq_overflow` 时超额的新 Park 直接进 DLQ（reason `l1_quota_overflow`）| 已 ack 的事件不能丢，只能挡住新的；磁盘配额、ENOSPC 只读模式在 M5 |
| 19 | §8 Prometheus | 只有内部 `Stats()` 和 `GET /v1/stats`（JSON） | 任务明确 M2 不要求 Prometheus |
| 20 | §4.5 webhook 带 `Idempotency-Key` 头 | 批量模式（默认）：POST JSON 数组，每个元素带 `idem_key`，请求头 `Idempotency-Keys` 按数组顺序逗号分隔；`batch: false` 时每个事件一个请求，头为 `Idempotency-Key`。整批共享同一个响应分类 | 一个 HTTP 请求只能有一组头 |
| 21 | 未知错误按 Retryable | 实现如此；另外 503/408 的 `Retry-After` 也会被尊重（取 `max(Retry-After, 退避)`），但只有 429 算 Throttled（不计熔断失败） | — |

## 2. 关键不变量（无法跑 race detector 的补偿）

本机没有 gcc，`go test -race` 跑不了。并发正确性靠下列设计约束，并在 Linux CI 上补跑 race（M3 起）：

1. **只派发已 fsync 的记录**：Dispatcher 只读 WAL 的 Committed 流和恢复迭代器，二者都只覆盖 fsync 过的字节。
2. **Resolve 在持久化之后**：sink 成功后、L1 Park 提交回调里、DLQ fsync 之后才 `tracker.Resolve`；L1 回调在 `db.Update` 返回之后、内存索引（segRef、pending 计数）更新之后才执行。
3. **dispatchCursor**：一个事件的所有 `Add` 完成后才 `Publish`（`defer`）；`Watermark` 先读 cursor 再锁 tracker。
4. **L2 入队与状态切换互斥**：`Enqueue` 在 slot 锁内检查状态，`Drain` 在同一把锁内切到 Draining，因此不会有事件在 drain 扫尾之后才入队而被遗留。
5. **(seq, dest) 独占**：L1 中一对 pending 同时只会被一个 claim 持有者处理（redeliver→worker 或 sweeper）。
6. **L1 单写者**：所有 bbolt 写（Park/Ack/Fail/Dead/ckpt/registry）都经过同一个 goroutine；回调不阻塞、不再提交新操作。
7. **slot 表**：写时复制（`atomic.Pointer`），Register/Unregister 由 `adminMu` 串行化。
8. **段删除**：`segMu` 下维护读者计数和 removed 标记，删除只在计数归零时发生。

## 3. 测试说明

- 单元测试：tracker（乱序 Resolve、无流量目的地随 cursor、随机模型对拍）、state（合并、清位删除、segRef、due、陈旧索引、提交后回调、Abort、ckpt 单调、registry 稳定 ID/tombstone、sweeper 候选）、delivery（HTTP 分类、jitter 上下界与可复现、Retry-After、预算、熔断状态机，FakeClock）、worker（份额策略、失败退避与 NextAt、熔断时不计 attempt、预算耗尽进 DLQ，FakeClock）、WAL（延迟删除、ScanFrom）、DLQ（滚动、撕裂尾）、sqlite（幂等）、webhook（httptest：5xx/400/429/连接拒绝）。
- 集成测试（进程内 fake sink + 真实 file/sqlite sink，真实时钟 + 很短的间隔，用轮询等待条件，没有固定 sleep 断言）：
  - 两个 sink 一个失败：健康 sink dup=0；失败 sink 恢复后总共精确一次；L1 归零。
  - NonRetryable → DLQ，DLQ 集合等于失败的 (seq, dest)。
  - 预算耗尽 → DLQ（attempts = max_attempts）。
  - 孤儿：TTL 内注册后送达；不注册则 sweeper 在 TTL 后转 DLQ。
  - Draining：在途批次完成，剩余 Park（NotActive），新事件也 Park，重新注册后全部送达。
  - GC：被 L1 引用的段保留，其余在 checkpoint 越过后删除；引用释放后再删除。
  - 配额 block_all → 429，积压消化后恢复。
  - 崩溃恢复（三个崩溃点 + 连续两次崩溃）：每个已 ack 事件到达每个目标至少一次；sqlite 存储无重复；崩溃在"sink 成功、checkpoint 之前"时 file sink 的原始重复数正好等于全部重放量，符合"上界 ≈ checkpoint 间隔内的事件数"。
- **崩溃是进程内模拟**：`Gateway.crash()` 丢弃未提交的 L1 操作、不写最终 checkpoint、忽略在途结果、丢弃 L2；WAL 句柄会被正常关闭（已 ack 的记录早已 fsync，等价于最后一次 fsync 后崩溃）。真正的 `kill -9` 场景（S3）在 M3 的 faultbench 里跑。

## 4. 已知缺口

1. 没有 Prometheus 指标、admin API（Register/Unregister 目前只有 Go API）、`loggatectl`，留到 M5。
2. 没有磁盘配额和 ENOSPC 只读模式；DLQ 不计入磁盘配额（§0.5 item 10 未完成）。
3. sweeper 每轮全量扫描 `l1` bucket，复杂度 O(L1 记录数)；L1 很大时应改为按 `ReceivedAt`/`FirstAt` 建二级索引。
4. bbolt 文件不会收缩；长时间故障后 `state.db` 保持高水位（REVIEW 已指出 soak 只看 WAL 段数）。
5. L1 提交失败（bbolt 错误）时，对应 seq 保持未解决，水位卡住、GC 停止，直到重启后重放；目前只记日志，没有告警指标。
6. `/v1/stats` 里的 DLQ 计数只统计本进程写入的条目。
7. redeliver 读不到 WAL 记录时（理论上不应发生，segRef 会钉住段）会按 `retry.max` 推迟，最终由 sweeper 在 `max_age` 时转 DLQ（body 记为 null）。
8. 只在 Windows 上跑过；race detector 未运行（见 §2）。
9. 脱敏和 OCSF 未做（M4）：WAL、L1 引用的记录、DLQ 都保存未脱敏的事件。日志里不输出事件内容，只输出 seq、段号等定位信息。
