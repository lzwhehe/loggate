# 实现说明（M2–M5）

本文记录 M2、M3 实现与 `DESIGN.md`（含 §0.5 v0.2 修订）的偏差、补充的细节和原因，以及已知缺口。没有列出的部分按设计实现。

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

1. ~~没有 Prometheus 指标~~（M5 已做，见 §13）；没有 `loggatectl` / gRPC（§0.5 不在 v1 范围）。（M3 已补最小 HTTP admin API，见 §7-7。）
2. 没有 ENOSPC 只读模式。（M3 已补最小磁盘配额，DLQ 计入，见 §7-8。）
3. sweeper 每轮全量扫描 `l1` bucket，复杂度 O(L1 记录数)；L1 很大时应改为按 `ReceivedAt`/`FirstAt` 建二级索引。
4. bbolt 文件不会收缩；长时间故障后 `state.db` 保持高水位（REVIEW 已指出 soak 只看 WAL 段数）。
5. L1 提交失败（bbolt 错误）时，对应 seq 保持未解决，水位卡住、GC 停止，直到重启后重放；目前只记日志。M5 起可以从 `loggate_watermark` 停止增长间接发现，没有专门的告警指标。
6. `/v1/stats` 里的 DLQ 计数只统计本进程写入的条目。
7. redeliver 读不到 WAL 记录时（理论上不应发生，segRef 会钉住段）会按 `retry.max` 推迟，最终由 sweeper 在 `max_age` 时转 DLQ（body 记为 null）。
8. 只在 Windows 上跑过；race detector 未运行（见 §2）。
9. ~~脱敏和 OCSF 未做~~（M4 已做，见 §10–§12）：脱敏在 WAL append 之前完成，WAL、L1 引用的记录、DLQ 只保存脱敏后的事件。日志里不输出事件内容，只输出 seq、段号等定位信息。

# M3：故障注入基准（faultbench）

## 5. 组成

| 组件 | 位置 | 说明 |
|---|---|---|
| mocksink | `cmd/mocksink`、`internal/bench/mocksink` | 一个进程托管多个具名 sink（各占一个端口）+ 独立控制端口：`POST /fault {dest, mode: ok\|down\|slow, delay_ms, error_code, error_rate, seed}`、`GET /stats`、`POST /flush`、`POST /shutdown`。每个事件写一行 CSV 日志 `dest,test_seq,idem_key,recv_unix_ns,scheduled_send_ns,status,gw_seq,run_id` |
| loadgen | `cmd/loadgen`、`internal/bench/loadgen` | 开环定速：第 i 条的计划时刻 = start + i/rate（不累积舍入误差），每条一个 goroutine，只受 `max-inflight` 约束；延迟从计划时刻算（无 coordinated omission）；派发滞后 > 10ms 或撞到并发上限时计数并在报告里写出。承诺集合 = HTTP 200 的 test_seq（`sent.csv` + `acked.bitmap`），429/503/传输错误分开记录；默认不重试（`-retries` 可配，重试保持原 test_seq 和计划时刻）。ack 延迟用 HdrHistogram |
| reconcile | `cmd/reconcile`、`internal/bench/reconcile` | 独立 oracle：`E_d = acked ∩ routed_to(d)`，routed_to 用场景文件里的路由表、由 reconcile 自己的代码求值（不调用网关的 router）。读取 mocksink 日志、file sink JSONL（容忍撕裂行）、sqlite 行、网关 DLQ |
| faultbench | `cmd/faultbench`、`internal/bench/faultbench` | 按场景 YAML 编排：mocksink 进程、网关进程（`cmd/loggate` 的二进制，生成配置 + 全新 data dir）、loadgen 进程、时间轴、排空等待、优雅停止、reconcile、多次重复聚合 |
| 场景 | `bench/scenarios/` | `_base.yaml`（共享拓扑、路由、网关配置、通用通过标准）+ S0、S1、S2、S2b、S3、S3b、S4b、S5、S6 |
| 脚本 | `scripts/faultbench.ps1`、`scripts/faultbench.sh`、`Makefile` | 构建 5 个二进制并运行场景 |

## 6. 指标定义（reconcile）

对每个目的地 d：

- **expected** = |E_d|，E_d = 承诺集合 ∩ oracle 路由到 d 的事件；
- **lost** = E_d 中既没送达、也不在 DLQ 的事件数（进 DLQ 的事件是"已交代"的，单独计 dlq；同时检查 `dlq_and_delivered`，既送达又进 DLQ 视为错误）；
- **dup_raw** = 送达副本数 − 唯一 test_seq 数（按 sink 实际写入的原始副本）；
- **dup_stored**：sqlite = 行数 − 唯一 test_seq；webhook = 唯一 idem_key 数 − 唯一 test_seq（即"遵守 Idempotency-Key 的接收方"会存下的重复）；file = dup_raw；
- **extra** = 送达但未被 ack 的事件（客户端看到失败、网关其实已持久化，不算错）；
- **misrouted** = 送达或进 DLQ、但 oracle 不路由到 d 的事件；
- **recovery_s** = 故障窗口结束后，所有"窗口结束前已 ack、未进 DLQ、路由到 d"的事件全部到达所需的时间；窗口结束前已全部到达记 0；有丢失或到达时间未知时为空；
- **延迟** = 首次到达时刻 − 计划发送时刻，按计划时刻分成 baseline（窗口前）/ during / after 三段，报告 p50/p99/p999；
- **dlq_matches_rejected**（webhook）= DLQ 集合是否等于 mocksink 以不可重试 4xx（非 408/429）拒绝过的事件集合。

## 7. 与设计的偏差和补充

| # | 设计 / 任务 | 实现 | 原因 |
|---|---|---|---|
| 1 | 三个目的地 archive/siem/soar | 另加第四个 **probe**（webhook → mocksink，永远健康，路由 auth+net） | §0.5 item 12 要求健康 sink 的延迟由独立接收方测量；file/sqlite 没有接收时钟 |
| 2 | file/sqlite 的到达时间 | faultbench 内的 **observer** 每 50ms 轮询 JSONL（增量读、处理半行）和 sqlite（`rowid >` 增量查询），记录每个 test_seq 首次出现的时间 | 与网关无关的独立观测；分辨率 50ms，在报告中注明 |
| 3 | lost 的定义 | lost 不含已进 DLQ 的事件，DLQ 单列 | S4b/S6 中进 DLQ 是预期行为；同时用 `dlq_and_delivered` 和 `dlq_matches_rejected` 防止 DLQ 掩盖错误 |
| 4 | mocksink `down` | `http.Server.Close()`：关闭 listener 和所有已建连接，客户端得到 connection refused/reset；恢复时在同一地址重新 listen（失败重试 5s） | 任务要求"连接失败而非 5xx" |
| 5 | mocksink `slow` | 先睡 delay 再判定、写日志、应答；客户端超时断开**不会**中止处理 | 与真实接收方一致，S2b 的重复就来自这里 |
| 6 | 错误注入 | 每个 sink 自设置故障起按请求计数 n，`SplitMix64(seed, n) < error_rate` 时返回 error_code | 给定请求顺序完全可复现 |
| 7 | 管理面（DESIGN §8 gRPC） | 在 ingest 同一端口上加 `POST /v1/admin/{shutdown,unregister,register}`，Bearer token（sha256 + 常数时间比较），未配置 `admin` 时不挂载；register 只给名字时按启动配置重新注册 | faultbench 需要注销/注册；Windows 没有 SIGTERM，优雅停止走 `/v1/admin/shutdown`（Linux 上 faultbench 用 SIGTERM） |
| 8 | 磁盘配额（§4.9、§0.5 item 10） | `disk: {max_bytes, check_interval}`：后台每 `check_interval`（默认 1s）遍历 data_dir 求和（WAL + state.db + DLQ），超出时 ingest 返回 429 + Retry-After；`/v1/stats` 增加 `disk_bytes`、`disk_over_quota` | 最小实现；ENOSPC 只读模式仍未做；bbolt 文件不收缩，长时间故障后 state.db 可能让用量停在配额之上（S5 需在 Linux 上验证） |
| 9 | file sink 撕裂行 | file sink `Init` 时若文件末尾不是换行，先补一个换行；reconcile 仍容忍"撕裂片段 + 完整记录粘在同一行"（从最后一个 `{"seq":` 重新解析） | 设计阶段发现：kill -9 后重启的第一条记录会和半行粘在一起，行式消费者读不出来，会被误判为丢失 |
| 10 | S3 时间轴 | 一次运行内 3 个 kill/restart 周期（60/70、120/130、180/190s） | 任务允许 |
| 11 | S0 | 增加无故障基线场景（同 seed、同速率、同"参考窗口"），fault.md 给出各场景 `p99 during` 相对 S0 的变化 | §0.5 item 12：p99 基线是同 seed 的无故障运行 |
| 12 | "健康 sink p99 增幅 < 20%" | 只在报告中列出（`p99 during vs S0` 列），**不作为自动通过标准** | 单机上 loadgen/mocksink/网关共用 CPU，Windows quick 下数值噪声大；在 Linux full 结果上人工判读，后续再决定是否做成自动检查 |
| 13 | profile | `quick`（200 eps，时间轴 ×0.2，约 60s，1 次）、`full`（2000 eps，5 分钟，5 次）、`smoke`（100 eps，×0.1，仅用于开发 harness）。网关的超时、TTL、退避、熔断参数**不随 profile 缩放** | quick 下故障窗口更短，但恢复时间仍由真实的退避（max 30s）和熔断（10s）决定 |
| 14 | S5 | 场景标 `platforms: [linux]`，Windows 上报告 SKIPPED；段 4MB、`disk.max_bytes: 64MiB`；需要真实 ENOSPC 时用 `LOGGATE_BENCH_DATA_ROOT` 指向 64MB tmpfs | 任务要求 |
| 15 | 原始数据 | 每次重复的原始文件在 `<results>/raw/<scenario>/rep<N>/`（sent.csv、mocksink 日志、sink 输出、observer 文件、gateway.yaml/log、data dir、reconcile.json/md），被 `.gitignore` 排除；提交的是 `<scenario>.json`、`fault.md`、`summary.csv`（每场景 × 重复 × 目的地一行） | 原始文件在 full profile 下很大 |

## 8. Windows quick 运行（本机，commit 1adcd08）

结果：`bench/results/20261017-234601-windows-quick/`。S0–S4b、S6 全部 PASS，所有目的地 lost = 0；S5 SKIPPED（仅 Linux）。没有发现需要修复的网关丢数据问题；开发 harness 时发现并修复了 file sink 的撕裂行问题（§7-9，带回归测试 `TestInitTerminatesTornLine`）。几个值得记下的观察：

- **S3 的重复量符合上界**：checkpoint 间隔 1s，quick 负载下 archive 200 eps、probe 160 eps、soar 100 eps，3 次 kill 的理论上界约为 600 / 480 / 300；实测 461 / 361 / 240。siem（幂等）存储重复 0，webhook 的重复全部带同一个 idem_key（dup_stored = 0）。extra = 2–3：kill 发生在 fsync 之后、响应发出之前，客户端看到失败但事件已持久化并送达。
- **S1 恢复 6.56s**：主要由 full-jitter 退避（max 30s）和熔断 open_for 10s 决定，不随 profile 缩放。
- **S2b**：soar 原始重复 908，dup_stored = 0（重复全部是同一 idem_key 的重发），与"超时比接收方短必然重复"的预期一致。
- **S3b**：dup = 0；停止窗口内已 ack 的事件在重启后约 2.3s 内送达（latency during p99 ≈ 2.3s，含 1s 的停机窗口和重启时间）。
- **S6**：DLQ 241 条，与 mocksink 以 400 拒绝的事件集合完全一致。
- 健康 sink（archive、siem、probe）在所有场景中故障期间的 p99 与 S0 相差 ≤ 1%（S3b 除外，那里所有 sink 都停了）。file/sqlite 的 p99 ≈ 80ms 主要来自 50ms 轮询分辨率；webhook 的 ≈ 32ms 来自 fsync（Windows 上约 10ms）和 20ms 批等待。

## 9. 已知缺口（M3）

1. **最终数字必须来自 Linux**：本机只跑了 Windows quick（单次重复、200 eps），只能当冒烟测试；kill 用 TerminateProcess，优雅停止用 admin API，fsync 语义与 Linux 不同。
2. S5（磁盘满）只在 Linux 上运行，尚未跑过；配额检查是轮询（默认 1s），在两次检查之间仍可能超出配额；没有 ENOSPC 只读模式。
3. loadgen、mocksink、网关在同一台机器上，互相争抢 CPU；延迟数字包含这部分干扰。
4. file/sqlite 的到达时间分辨率是 observer 的 50ms 轮询；webhook（soar、probe）的到达时间是 mocksink 判定结果的时刻（slow 模式下在延迟之后）。
5. Vector / Fluent Bit 对照（§10.4）、soak、性能矩阵（§11）未做。
6. race detector 仍未在本机运行（无 gcc）。

# M4：OCSF 归一化、脱敏、分级路由、合成评测

## 10. 组成与数据流

ingest（HTTP JSON 或 `text/plain` 按行）→ `normalize.JSON / normalize.Text`（OCSF 子集 map）→ `mask.Engine.MaskEvent`（就地改写，返回 DataClass / DataLevel）→ `route.Table.Route`（规则匹配 + max_level 过滤）→ WAL append。四步都在请求 goroutine 的内存里完成，WAL 编码器只序列化脱敏后的 `LogEvent`。

| 组件 | 位置 | 说明 |
|---|---|---|
| 归一化 | `internal/normalize` | 3002（sshd 正则、JSON `event.type=auth`）和 Base Event 0；golden 文件在 `testdata/golden`，schema 在 `testdata/ocsf_subset.schema.json` |
| 检测器 | `internal/mask/detect.go` | 数字串扫描（分组卡号 + 纯数字串）、校验函数、e-mail RE2、区间冲突消解 |
| 引擎 | `internal/mask/engine.go` | 策略 trie、树遍历、掩码改写、HMAC、fail-closed、分级 |
| 评测 | `internal/piibench`、`cmd/piigen`、`cmd/piieval` | 生成器、span 级评测、残留检查、markdown/JSON 报告 |

## 11. 与设计的偏差和补充

| # | 设计 / 任务 | 实现 | 原因 |
|---|---|---|---|
| 1 | §6 metadata.uid | 未填；idem_key 在 LogEvent 顶层 | idem key 在 WAL writer 里才分配，在那里改 map 没必要 |
| 2 | §6 JSON 事件的 raw_data | JSON 输入不保留 raw_data，整个对象在 unmapped（3002 时去掉已映射的键）；文本行的 raw_data = 原行 | 避免同一内容存两份、扫两遍；`LogEvent.Raw` 不再填充（以前存的是未脱敏原文） |
| 3 | §6 time | 3002 的 JSON 事件解析 `@timestamp / timestamp / time`（RFC 3339 或毫秒）；sshd 和 Base Event 用接收时间 | syslog 时间戳没有年份；Base Event 的 unmapped 保持原样 |
| 4 | "Connection closed by" | 带 `[preauth]` → Logon 失败（status_detail 说明）；不带 → Logoff 成功；另支持 `Disconnected from` | preauth 断开本质上是一次没完成的登录尝试 |
| 5 | auth_protocol | 字符串取方法名（`ssh2/password`、`publickey`），`auth_protocol_id = 99 (Other)` | OCSF 的枚举里没有 SSH 方法 |
| 6 | 策略 trie | 显式 drop/hash 作用于整个子树；从祖先继承来的 drop/hash 遇到"下面还配置了策略"的节点时改为逐叶应用（否则 `default: drop` + `unmapped.x: scan` 会把整个 unmapped 丢掉）；keep 节点下面有配置时继续向下走 | 让"默认丢弃、白名单保留/扫描"可用 |
| 7 | 内置 keep | `class_uid、class_name、category_*、activity_*、type_*、status_id、severity_id、time、metadata` 默认 keep，可被配置覆盖 | 网关自己生成的字段，不含输入内容 |
| 8 | 扫描范围 | 字符串和 `json.Number` 都扫；命中的数字值改写成字符串。bool / 网关生成的 int 不扫 | JSON 数字里也可能是身份证、卡号 |
| 9 | URL 转义 | `%XX` 结尾视为左边界；`%20` 这类全数字转义会和后面的数字连成一串，扫描时跳过前两位 | nginx 访问日志里常见；否则严格边界会全部漏掉 |
| 10 | 掩码收敛 | 掩码后再检测一遍，最多 4 轮，仍有命中则整值 `[REDACTED:error]` | 第一张卡掩码保留后 4 位，可能和后面的组构成新的合法卡号窗口（`TestMaskRescanConverges`）；fuzz 的性质依赖这一点 |
| 11 | 手机号 span | span 只含 11 位本号，`+86 / 86` 是上下文 | 精确匹配评测里对基线公平 |
| 12 | 卡号长度 | 严格 16–19 位（Amex 15 位不识别，BIN 34/37 只对 16+ 位生效） | 任务规定 16–19 |
| 13 | 出生日期上界 | "今天"在引擎创建时确定（可注入），进程长时间运行时上界不更新 | 差一天的边界影响可以忽略；测试和评测可复现 |
| 14 | HMAC 密钥 | `masking.hmac_key_env`（默认 `LOGGATE_HMAC_KEY`，≥ 16 字节）；未设置时随机生成，配置了 hash 策略时告警 | 与 idem key 的处理一致 |
| 15 | max_level | `route.Table` 里每目的地一个原子上限 + 计数；运行时 register 时同步更新 | Route 在 ingest 时执行一次，结果写入 WAL 的 targets；重放、重投不再重新判断 |
| 16 | 400 错误 | 只返回 `event N: invalid JSON at offset K` 这类信息 | encoding/json 的错误串会引用输入字节 |
| 17 | 文本输入 | `Content-Type: text/plain` 走按行切分；syslog TCP 没做 | 任务里 syslog TCP 为可选；HTTP 已覆盖 |
| 18 | `masking.disabled` | 新增，仅供基准对照 | 关闭时启动日志告警 WAL 中是原文 |
| 19 | file sink 行格式 | 增加 `data_class`、`data_level` | 便于在归档里核对标签 |

## 12. 评测方法与已知缺口（M4）

**评测方法**：`piigen` 用固定 seed（PCG）生成记录，每条记录带正样本 span（类型、表层变体、是否已知局限）和困难负样本 span（类型）。`piieval` 对 LogGate 检测器、`regex_naive`、`regex_bounded` 做 span 级精确匹配；误报按重叠的负样本类型或"落在另一种 PII 里"归因，漏报按表层变体统计；已知局限（全角数字、`%40`、紧贴字母）单列、不计分，落在上面的预测也不算误报。另外用 `MaskString`（整行）和 `MaskEvent`（app_json 记录解码后整棵树）检查正样本原文是否残留。报告记录数据集 sha256。

**本次结果**（`bench/results/20261018-002220-pii/`）：LogGate micro P/R/F1 = 0.9950 / 1.0000 / 0.9975，误报 83（全部是银行卡：像身份证但校验失败的 18 位数字落在 BIN 里，以及随机数字 ID 碰巧通过 BIN + Luhn）；naive 0.4334 / 0.8696 / 0.5785；bounded 0.6348 / 0.8536 / 0.7281。残留 0。吞吐见 README 和 `bench.txt`。脱敏开启后重跑 quick S0、S1：全部目的地 lost = 0（`bench/results/20261018-m4-windows-quick-S0S1/`）。

**已知缺口**：
1. 召回 1.0 主要验证实现：生成器与检测器共享格式假设；没有真实日志样本（也不应该用真实 PII）。
2. 18 位、像身份证但校验失败的数字会被当成卡号（省码与 BIN 重叠）；没有为此加启发式，避免对自己生成的数据调参。
3. 不识别：姓名、地址、15 位老身份证、全角数字、`%40` 邮箱、紧贴 ASCII 字母的号码、空格分隔的手机号（`138 1234 5678`）、IPv4（设计里是可选项）。
4. 分级只到单事件；"重要/核心"取决于数据规模和用途，这里只是可配置标签。
5. 泄露面：panic 栈、core dump、swap 不在进程控制范围内；`masking.disabled` 时 WAL 存原文。
6. 吞吐数字来自 Windows 笔记本的单 goroutine 基准，未做 CPU 隔离；端到端（含 WAL fsync）的"脱敏开/关"对照在 M6 的性能矩阵里做。
7. syslog TCP 输入未实现。

# M5：Prometheus 指标、演示模式、Web 控制台

## 13. 组成

| 组件 | 位置 | 说明 |
|---|---|---|
| 指标 | `internal/metrics` | 私有 `prometheus.Registry`（另注册 Go / process collector）。事件类：`IngestEvents`、`WALFsync`、`Delivered`、`DeliveryError`、`DLQ`，由 gateway 接到 httpingest `OnResult`、`wal.Options.OnFsync`、`worker.Deps.Observer`、`dlq.Log.SetOnAppend`。状态类：自定义 collector 在抓取时调用 `Source`（`Gateway.MetricsSnapshot`），网关未运行时不输出这些 gauge |
| 控制台 | `internal/ui` | `go:embed assets`（index.html / app.js / app.css，无外部资源、无内联脚本，CSP `default-src 'self'`）；`GET /ui/api/snapshot`、`GET /ui/api/stream`（SSE，`event: snapshot`，默认 1s）；`ui.FromGateway` 由网关 Stats + 指标视图构建快照 |
| 演示 | `internal/demo` | `Runner`：模拟 SOAR 接收端（`down` 关闭监听、`slow` 先睡再收）、合成负载、对账、脱敏样例环形缓冲、操作记录、`/demo/*` 处理器 |
| 主程序 | `cmd/loggate` | 普通模式按配置起 metrics / ui 监听；`--demo` 忽略 `-config`，使用内置演示配置，全部只绑定 127.0.0.1 |

## 14. 与设计的偏差和补充

| # | 设计 / 任务 | 实现 | 原因 |
|---|---|---|---|
| 1 | §8 指标名 | 统一加 `loggate_` 前缀；breaker / plugin 状态用数值编码（含义写在 HELP）；另加 `loggate_watermark{dest}`、`loggate_route_level_blocked_total{dest}` | 任务列表；数值 gauge 便于告警表达式 |
| 2 | `ingest_events_total{source,result}` | accepted / wal_error 按事件数计；在解析请求体之前就决定的结果（backpressure / unauthorized / bad_request / too_large / draining）按请求计 1；无有效 token 的 source 记为 `unauthenticated` | 这些结果下不知道事件数，读请求体只为计数不值得；客户端不能自报 source，所以基数受 token 配置限制 |
| 3 | 标签基数 | 每个标签一个限流器：首见的前 N 个值原样保留（dest 64、source 64、reason 32、type 16、kind / origin / result 各 8–16），之后的值记为 `other` | dest 本就受 registry 64 个上限约束；限流器是兜底，防止代码以后引入不受控的值 |
| 4 | e2e 延迟 | `now − ReceivedAt`，在 sink 确认时记录，含 L1 暂存和重放时间。控制台 p99 用每目的地最近 4096 个样本中 30s 窗口内的精确分位数，不从直方图桶估算 | 网关自报的延迟只用于运维观察；基准仍以独立 mocksink 测量为准（§0.5 item 12） |
| 5 | mask_hits、route_level_blocked、backpressure 等 | 从网关的累计计数器抓取时读出，作为 counter 输出；模拟崩溃 / 重启后从 0 开始（Prometheus 按计数器重置处理） | 计数器本来就在 mask.Engine / route.Table 里，不重复维护 |
| 6 | §16 demo "kill -9 后重启" | 控制台按钮是**进程内模拟**：`Gateway.Crash()`（原 `crash()` 导出）丢弃未提交的 L1 操作、L2、在途结果，不写最终 checkpoint，WAL 句柄正常关闭；随后用同一配置和数据目录 `New + Start` | 演示进程同时托管接收端、负载和对账，真正 kill 掉就没有观察者了；真实 kill -9 在 faultbench S3 |
| 7 | §16 "脱敏前后对比" | 只展示**脱敏后**的事件（§0.5 item 11 已改）；数据来自 httpingest `OnAccepted`，在 WAL append 成功之后调用，只在演示模式设置 | 不保留原文；append 返回后 encoder 已完成，读事件不会与 WAL writer 竞争 |
| 8 | §16 "Replace v2" 按钮 | 没有（v1 不做 Replace），改为 Unregister / Register | §0.5 item 5 |
| 9 | 演示对账 | 按网关 seq 对账：负载端每个请求一条事件，收到 200 时记下 `first_seq` 和预期目的地（预期路由由生成器根据自己生成的内容独立计算：认证类 → 三个目的地，其他 → archive + siem；同时含身份证和银行卡 → 分级"重要"，不投 soar）。接收端：soar 记录每个 seq 的副本数和 idem_key；archive 增量读文件；siem 增量查 `rowid > ?`；DLQ 读目录 | sshd 文本行无法携带 test_seq；seq 对账足以展示 lost / dup。faultbench 仍使用独立的 test_seq oracle |
| 10 | lost 的判定时机 | 先读网关每目的地水位 W 和 L1 pending（同一次 Stats，水位先读），再轮询接收端；只有 L1 pending = 0 时才判定 `lost = 预期中 ≤ W、未送达、不在 DLQ 的事件数`，否则显示"收敛中" | W 以下的事件要么已被 sink 确认，要么在 L1 / DLQ。L1 为空时，W 以下未送达且不在 DLQ 的事件就是真丢失；轮询在读 W 之后进行，不会因为观测滞后而误报 |
| 11 | 演示端点保护 | 只接受回环来源 + 回环 Host（防 DNS rebinding）+ `X-LogGate-Demo: 1` 头（自定义头会触发 CORS 预检，服务端不应答预检，其他网页无法调用）+ JSON 请求体；只在 `--demo` 时挂载 | 任务要求"仅本机或 token 保护"；本机即可满足，不再引入额外 token |
| 12 | 演示参数 | 退避 100ms–2s、熔断 3 次 / 3s、`max_attempts` 100000、`orphan_ttl` 10m、WAL 段 4MiB | 让故障和恢复在几秒内可见，演示中不会因预算耗尽进入 DLQ |
| 13 | 演示目录 | 默认新建临时目录，退出时删除；`-demo-dir` 指向已有 `data/` 的目录会拒绝启动 | 对账状态只在内存中；旧数据重放会产生负载端没见过的事件 |
| 14 | 默认监听 | 配置默认 `admin.metrics: ":9091"`、`admin.ui: ":8081"`（DESIGN §9）；示例配置改为绑定 127.0.0.1；faultbench 生成的配置用 `metrics: 127.0.0.1:0`、`ui: off` | 控制台只读，但非演示模式也不展示事件内容；基准运行不应占用开发者常用端口 |

## 15. 本机演示走查（Windows，50 eps）

按 `docs/DEMO.md` 的顺序在浏览器里操作：
- 正常流量：三个目的地 p99 约 27ms，对账 lost=0。
- soar 停止约 25s：熔断断开，最近错误显示 retryable，soar 的 L1 涨到 400 条以上；archive / siem 照常投递，p99 不变；soar 对账显示"收敛中"。
- 恢复：8s 内 L1 回到 0，soar 的 lost=0、dup=0；这段时间 soar 的 30s 窗口 p99 约 21s，就是停机时长。
- 模拟崩溃：9ms 内完成重启，重放 38 条 WAL 记录。archive（file）原始重复 37；siem 存储重复 0；soar 原始重复 26，但没有新的 idem_key，存储重复为 0；三个目的地 lost 都是 0。崩溃瞬间有 1 个请求失败（`wal_error`，客户端看到 EOF），没有 ack，因此不在期望集合里。
- 崩溃后约 30s 内，archive / siem 的控制台 p99 升到约 475ms。原因是被重放事件的 e2e 延迟从原始 `received_at` 起算，并不是变慢；窗口滑过后回到约 27ms。
- soar 慢 500ms：soar 的 p99 约 1s，archive / siem 保持约 27ms，没有被拖慢。
- 注销 soar 后，事件 Park 到 L1（143 条）；重新注册后 5s 内投递完，lost=0。
- 窄屏（375px）无横向滚动；亮色和暗色主题都检查过。

## 16. 已知缺口（M5）

1. 没有 gRPC admin / `loggatectl` / Replace（§0.5 v1 范围之外）。
2. 指标端点没有认证。默认 `:9091` 监听所有网卡（按 DESIGN §9），部署时应绑定内网地址或由反向代理保护。普通模式下的控制台也没有认证，但只读，不展示事件内容。
3. 状态类计数器（mask_hits、route_level_blocked）和 `/v1/stats` 一样只统计当前网关实例；演示中模拟崩溃后会重置，但 `delivered_total` 等事件类指标跨重启累计。
4. 演示对账依赖网关返回的 `first_seq`，不如 faultbench 的 test_seq oracle 独立；演示 lost=0 只是展示，正式数字以 faultbench 为准。
5. 演示负载按顺序发送：请求变慢时 ticker 会丢 tick，实际速率可能低于设定值（控制台显示实测 eps）。
6. `Store.Count()`（`l1_records`）每次抓取都会遍历 L1 bucket；L1 很大时抓取和控制台刷新的开销随之增加。
7. 控制台在 Windows 的内置浏览器中检查过；其他浏览器只依赖标准的 EventSource / fetch / SVG，未逐一测试。
