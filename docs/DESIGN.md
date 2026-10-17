# LogGate 设计文档：可靠的安全日志采集网关

> 版本 v0.1（2026-10-17）· 状态：待实现、待红队评审
> 方案：方案 C（L1 失败暂存 + 重投）
> 调研依据：`research/01_业界方案与痛点调研.md`

## 0. 定位
LogGate 是一个单机运行的安全日志采集网关，用 Go 编写。它做三件事：
1. 先写 WAL，再给客户端 ack；
2. 按目的地记录投递状态位图，部分失败时只重投失败的目的地；
3. 用幂等键配合幂等 sink，实现 effectively-once。

在这个可靠管道之上，它针对安全场景做了两项特化：
- 带校验位的中国 PII 脱敏，并打上 GB/T 43697-2024 分级标签；
- 归一化为 OCSF。

核心交付物是一套确定性、可复现的故障注入基准，每个场景都输出丢失数、重复数和恢复时间。

## 0.5 v0.2 修订（根据红队评审 REVIEW_01，优先级高于下文）
**正确性**
1. Dispatcher 只消费**已 fsync** 的 WAL 记录。恢复时，`nextSeq = max(WAL 末尾, 所有 ckpt, L1 中出现过的最大 seq) + 1`，seq 绝不复用。
2. 水位 = `min(tracker 队头未解决 seq − 1, dispatchCursor)`。只有一个事件的所有 `Add` 都完成后，才发布 `dispatchCursor`。没有流量的目的地，水位随游标一起推进，因此 GC 不会被卡住。
3. 增加**全局 L1 sweeper**，与目的地状态无关：到达 `orphan_ttl` 或 `max_age` 的记录转为 Dead，写入 DLQ。
4. 转 Dead 的顺序：**先 fsync DLQ，再提交 bbolt 或执行 Resolve**。
5. Draining 时，在途批次必须全部跑完，并走正常的结果处理流程。**v1 不实现 Replace**，S4 推迟，只保留 S4b（注销 → 孤儿 TTL → DLQ）。
6. fsync 返回错误时：把当前段标记为毒段，截断到上一个确认点，滚到新段，本批事件一律不 ack。

**幂等与安全**

7. IdemKey 默认由网关分配：`HMAC(node|boot_epoch|seq)`，写入 WAL，重放和重投时保持不变。只有绑定**认证 token** 的 SourceID 才接受客户端提供的 `X-Source-Seq`，用于吸收客户端重发，客户端不能自报 SourceID。去掉内容哈希和入口 LRU。

**资源**

8. L1 writer 只有一个，用 `db.Update` 自行攒批，不用 `db.Batch`。文档里写清楚 bbolt 是 COW，存在写放大，不再宣称"原地 80B"。
9. 每个目的地单独设 L1 配额，超出后按 `on_quota` 处理：`block_all`（默认）或 `dlq_overflow`。只有真实发送失败才计入 attempts，QueueFull 和 BreakerOpen 不计入。redeliver 至少保证 20% 份额。half-open 状态下新事件也放行。
10. DLQ 计入磁盘配额，删除 `drop_oldest_to_dlq`。

**脱敏**

11. `raw_data` 和 `unmapped` 同样要扫描。银行卡必须同时满足 BIN 前缀和 Luhn 校验。15 位老身份证不在本版范围内。`test_seq` 等测试字段固定设为 keep。措辞改为"默认 scan；脱敏出错时 fail-closed"。泄露面单独列出：错误信息、panic、core dump、swap、demo 的原文回显（demo 只展示脱敏后的结果和命中类型）。

**基准**

12. 每个目的地都通过独立的 mocksink 测延迟，不用网关自报的数据。延迟从**计划发送时刻**算起。p99 的基线是相同 seed 下的无故障运行。`recovery_s` 定义为"故障结束前已 ack 的事件全部送达"的时刻。`routed_to` 由独立 oracle 计算。soak 看 WAL 段数是否回到基线。数据目录放在 Linux 文件系统上，并注明 kill -9 不等于掉电。S5 的段大小设为 4MB。与 Vector、Fluent Bit 的对照最多只跑 S1，结论以实测为准，客户端重试行为保持一致。

**v1 范围（12 天内必须完成）**
- **主链路**：WAL、tracker、ckpt、L1 + sweeper、恢复、GC
- **输入**：HTTP、syslog TCP
- **输出**：file、sqlite、mock webhook
- **测试**：reconcile；场景 S1、S2、S3、S3b、S4b、S5、S6
- **脱敏**：身份证、银行卡、手机号（含 Email），附 piieval
- **OCSF**：3002 和 0
- **分级**：只做 max_level
- **管理面**：HTTP JSON + /healthz + Prometheus（不做 gRPC）
- **展示**：报告页 + 对账面板
- **性能**：1KB × 脱敏开/关，各重复 3 次

**以后再做**：Replace、Postgres、Kafka、UDP、file tail、gRPC、全矩阵基准。

## 1. 目标与非目标
### 1.1 目标
| # | 目标 | 可验证指标 |
|---|---|---|
| G1 | 已 ack 的事件不丢（at-least-once） | 故障场景下 lost = 0 |
| G2 | 部分成功时只重投失败的目的地 | 一个 sink 宕机时，健康 sink 的 dup = 0 |
| G3 | 幂等 sink 上 effectively-once | SQLite/PG 中按 idem_key 去重后，行数等于期望数 |
| G4 | 慢 sink 不拖累其他 sink | 某 sink 延迟 500ms 时，健康 sink 的 p99 增幅 < 20% |
| G5 | 资源有界 | L2 按条数和字节数有界，磁盘有配额；满了就反压（HTTP 429，TCP 停读） |
| G6 | 带校验的 PII 脱敏 | 在合成标注集上报告逐类 P/R/F1，并与纯正则基线对比 |
| G7 | OCSF 归一化 | 覆盖 3002、4001、Base Event (0)，字段子集通过 schema 校验 |
| G8 | 可运维 | Prometheus 指标、gRPC admin/health，插件生命周期可在运行时切换 |

### 1.2 非目标（写进 README）
- **不在吞吐量和插件数量上与 Vector、Fluent Bit 竞争。**
- **不宣称"首创按目的地重试"**：Fluent Bit 已经按 output 独立重试。本项目的差异在三点：
  - (a) 状态位图持久化，崩溃后能按位恢复；
  - (b) 幂等键一直贯穿到 sink；
  - (c) 用公开的对账基准把丢失和重复量化出来。
- 不做多节点复制，不做 HA。
- 不保证顺序。
- 不用 NER 识别姓名和地址。
- 不用 Go 原生 `plugin`，插件在编译期注册。
- 不做 RELP；syslog UDP 只做尽力而为。

## 2. 架构
### 2.1 总览
```
 HTTP JSON ──┐
 syslog TCP ─┼─► Ingestor ─► Normalizer ─► Filter/Masker ─► Router ─► WAL Writer（分段追加+组提交 fsync）
 syslog UDP ─┤   (限流)      (OCSF)       (PII+分级标签)   (DestMask)        │ ① 持久化后才 ack
 file tail ──┘     ▲ 429/停读 ◄──── 反压（磁盘配额 / L1 上限） ◄─────────────┘
                                                                            ▼
                                                                       Dispatcher
                         ┌──────────────── TryEnqueue ─────────────────────┼──────────────┐
                         ▼                                                 ▼              ▼
                  Worker[file] L2(有界)+breaker               Worker[sqlite] L2   Worker[webhook] L2
                         │ ok / err
   L2 满 / breaker 打开 / 非 Active / 投递失败 ──► Park ──► L1 暂存（bbolt：状态位图 + 到期索引）
                         Redeliver Loop（每个目的地一个）── 只重投 Pending 位 ──► Worker L2
                                                         └──► DLQ（不可重试 / 预算用尽 / 孤儿 TTL）
   AckTracker（每目的地水位）─► Checkpoint（bbolt）─► Segment GC
   gRPC Admin/Health · Prometheus /metrics · Web Dashboard /ui
```

### 2.2 数据流与 ack 语义
1. 解析、归一化、脱敏、路由都在内存里完成。**含 PII 的原文从不落盘**，WAL 只存脱敏后的事件。
2. 单个 WAL writer 分配全局 Seq，组提交 fsync（默认 10ms 或 512 条）。fsync 完成后才 ack：
   - HTTP 返回 200；
   - file tail 推进 offset；
   - TCP 只能停读（没有应用层 ack）；
   - UDP 没有 ack。
3. Dispatcher 按 Seq 顺序消费，非阻塞地把事件入队到每个目标的 L2；入队失败就 Park。
4. ack 只代表"已持久化"，不代表"已投递"。投递状态分两条路径记录：
   - 快路径：每个目的地一个水位 checkpoint；
   - 慢路径：L1 里每个事件的位图。

### 2.3 部分成功语义
对事件 e 的每个目标 d，状态取 Pending / Acked / Dead / Resolved-by-watermark 之一。
- 所有 d 都是 Acked、Dead 或已被水位覆盖时，e 才算完成，此后不再阻止 GC。
- 某个 d 失败只影响 d 这一位，其他目的地不会重发。
- 进入 DLQ 不算丢失，单独计数。

## 3. 核心数据结构
```go
type Level uint8 // GB/T 43697-2024: Unset/一般/重要/核心
type DestID uint8      // 0..63，注册时分配并持久化到 registry
type DestMask uint64   // 最多 64 个目的地（已知限制）

type LogEvent struct {
    Seq uint64; IdemKey [16]byte
    SourceID string; SourceSeq uint64
    ReceivedAt, Time int64
    ClassUID, ActivityID, StatusID, SeverityID int32
    Fields map[string]any   // OCSF 字段，未映射的放 "unmapped"
    Raw []byte              // 脱敏后的原文（可选保留）
    DataClass []string; DataLevel Level
    Targets DestMask
}
```

**WAL 记录格式**（小端，CRC32C）：
```
| len u32 | crc u32 | seq u64 | targets u64 | flags u8 | payload(JSON) |
```
- 分段命名为 `%020d.seg`，满 64MB 滚动。
- payload 先用 JSON，profile 发现瓶颈后再换格式。

**L1 记录**：放在 bbolt 的 `l1` bucket，key 是大端编码的 seq。记录长度只随 Owned 中的目的地数变化，因此可以原地覆盖。
```go
type L1Record struct {
    Seq uint64; SegID uint64; Offset int64; Length uint32 // 指向 WAL，不复制事件体
    Owned, Pending, Acked, Dead DestMask
    ParkedAt int64; Reason uint8 // QueueFull/BreakerOpen/NotActive/NoDest/SendFailed/Shutdown
    Slots []DestSlot             // len == popcount(Owned)
}
type DestSlot struct { Dest DestID; Attempts uint16; LastErr uint16; NextAt int64 }
```

其他 bucket：
- `due/<destID>`：key 为 `NextAt|Seq`，作为到期索引；
- `ckpt`：`destID → watermark`；
- `registry`：`name → DestID / 版本 / 状态 / tombstone`。

**错误分类**：
- Retryable：网络错误、超时、5xx；
- NonRetryable：4xx（408/429 除外）、schema 不合法、超大请求；
- Throttled：遵守 Retry-After，且不计入熔断失败。

未知错误一律按 Retryable 处理。

**接口**：
```go
type Sink interface {
    Init(ctx context.Context, cfg map[string]any) error
    Write(ctx context.Context, batch []*Envelope) []error // 与 batch 一一对应
    Health(ctx context.Context) error
    Close(ctx context.Context) error
    Idempotent() bool
}
type Worker interface {
    ID() DestID; State() PluginState
    TryEnqueue(env *Envelope) bool
    Replace(ctx context.Context, next Sink) error
    Drain(ctx context.Context) []*Envelope
    Stats() WorkerStats
}
```

**关键点**：L2 队列属于"目的地槽位"，不属于某个 sink 实例。所以 Replace 时只需换消费者，不用 Park。

## 4. 核心算法
### 4.1 Dispatch
Dispatcher 是单个 goroutine。目的地表用写时复制快照（`atomic.Pointer`）。对每个事件的每个目标 d：
- 先执行 `tracker[d].Add(seq)`；
- 以下情况就 Park：Worker 不存在或不是 Active、breaker 处于 Open、`TryEnqueue` 失败。

Worker 每批取至多 256 条（或等至多 20ms），经 breaker 调用 `Write`，然后逐条处理结果：
- 成功：`tracker.Resolve`；如果是从 L1 来的，再调 `l1.Ack`；
- NonRetryable：进 DLQ，并执行 Resolve 或 `l1.Dead`；
- Retryable 或 Throttled：Park，或调 `l1.Fail`。

### 4.2 Park
Park 交给单个 L1 writer，用 bbolt Batch 组提交（≤ 5ms 或 1000 条）：
1. 新记录：`Owned = Pending = {d}`。已有记录：`|= d`，并追加一个 Slot。
2. 计算 NextAt，写入 `due/d`。
3. **提交之后**才调用 `tracker[d].Resolve(seq)`，表示责任已转给 L1。顺序反过来，崩溃时就有一个丢失窗口。

### 4.3 原地更新位图
- **Ack**：`Pending &^= d; Acked |= d`，并删除对应的 due 项。
- **Fail**：`Attempts++`；超过预算就转为 Dead，否则更新 NextAt 和 due。
- **Dead**：`Pending &^= d; Dead |= d`，写入 DLQ。
- Pending 为 0 时删除记录并执行 `segRef--`；否则覆盖写回（约 80B）。

**退避**：采用 full jitter，`rand(0, min(30s, 200ms·2^n))`。Throttled 时取 `max(RetryAfter, backoff)`。

**预算**：`max_attempts=20` 或 `max_age=24h`，先到先算。

### 4.4 Redeliver（每个目的地一个，每 200ms tick 一次）
跳过的情况：目的地不是 Active，或 breaker 处于 Open（half-open 时只放 1 条探测）。

每轮：
1. 预算为 `L2Free × redeliver_share(0.5)`，给新事件留一半容量。
2. 扫描 `due/d` 里已到期的项。如果该项的 Pending 已不含 d，说明是陈旧索引，删掉。
3. 用 pread 从 WAL 读出事件体，入队 L2；入队失败就停止本轮。

**只重投 Pending 位，已经 Acked 的目的地不会再收到。**

### 4.5 幂等键
- 有来源序号时：`SHA-256(SourceID|SourceSeq)[:16]`，序号来自 HTTP 头 `X-Source-Seq`，或 file 的 inode+offset。
- 没有来源序号时：`SHA-256(SourceID|canonical(raw))[:16]`。副作用是会合并完全相同的合法事件（见 Limitations）。
- 幂等键在 ingest 时计算，所以重放和重投时不会变。
- 可选的入口 LRU 去重（容量 100k），用来吸收客户端的重发，不作为保证。
- 各 sink 的处理：
  - SQLite/PG：`ON CONFLICT(idem_key) DO NOTHING`；
  - webhook：带 `Idempotency-Key` 头；
  - file：非幂等，专门用来暴露原始重复率。

### 4.6 AckTracker 与 Checkpoint
每个目的地一个 deque，按 seq 递增。Resolve 时标记，队头已解决就弹出，并推进水位。每 1s 把水位写入 `ckpt`。

崩溃后重复数的上界约为：checkpoint 间隔 × 速率 + L2 在途数。这个上界由 S3 场景实测验证。

### 4.7 崩溃恢复
1. 打开 bbolt，载入 registry 和 ckpt，扫描 l1 重建 segRef。
2. 校验最后一个分段的 CRC，截断撕裂的尾部。这是安全的：ack 发生在 fsync 之后，被撕裂的记录一定没有 ack。
3. 从 `min(ckpt)+1` 开始扫描 WAL。对每个事件 e 的每个目标 d：
   - `e.Seq ≤ ckpt[d]`：跳过；
   - L1 中 e 的 Owned 含 d：跳过（由 L1 负责）；
   - 其余情况：以 Replay 方式重新 dispatch，可能产生的重复由幂等 sink 吸收。
4. 启动 redeliver，然后开放 ingest。

### 4.8 Segment GC
满足以下三条时删除分段：
- `maxSeq ≤ min(ckpt)`（已注销的目的地取其最终水位）；
- `segRef == 0`；
- 不是当前活动段。

DLQ 是独立的追加日志，会复制事件体，所以不会钉住 WAL。GC 每 10s 运行一次。

压缩（可选）：把被长期钉住的少量事件体搬到新段，再在一个事务里改写引用。

### 4.9 反压
| 条件 | 行为 |
|---|---|
| L2 满 | Park，不阻塞 ingest |
| 磁盘超出 `disk.max_bytes`，或 L1 超出 `max_records` | HTTP 返回 429 + Retry-After；TCP 停读；file 暂停；UDP 丢弃并计数 |
| ENOSPC | 当前批次返回 503，进入只读，直到 GC 释放空间 |
| 单一来源速率过高 | 按来源限流，返回 429 |

磁盘满时默认策略是 block，另有可选的 `drop_oldest_to_dlq`。

### 4.10 优雅关闭
按顺序执行：
1. ingest 返回 503，关闭 listener；
2. WAL 刷盘；
3. Dispatcher 处理完已提交的事件；
4. 各 Worker 等在途请求结束（10s），L2 中剩余事件 Park（Reason=Shutdown）；
5. 写入最终 checkpoint。

验收：优雅重启后 dup = 0。

## 5. 插件生命周期
```
Register ─► Registered ─(Start+Health ok)─► Active ─► Replace(v2) ─► Replacing ─(新实例 ok，原子换消费者 / 失败回滚)─► Active
                                               └──► Unregister ─► Draining ─(L2 清空或到 deadline)─► Unregistered(tombstone)
```
- **Registered**：已分配 DestID，但还不投递。
- **Draining**：不再接收新事件（新事件 Park，Reason=NotActive），把 L2 处理完，到 deadline 后剩余的也 Park。
- **Replacing**：新实例 Health 通过后，停掉旧消费者，新实例接管同一个 L2，全程不 Park。失败则回滚，并上报 `replace_failed`。
- **Unregistered**：tombstone 期间（默认 24h）DestID 不复用。

**目的地不存在或已注销时的统一策略**：
1. 加载配置时告警；按名字预留 DestID，状态为 Absent。
2. 事件 Park，Reason 为 NoDest 或 NotActive，**不消耗重试次数**。
3. TTL 内该目的地重新注册并变为 Active，就自动接管这些事件。
4. 超过 `orphan_ttl`，事件转为 Dead，进 DLQ（`orphan_ttl_expired`）。

这样，正在注销的目的地和拼写错误的目的地都不会静默丢数据。

## 6. 归一化（OCSF 子集）
| 类 | 来源 | 关键字段 |
|---|---|---|
| Authentication 3002 | sshd 的 Accepted/Failed；JSON 中的 auth 事件 | activity_id（1 Logon / 2 Logoff）、status_id（1 成功 / 2 失败）、user.name、src_endpoint.ip/port、auth_protocol、type_uid = 300200 + activity_id |
| Network Activity 4001 | iptables/nftables（SRC=、DST=、PROTO=、SPT=、DPT=）；JSON 流量日志 | src/dst_endpoint、connection_info.protocol_name、traffic.bytes、disposition |
| Base Event 0 | 其他 | raw_data、unmapped、metadata.product |

公共字段：time、severity_id、metadata.{version, product.name, uid}。测试时用 OCSF 子集 schema 校验（santhosh-tekuri/jsonschema），生产路径不做校验。

## 7. 脱敏引擎
### 7.1 流程
每个字段先查策略（trie）：`keep | drop | hash(HMAC-SHA256→16B) | mask(保留格式) | scan`。

`scan` 的流程：
1. 预过滤：最长连续数字长度、是否含 `@`、`.` 的个数；
2. 只运行命中的检测器；
3. 校验；
4. 区间冲突消解；
5. 改写。

默认策略是 scan，可配置为 `default: drop`（fail-closed）。脱敏出错时，把整个字段替换为 `[REDACTED:error]`。

### 7.2 检测器
数字类检测器用手写的 `[]byte` 扫描器提取边界明确的数字串，零分配。

| 检测器 | 候选 | 校验 | 置信度 | 掩码 |
|---|---|---|---|---|
| 身份证 | 17 位数字 + `[0-9Xx]` | ISO 7064 MOD 11-2（权重 7 9 10 5 8 4 2 1 6 3 7 9 10 5 8 4 2，sum%11 映射到 `10X98765432`）；出生日期合法；省级代码合法 | 高 | `110105********123X` |
| 银行卡 | 16–19 位，允许每 4 位用空格或 `-` 分隔 | Luhn；BIN 前缀可配置 | 高 | `6222 **** **** 1234` |
| 手机号 | `(+86\|86)?1[3-9]\d{9}` | 号段表 | 中（无校验位） | `138****5678` |
| Email | 含 `@` 才运行 RE2 | 域名含点，TLD ≥ 2 位 | 中 | `z***@example.com` |
| IPv4（可选） | 点分十进制 | netip，区分私网和公网 | 低 | 保留 /24 |

区间重叠时，优先级为 身份证 > 银行卡 > 手机号；同级取较长的区间。

### 7.3 分级标签
DataLevel 取所有命中项分级的最大值，映射关系可配置。默认：
- 身份证、银行卡标记为 `pii.sensitive`，级别为一般；
- 同一条事件里身份证和银行卡同时出现，升为重要；
- 核心只能显式配置。

**诚实声明**：GB/T 43697 的分级依赖数据规模和用途，单条事件做不了法律判定。这里只是可配置的标签，不是合规结论。

每个目的地可以配置 `max_level`，超出的事件不投给它，并计数。

### 7.4 性能
- 预过滤加零分配扫描；策略 trie 只构建一次；改写时复用 `sync.Pool`。
- 基准覆盖 200B/1KB/4KB × PII 密度 0/10/50%，报告 ns/op、B/op、allocs/op、MB/s。
- 目标（实测后更新）：1KB、10% 密度时单核 ≥ 50MB/s；不含 PII 时 0 allocs。

### 7.5 合成标注集（piigen / piieval）
- 固定 seed，生成 JSONL 和 spans。
- **正样本**：按算法生成的身份证、银行卡、手机号、邮箱。
- **困难负样本**：
  - 校验位错误的身份证；
  - 日期非法的身份证；
  - 不满足 Luhn 的数字串；
  - 13 位时间戳；
  - 订单号、trace_id；
  - 版本号。
- **模板**：sshd、nginx、应用 JSON、自由文本，并加入全角字符、URL 编码等扰动。
- **评测**：span 级 P/R/F1，与纯正则基线对比，输出误报样例。
- 说明：合成分布不等于真实分布。

## 8. 可观测性
Prometheus 指标：
- `loggate_ingest_events_total`
- `wal_bytes`、`wal_fsync_seconds`
- `l2_depth{dest}`、`l1_pending{dest}`
- `delivered_total{dest,origin}`、`delivery_errors_total{dest,kind}`
- `dlq_total{dest,reason}`
- `breaker_state`、`plugin_state`
- `e2e_latency_seconds`
- `mask_hits_total{type}`
- `backpressure_active`

gRPC Admin：ListPlugins / Register / Unregister / Replace / Stats / DLQList，另加 grpc.health.v1。命令行工具为 `loggatectl`。

## 9. 配置示例
```yaml
node_id: gw-1
data_dir: ./data
wal: { segment_bytes: 64MiB, fsync: batch, fsync_interval: 10ms, fsync_max_events: 512 }
disk: { max_bytes: 8GiB, on_full: block }
l1:  { max_records: 5_000_000, checkpoint_interval: 1s, orphan_ttl: 24h }
retry: { base: 200ms, max: 30s, max_attempts: 20, max_age: 24h, redeliver_share: 0.5 }
ingest:
  http:   { listen: ":8080", max_body: 1MiB, rate_per_source: 20000 }
  syslog: { udp: ":5514", tcp: ":5514" }
  file:   [{ path: /var/log/secure, parser: sshd }]
masking:
  hmac_key_env: LOGGATE_HMAC_KEY
  default: scan
  detectors: [id_card, bank_card, phone, email]
  fields: { "user.name": hash, "user.credential": drop, "src_endpoint.ip": keep }
  levels: { "pii.id_card": general, "combo:id_card+bank_card": important }
destinations:
  - { name: archive, type: file,    queue: { max_events: 10000, max_bytes: 64MiB }, config: { path: ./out/archive.jsonl } }
  - { name: siem,    type: sqlite,  max_level: important, config: { dsn: ./out/siem.db } }
  - { name: soar,    type: webhook, max_level: general, breaker: { failures: 5, open_for: 10s },
      config: { url: "http://127.0.0.1:9100/ingest", timeout: 2s } }
routes:
  - { match: { class_uid: 3002 }, to: [archive, siem, soar] }
  - { match: { class_uid: 4001 }, to: [archive, siem] }
  - { match: {},                  to: [archive] }
admin: { grpc: ":9090", metrics: ":9091", ui: ":8081", demo: false }
```

## 10. 故障注入测试框架（核心交付）
### 10.1 组件
- **loadgen**：开环、固定速率发送，每条事件带 run_id、test_seq、X-Source-Seq。只把收到 200 的 test_seq 记进"承诺集合"（acked.bin）。
- **mocksink**：提供 `POST /fault {mode: ok|down|slow, delay_ms, error_code, error_rate}`。`down` 时关闭 listener。每收到一条就记录 `(dest, test_seq, idem_key, recv_ns)`。
- **faultbench**：按场景 YAML 的时间轴执行，最后调用 reconcile。
- **reconcile**：期望集合 `E_d = acked ∩ routed_to(d)`。输出：
  - lost = |E_d \ uniq(R_d)|；
  - dup（raw 和 stored 分别统计）；
  - dlq；
  - extra（未 ack 但送达，不算错）；
  - recovery_s：故障解除后首次满足 E_d ⊆ R_d 的时间；
  - 健康 sink 在故障期间的 p50/p99。

### 10.2 确定性
- 固定 seed、内容、速率和时间轴。
- 进程故障用 SIGKILL 或 `docker kill -s KILL` 注入。
- 单元层用 FaultFS（ENOSPC / 延迟）和 FakeClock，不用 sleep。
- 端到端测试每个场景跑 5 次，报告均值和 95% CI，并附原始 CSV。在 Linux（Docker / WSL2）上运行。

### 10.3 场景
三个 sink：archive(file)、siem(sqlite)、soar(mock webhook)。默认负载 2000 eps × 5 分钟。

| # | 场景 | 时间轴 | 通过标准 |
|---|---|---|---|
| S1 | sink 宕机 | 30s 时 soar down，150s 时恢复 | lost=0；archive/siem 的 dup=0；记录恢复时间；健康 sink p99 增幅 < 20% |
| S2 | 慢 sink | soar 慢 500ms（超时 2s），持续 120s | lost=0；健康 sink p99 增幅 < 20%；L1 涨起来后回落到 0 |
| S2b | 超过超时 | 慢 3s，超时 2s | lost=0；soar 的 dup>0 属于预期，如实报告 |
| S3 | kill -9 | 60s 时 SIGKILL，70s 时重启，共 3 次 | lost=0；dup ≤ 理论上界；sqlite stored dup=0 |
| S3b | 优雅重启 | SIGTERM | lost=0，dup=0 |
| S4 | 注销 / 替换 | 60s 注销 soar，120s 重新注册；180s 对 siem 执行 Replace(v2) | lost=0；Replace 期间 `l1_pending{siem}`=0 |
| S4b | 孤儿 TTL | orphan_ttl=30s，注销后不恢复 | 期望事件全部进入 DLQ（原因为 orphan），lost=0 |
| S5 | 磁盘满 | data_dir 挂 64MB tmpfs，同时 soar down | 先反压返回 429/503；已 ack 的 lost=0；GC 后恢复 |
| S6 | 不可重试错误 | soar 对 10% 请求返回 400 | DLQ 中的事件集合与 mock 日志一致；其余 lost=0 |

报告路径：`bench/results/<date>/fault.md`。

### 10.4 对照模式（可选）
在 Vector（disk buffer + e2e ack）和 Fluent Bit（filesystem storage）上，用 docker-compose 只跑 S1 和 S3。结论只写成"在公开配置 X 下观测到 Y"，并附完整配置。

## 11. 性能基准
**测试矩阵**：
- 事件大小：200B / 1KB / 4KB
- 脱敏：关闭 / 开启（10%）
- sink：null / sqlite

**两种模式**：
- 闭环：测最大吞吐；
- 开环：分别跑最大吞吐的 50% 和 80%，测延迟（HdrHistogram，p50/p99/p999）。

**采样**：CPU、RSS、goroutine 数、磁盘占用，每 5s 一次。

**流程**：每个测试点预热 60s，正式测 5 分钟，重复 5 次。

**Soak**：50% 负载下持续运行 ≥ 30 分钟，第 10 分钟注入 S1。验收：
- RSS 斜率约为 0；
- 磁盘占用回到基线；
- goroutine 和 fd 数不增长。

**环境记录**：CPU、内存、磁盘、OS/内核、Go 版本、commit、配置。

## 12. 仓库结构
```
loggate/
├── cmd/{loggate,loggatectl,loadgen,mocksink,faultbench,reconcile,piigen,piieval}/
├── api/loggate/v1/admin.proto
├── internal/{event,config,ingest/{http,syslog,filetail},normalize/{ocsf,parsers},mask/{engine,detectors,policy,level},
│             route,wal,state,dispatch,worker,delivery,dlq,sink/{file,sqlite,postgres,webhook,kafka,null},admin,metrics,clock}/
├── web/  bench/{scenarios,results}/  scripts/plot.py
├── deploy/{docker-compose.yml,compare/}
└── docs/{DESIGN.md,BENCHMARK.md,LIMITATIONS.md}
```

依赖：
- 存储与数据库：bbolt、modernc.org/sqlite、pgx
- 熔断与限流：gobreaker、x/time/rate
- 可观测与通信：prometheus/client_golang、grpc
- 配置：yaml.v3、validator
- 统计：hdrhistogram-go

## 13. 里程碑（约 12 个工作日）
| 步 | 内容 | 验收 |
|---|---|---|
| M1 | event、config、HTTP ingest、WAL（分段 / CRC / 组提交 fsync / 截断撕裂尾）、file sink、单目的地直投 | WAL 单测（含 FaultFS）；断言 ack 在 fsync 之后；kill -9 后已 ack 的事件不丢 |
| M2 | Dispatcher、Worker/L2、Tracker、checkpoint、L1 位图、Park/Ack/Fail、redeliver、退避、breaker、DLQ、恢复、GC；sqlite 和 webhook sink | 两个 sink 一个失败时，健康 sink 精确收到一次；重启恢复；GC 后磁盘占用回落 |
| M3 | loadgen、mocksink、faultbench、reconcile；S1、S2、S2b、S3、S3b、S6 | `make faultbench` 一键出报告；lost=0 |
| M4 | OCSF、脱敏引擎、分级 / max_level、piigen、piieval、基准 | 身份证、银行卡的 P/R 以 ≥ 0.99 为目标（如实报告）；误报相比基线明显下降；不含 PII 时 0 allocs |
| M5 | 生命周期 / Replace / 孤儿 TTL、gRPC admin + loggatectl、Prometheus、反压、优雅关闭；S4、S4b、S5 | 全部场景通过；非法状态转换返回错误 |
| M6 | 基准矩阵、30 分钟 soak、dashboard / demo、README / Limitations；（可选）对照 | BENCHMARK.md 含环境、配置、CI、原始数据；3 分钟走完 demo |

## 14. 测试清单
**单元测试**：
- WAL：往返读写、CRC、撕裂尾、滚动、ENOSPC 时不 ack
- tracker：乱序 Resolve；某事件不含 d 时不阻塞 d 的水位
- L1：合并、清位删除、segRef、due、预算转 Dead、陈旧索引
- 恢复：表驱动用例
- GC
- delivery：Classify、jitter 上下界、Retry-After
- 生命周期：合法 / 非法状态转换、Replace 回滚
- 脱敏：
  - MOD 11-2 向量（含 X）、闰年 0229、Luhn 向量、数字边界、区间重叠、fail-closed
  - 用 `go test -fuzz`：不 panic，输出中不残留校验通过的 PII
- 归一化：sshd、iptables golden + schema 校验
- 路由：规则优先级、max_level

**集成测试**（FakeClock + 进程内 mocksink）：
- 只重投失败的目的地
- breaker 打开时 Park，half-open 时探测
- 优雅关闭后 dup=0
- 孤儿 TTL 进 DLQ
- 返回 429

**端到端**：S1–S6 由 faultbench 执行，nightly 或手动触发。

## 15. 风险与局限
1. 单机单盘，没有复制；`fsync=none` 等于放弃 G1。
2. syslog UDP 有损；TCP 没有应用层 ack。只有 HTTP 和 file tail 能做到 ack 即持久化。
3. effectively-once 依赖下游幂等。file 和 webhook 在超时有歧义时会重复（S2b 会测出来）。内容哈希还会合并完全相同的合法事件。
4. 不保证顺序。
5. 长时间故障时，Park 每个事件都要写一次 bbolt，L1 线性增长。未来可以为落后的目的地切换到 WAL 游标模式。
6. bbolt 单写者，写放大明显；接口已隔离，可以换 Pebble。
7. 目的地最多 64 个。
8. kill -9 后重复有上界，但不为 0。
9. 脱敏只覆盖格式可识别的 PII：姓名、地址识别不了；手机号误报偏高；HMAC 换密钥后无法关联历史数据；结果不可逆。
10. 分级标签不等于合规判定。
11. Replace 只在本项目场景里验证过，不外推。
12. 评测：端到端有时序抖动，用重复运行和 CI 缓解；正式数据在 Linux 上采集。

## 16. Demo
`loggate --demo` 会在进程内启动 mocksink 和 loadgen，只用合成数据。dashboard 在 `:8081/ui`，用 SSE 每秒刷新：
1. **计数器**：ingested、acked、各目的地的 delivered、L1 pending、DLQ、反压状态
2. **目的地表**：生命周期、breaker、L2/L1、p99、最近错误
   - 每行有 Kill sink / Slow 500ms / Restore 按钮（仅 demo 模式可用）
   - 还有 Unregister / Replace v2 按钮
3. **对账面板**：lost / dup，lost=0 时高亮
4. **脱敏样例**：最近 20 条事件的脱敏前后对比，以及分级标签

**3 分钟演示流程**：
1. 正常流量；
2. Kill soar，看 L1 上涨，而 archive 和 siem 不受影响；
3. 对网关 kill -9 后重启，lost=0；
4. Restore，L1 回落到 0，看到恢复时间；
5. 查看脱敏样例和 DLQ。
