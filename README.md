# LogGate

LogGate 是一个用 Go 编写的单机安全日志采集网关。它的目标是：已经 ack 的日志不丢；某个目的地失败时，只重投这个目的地；借助幂等键，在幂等 sink 上做到 effectively-once。丢失和重复用一套可复现的故障注入基准来量化。

设计见 [docs/DESIGN.md](docs/DESIGN.md)（§0.5 的 v0.2 修订优先于正文），红队评审见 [docs/REVIEW_01_红队评审.md](docs/REVIEW_01_红队评审.md)，实现与设计的偏差见 [docs/IMPLEMENTATION_NOTES.md](docs/IMPLEMENTATION_NOTES.md)。

## 当前状态：M2（可靠投递主链路）

### M1（已完成）
- **WAL**：分段追加、CRC32C、单 writer 分配 Seq、组提交 fsync；fsync 成功后才 ack；打开时截断撕裂尾部；write/fsync 出错时毒段 + 滚段，本批不 ack。
- **HTTP ingest**：`POST /v1/events`，fsync 后返回 200；Bearer token 绑定 SourceID。
- **幂等键**：网关分配 `HMAC-SHA256(node | boot_epoch | seq)[:16]`，写入 WAL。

### M2（本次）
- **Dispatcher**：只消费已 fsync 的记录，按 seq 顺序；对每个目标先 `tracker.Add`，再非阻塞入队 L2，入不了就 Park 到 L1（原因 `NoDest / NotActive / BreakerOpen / QueueFull`，都不计重试次数）；一个事件的所有 Add 完成后才发布 dispatch 游标。
- **AckTracker + checkpoint**：每目的地水位 = `min(最早未解决 seq − 1, 游标)`，没有流量的目的地随游标推进；只有在 sink 成功、L1 提交或 DLQ fsync 之后才 Resolve；每 `checkpoint_interval` 持久化，永不回退。
- **L1（bbolt）**：`l1`、`due/<dest>`、`ckpt`、`registry`、`meta` 五个 bucket；状态位图 `Owned/Pending/Acked/Dead` + 每目的地 Slot + WAL 指针（不复制事件体）；单 writer 自行把操作攒进一个 `db.Update`，提交后才回调；Pending 清空即删记录；segRef 在内存维护、启动时重建。
- **Worker / L2**：每目的地一个有界队列（条数 + 字节）、批量（≤256 条或等 20ms）、熔断器（half-open 时新事件也放行）；逐条处理结果：成功 → Resolve / L1 Ack；NonRetryable → 先写 DLQ（fsync）再 Resolve / L1 Dead；Retryable / Throttled → Park / L1 Fail（full-jitter 退避，Throttled 取 `max(Retry-After, 退避)`）；预算（`max_attempts` / `max_age`）用尽进 DLQ。
- **Redeliver**：每目的地每 200ms 扫描到期的 `due/<dest>`，只重投 Pending 位；L2 中为重投预留至少 `redeliver_share`（≥ 20%）的容量（策略见实现说明 §1-3）。
- **生命周期**：Registered → Active → Draining → Unregistered（v1 不做 Replace）。Draining 时在途批次正常完成，L2 剩余 Park。运行时 `Gateway.Register / Unregister`（Go API）。
- **全局 L1 sweeper**：与目的地状态无关，超过 `orphan_ttl`（非 Active 的目的地）或 `max_age` 的记录先写 DLQ 再标 Dead。
- **DLQ**：追加写分段日志（`len | crc32c | JSON`），包含目的地、原因、尝试次数、原始 seq、idem_key 和事件体副本；fsync 之后才提交 L1 / Resolve。
- **崩溃恢复**：nextSeq 大于所有 ckpt 和 L1 中出现过的 seq；WAL 恢复只校验 `min(ckpt)` 之后的段；从 `min(ckpt)+1` 重放，跳过 `seq ≤ ckpt[d]` 和 L1 已负责的 `(seq, d)`；重放完再启动 redeliver，最后开放 ingest。
- **段 GC**（每 10s）：段内 seq 全部 ≤ 所有目的地的最小持久化 ckpt、segRef = 0、且不是最新段时删除；Windows 上有读者打开时延迟删除。
- **sink**：file（非幂等）、sqlite（`modernc.org/sqlite`，`ON CONFLICT(idem_key) DO NOTHING`，幂等）、webhook（JSON 数组 + `Idempotency-Keys` 头，或单条 + `Idempotency-Key`；5xx 重试、4xx 进 DLQ、429 限流）。
- **路由**：首个匹配生效，match 是入站 JSON 顶层字段的等值比较；可以引用未配置的目的地（预留 ID，事件 Park 等它出现或过 `orphan_ttl` 进 DLQ）。DestID 持久化在 registry，重启、调整配置顺序都不变。
- **反压**：`l1.on_quota: block_all`（默认）时，任一目的地 L1 积压超过 `max_records_per_dest`，ingest 返回 429 + `Retry-After`；`dlq_overflow` 时超额事件进 DLQ。
- **统计**：`GET /v1/stats` 返回每目的地状态、熔断、L2 深度、L1 pending、已投递、错误、DLQ、水位和 checkpoint。

M2 的已知缺口（详见实现说明 §4）：没有 Prometheus 和 admin API；没有磁盘配额 / ENOSPC 只读模式；sweeper 全量扫描 L1；还没有脱敏和 OCSF（WAL、L1、DLQ 中是未脱敏数据）；race detector 未在本机运行。

目前还没有任何性能数据，后面的基准实测之后才会补上。

## 运行

需要 Go 1.27 及以上。

```bash
go build -o bin/loggate ./cmd/loggate
export LOGGATE_IDEM_KEY='至少16字节的随机串'   # 不设置时，每个进程随机生成一个
./bin/loggate -config configs/example.yaml

curl -s localhost:8080/healthz
curl -s -XPOST localhost:8080/v1/events -d '[{"class_uid":3002,"msg":"sshd: Failed password for root"},{"msg":"ok"}]'
# {"accepted":2,"first_seq":1}
curl -s localhost:8080/v1/stats
tail -n 2 out/archive.jsonl
```

数据目录：`data/wal/`（WAL 段）、`data/state.db`（bbolt：L1、checkpoint、registry）、`data/dlq/`（死信）。

收到 SIGINT 或 SIGTERM 时优雅关闭：停止接收 HTTP、WAL 刷盘、dispatcher 处理完已提交事件、各目的地完成在途批次并把 L2 剩余事件 Park、写最终 checkpoint。重启后不重放、不重复。超时用 `-shutdown-timeout` 设置，默认 30s。

## 测试

```bash
go vet ./...
go test ./... -count=1
```

覆盖内容（详见实现说明 §3）：
- **WAL**：往返、CRC、撕裂尾、滚段、毒段、ENOSPC 不 ack、ack 在 fsync 之后、延迟删除、ScanFrom。
- **tracker / state / delivery / worker 单元测试**：乱序 Resolve、无流量目的地、L1 合并与清位删除、segRef、due 与陈旧索引、提交后回调、ckpt 单调、registry 稳定 ID、HTTP 状态分类、jitter 上下界、熔断、份额策略（FakeClock）。
- **集成测试**（进程内 fake sink + 真实 file/sqlite）：两个 sink 一个失败时健康 sink 精确一次、失败 sink 恢复后精确一次且 L1 归零；NonRetryable 与预算耗尽进 DLQ；孤儿 TTL；Draining 不丢；GC；429 反压；优雅重启 dup=0。
- **崩溃恢复**：在"WAL 提交后、派发前""Park 提交后""sink 成功后、checkpoint 前"三个点模拟崩溃，以及故障期间连续崩溃两次：每个已 ack 事件到达每个目标至少一次，sqlite 中没有重复行。

本机（Windows）没有 gcc，跑不了 `-race`；并发正确性依赖的不变量列在实现说明 §2。

## 目录

```
cmd/loggate/           主程序
configs/example.yaml   示例配置（未知键会报错）
internal/event         LogEvent、DestMask、IdemKey
internal/config        YAML 配置、默认值和校验
internal/clock         Clock 接口、真实时钟和 FakeClock
internal/wal           WAL、FS 抽象、FaultFS、段引用计数
internal/state         bbolt：L1 位图、due 索引、checkpoint、registry、claims
internal/tracker       AckTracker 与 dispatch 游标
internal/delivery      错误分类、退避、重试预算、熔断器
internal/dlq           死信日志
internal/worker        目的地槽位：L2、消费者、结果处理、redeliver、生命周期
internal/dispatch      Dispatcher、恢复重放、checkpoint、sweeper、段 GC
internal/ingest/http   HTTP 输入（含 429 反压、/v1/stats）
internal/route         路由（首个匹配，顶层字段等值）
internal/sink          Sink / Envelope 接口
internal/sink/file     file sink
internal/sink/sqlite   sqlite sink（幂等）
internal/sink/webhook  webhook sink
internal/gateway       组装各组件，main 和端到端测试共用
```

## License

MIT
