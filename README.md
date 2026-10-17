# LogGate

LogGate 是一个用 Go 编写的单机安全日志采集网关。它的目标是：已经 ack 的日志不丢；某个目的地失败时，只重投这个目的地；借助幂等键，在幂等 sink 上做到 effectively-once。丢失和重复用一套可复现的故障注入基准来量化。

设计见 [docs/DESIGN.md](docs/DESIGN.md)（§0.5 的 v0.2 修订优先于正文），红队评审见 [docs/REVIEW_01_红队评审.md](docs/REVIEW_01_红队评审.md)，实现与设计的偏差见 [docs/IMPLEMENTATION_NOTES.md](docs/IMPLEMENTATION_NOTES.md)。

## 当前状态：M3（故障注入基准）

### M1（已完成）
- **WAL**：分段追加、CRC32C、单 writer 分配 Seq、组提交 fsync；fsync 成功后才 ack；打开时截断撕裂尾部；write/fsync 出错时毒段 + 滚段，本批不 ack。
- **HTTP ingest**：`POST /v1/events`，fsync 后返回 200；Bearer token 绑定 SourceID。
- **幂等键**：网关分配 `HMAC-SHA256(node | boot_epoch | seq)[:16]`，写入 WAL。

### M2（已完成）
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

### M3（本次）
- **mocksink**（`cmd/mocksink`）：一个进程托管多个具名 webhook 接收方，控制端口 `POST /fault {dest, mode: ok|down|slow, delay_ms, error_code, error_rate, seed}`。`down` 关闭 listener 和所有连接（客户端得到连接失败，不是 5xx）；`slow` 延迟后照常处理（客户端超时也会落日志，这正是 S2b 重复的来源）；错误注入由 `(seed, 请求序号)` 决定，可复现。每个事件记一行 CSV 日志（dest、test_seq、idem_key、到达时间、计划发送时间、状态码）。
- **loadgen**（`cmd/loadgen`）：开环定速，延迟从**计划发送时刻**算起（避免 coordinated omission），客户端落后会计数并报告；每条事件带 run_id、单调 test_seq、计划发送时间、路由用的 class，Bearer token 认证；只有 HTTP 200 进入"承诺集合"，429/503/错误分开记录，默认不重试；ack 延迟用 HdrHistogram。
- **reconcile**（`cmd/reconcile`）：独立 oracle，`E_d = acked ∩ routed_to(d)`，路由表来自场景文件、由 reconcile 自己求值。读取 mocksink 日志、file sink（容忍撕裂行）、sqlite 行和网关 DLQ，按目的地输出 expected、lost、dup_raw、dup_stored、dlq、extra、misrouted、recovery_s、基线/故障中/故障后的 p50/p99/p999，JSON + markdown。
- **faultbench**（`cmd/faultbench`）：按场景 YAML 启动 mocksink、网关（独立进程，生成配置和全新 data dir）、loadgen，按时间轴注入故障（sink down/slow/错误率、硬杀网关、重启、优雅停止、注销/注册目的地），等网关排空后对账；重复 N 次，输出均值和 95% CI（t 分布）、原始 CSV 和环境信息（OS、CPU、Go 版本、commit、配置）。
- **场景**（`bench/scenarios/`）：S0 基线、S1 sink 宕机、S2 慢 sink、S2b 超过超时、S3 kill -9（3 个周期）、S3b 优雅重启、S4b 注销 → 孤儿 TTL → DLQ、S5 磁盘满（仅 Linux）、S6 10% 不可重试错误。拓扑为 archive(file)、siem(sqlite)、soar(webhook → mocksink)，另加始终健康的 probe(webhook → mocksink) 用来在独立接收方测健康 sink 的延迟。
- **网关新增**：token 保护的 admin API（`POST /v1/admin/{shutdown,unregister,register}`，Windows 上优雅停止靠它）；最小磁盘配额（`disk.max_bytes`，统计 WAL + state.db + DLQ，超出返回 429）；file sink 启动时补齐被 kill 截断的半行。

已知缺口（详见实现说明 §4、§9）：最终数字必须在 Linux 上跑 full profile；S5 尚未运行；没有 Prometheus、ENOSPC 只读模式；sweeper 全量扫描 L1；还没有脱敏和 OCSF；race detector 未在本机运行。

### 基准结果

**Windows quick profile 冒烟结果**：[bench/results/20261017-234601-windows-quick/fault.md](bench/results/20261017-234601-windows-quick/fault.md)（commit `1adcd08`，200 eps × 60s，每场景 1 次）。

> 这是 Windows 上的冒烟测试，不是最终数字：单次重复、负载只有 full 的 1/10，loadgen、mocksink、网关同机运行；kill 用 TerminateProcess，优雅停止走 admin API。最终数字将来自 Linux 上的 full profile（2000 eps × 5 分钟 × 5 次，带 95% CI）。S5（磁盘满）只在 Linux 上运行，本次为 SKIPPED。

| 场景 | 结果 | 丢失（全部目的地） | 重复（dup_raw） | DLQ | soar 恢复时间 |
|---|---|---:|---|---:|---:|
| S0 基线 | PASS | 0 | 0 | 0 | - |
| S1 soar 宕机 24s | PASS | 0 | 0 | 0 | 6.56s |
| S2 soar 慢 500ms | PASS | 0 | 0 | 0 | 0.01s |
| S2b soar 慢 3s（超时 2s） | PASS | 0 | soar 908（预期；同一 idem_key，dup_stored=0） | 0 | 8.49s |
| S3 kill 3 次 | PASS | 0 | archive 461、probe 361、soar 240；siem 存储重复 0 | 0 | 0.12s |
| S3b 优雅重启 | PASS | 0 | 0 | 0 | 0.31s |
| S4b 注销 soar + 孤儿 TTL 30s | PASS | 0 | 0 | soar 4902（orphan_ttl_expired） | - |
| S6 soar 10% 返回 400 | PASS | 0 | 0 | soar 241，与 mocksink 的 4xx 日志完全一致 | - |

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

## 故障注入基准

```powershell
# Windows：构建 5 个二进制并运行全部场景（quick：200 eps、约 60s、1 次）
.\scripts\faultbench.ps1
.\scripts\faultbench.ps1 -Scenarios S1,S3 -BenchProfile quick
```

```bash
# Linux：quick 冒烟；full = 2000 eps × 5 分钟 × 5 次（最终数字）
scripts/faultbench.sh
PROFILE=full REPS=5 scripts/faultbench.sh
# S5 的真实 ENOSPC：data_dir 放到 64MB tmpfs
sudo mount -t tmpfs -o size=64m tmpfs /mnt/lgdata
LOGGATE_BENCH_DATA_ROOT=/mnt/lgdata scripts/faultbench.sh S5

# 单独重新对账某次重复
bin/reconcile -dir bench/results/<run>/raw/S1/rep1
```

结果写到 `bench/results/<时间戳>-<os>-<profile>/`：`fault.md`（汇总表）、`<场景>.json`（每次重复的完整对账结果 + 聚合 + 环境）、`summary.csv`（每场景 × 重复 × 目的地一行）、`raw/`（原始日志和数据目录，不进 git）。

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
cmd/mocksink/          基准：mock webhook 接收方
cmd/loadgen/           基准：开环负载
cmd/reconcile/         基准：独立对账
cmd/faultbench/        基准：场景编排
bench/scenarios/       故障场景 YAML
bench/results/         基准结果
scripts/               构建并运行基准的脚本
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
internal/gateway       组装各组件（含 admin API、磁盘配额），main 和端到端测试共用
internal/bench/        基准实现：scenario、loadgen、mocksink、journal、reconcile、stats、faultbench
```

## License

MIT
