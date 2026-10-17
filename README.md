# LogGate

LogGate 是一个用 Go 编写的单机安全日志采集网关。它的目标是：已经 ack 的日志不丢；某个目的地失败时，只重投这个目的地；借助幂等键，在幂等 sink 上做到 effectively-once。丢失和重复用一套可复现的故障注入基准来量化。

设计见 [docs/DESIGN.md](docs/DESIGN.md)（§0.5 的 v0.2 修订优先于正文），红队评审见 [docs/REVIEW_01_红队评审.md](docs/REVIEW_01_红队评审.md)，实现与设计的偏差见 [docs/IMPLEMENTATION_NOTES.md](docs/IMPLEMENTATION_NOTES.md)。

## 当前状态：M4（OCSF 归一化 + PII 脱敏 + 分级路由 + 合成标注评测）

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

### M3（已完成）
- **mocksink**（`cmd/mocksink`）：一个进程托管多个具名 webhook 接收方，控制端口 `POST /fault {dest, mode: ok|down|slow, delay_ms, error_code, error_rate, seed}`。`down` 关闭 listener 和所有连接（客户端得到连接失败，不是 5xx）；`slow` 延迟后照常处理（客户端超时也会落日志，这正是 S2b 重复的来源）；错误注入由 `(seed, 请求序号)` 决定，可复现。每个事件记一行 CSV 日志（dest、test_seq、idem_key、到达时间、计划发送时间、状态码）。
- **loadgen**（`cmd/loadgen`）：开环定速，延迟从**计划发送时刻**算起（避免 coordinated omission），客户端落后会计数并报告；每条事件带 run_id、单调 test_seq、计划发送时间、路由用的 class，Bearer token 认证；只有 HTTP 200 进入"承诺集合"，429/503/错误分开记录，默认不重试；ack 延迟用 HdrHistogram。
- **reconcile**（`cmd/reconcile`）：独立 oracle，`E_d = acked ∩ routed_to(d)`，路由表来自场景文件、由 reconcile 自己求值。读取 mocksink 日志、file sink（容忍撕裂行）、sqlite 行和网关 DLQ，按目的地输出 expected、lost、dup_raw、dup_stored、dlq、extra、misrouted、recovery_s、基线/故障中/故障后的 p50/p99/p999，JSON + markdown。
- **faultbench**（`cmd/faultbench`）：按场景 YAML 启动 mocksink、网关（独立进程，生成配置和全新 data dir）、loadgen，按时间轴注入故障（sink down/slow/错误率、硬杀网关、重启、优雅停止、注销/注册目的地），等网关排空后对账；重复 N 次，输出均值和 95% CI（t 分布）、原始 CSV 和环境信息（OS、CPU、Go 版本、commit、配置）。
- **场景**（`bench/scenarios/`）：S0 基线、S1 sink 宕机、S2 慢 sink、S2b 超过超时、S3 kill -9（3 个周期）、S3b 优雅重启、S4b 注销 → 孤儿 TTL → DLQ、S5 磁盘满（仅 Linux）、S6 10% 不可重试错误。拓扑为 archive(file)、siem(sqlite)、soar(webhook → mocksink)，另加始终健康的 probe(webhook → mocksink) 用来在独立接收方测健康 sink 的延迟。
- **网关新增**：token 保护的 admin API（`POST /v1/admin/{shutdown,unregister,register}`，Windows 上优雅停止靠它）；最小磁盘配额（`disk.max_bytes`，统计 WAL + state.db + DLQ，超出返回 429）；file sink 启动时补齐被 kill 截断的半行。

### M4（本次）
- **OCSF 归一化**（`internal/normalize`，OCSF 1.3.0 子集）：sshd 的 `Accepted|Failed <method> for [invalid user ]U from IP port P ssh2`、`Connection closed by ...`、`Disconnected from ...` → Authentication **3002**（activity_id 1 Logon / 2 Logoff，status_id 1/2，user.name，src_endpoint.ip/port，auth_protocol，`type_uid = 300200 + activity_id`，category_uid 3）；`event.type = auth` 的 JSON（嵌套或扁平键）→ 3002，已映射的键从 unmapped 中移除；其余 → Base Event **0**（文本行放 raw_data，JSON 整个对象放 unmapped）。公共字段 time（ms）、severity_id、metadata（version、product.name = LogGate、log_name）。4001 按 §0.5 不在 v1 范围。golden 文件测试 + OCSF 子集 JSON Schema 校验（`santhosh-tekuri/jsonschema/v6`，只在测试里用）。
- **文本输入**：`POST /v1/events` 带 `Content-Type: text/plain` 时按行切分，每行一个事件。syslog TCP 未做（见缺口）。
- **脱敏引擎**（`internal/mask`）：字段策略 trie（`keep | drop | hash | mask | scan`，默认 scan，路径支持 `*`，数组透明）；hash = HMAC-SHA256 取前 16 字节 hex，密钥来自环境变量；scan 遍历整棵树（含 raw_data、unmapped），字符串和 json.Number 都扫。预过滤（数字 ≥ 11 个才跑数字扫描，有 `@` 才跑 e-mail RE2）+ 手写零分配数字串扫描器，严格边界（前后不能紧挨 ASCII 字母数字；`%XX` URL 转义视为边界）。
  - **身份证**：18 位、末位 `[0-9Xx]`；ISO 7064 MOD 11-2；出生日期 1900-01-01..今天（含闰年）；省级代码白名单。掩码 `110105********123X`。
  - **银行卡**：16–19 位，可按 4 位一组用同一个空格或 `-` 分隔；**BIN 前缀白名单（可配置，默认 62、4、51-55、2221-2720、34、37、35）且 Luhn 都必须通过**。掩码保留前 4 后 4。
  - **手机号**：`(+86|86)?1[3-9]\d{9}` + 号段表（span 只含 11 位本号）。掩码 `138****5678`。
  - **E-mail**：只在含 `@` 时跑 RE2。掩码 `z***@example.com`。
  - 区间冲突：身份证 > 银行卡 > 手机号 > e-mail，同级取较长者。掩码后再扫一遍直到干净（最多 4 轮），用来处理"相邻卡号窗口在第一张卡掩码后才出现"的情况。
  - **fail-closed（出错时）**：某个值脱敏 panic 或不收敛 → 整个值换成 `[REDACTED:error]` 并计数；`masking.default: drop` 可把未列出的字段整体丢弃。
  - **标签与分级**：DataClass `pii.id_card / pii.bank_card / pii.phone / pii.email`；DataLevel 映射可配置，默认任何 PII → 一般，同一事件同时有身份证和银行卡 → 重要，核心只能显式配置。
- **脱敏位置**：在 HTTP handler 里、WAL append **之前**完成（解析 → 归一化 → 脱敏 → 路由 → WAL），所以 WAL、L1 引用的记录、DLQ 都只有脱敏后的事件。400 错误只报偏移和序号，不回显请求内容（encoding/json 的错误串会引用输入字节，已替换）；日志不打印事件内容。
- **分级路由**：每个目的地可设 `max_level`，DataLevel 超出的事件从该目的地的 targets 中去掉并计数（`/v1/stats` 的 `route_level_blocked`）。路由 match 先查 OCSF 顶层字段（如 `class_uid`），再查 unmapped 顶层键。
- **基准配置**：faultbench 的 `_base.yaml` 把 `unmapped.test_seq / run_id / scheduled_send_ns` 设为 keep；脱敏开启后重跑 quick S0+S1，lost 全部为 0（[结果](bench/results/20261018-m4-windows-quick-S0S1/fault.md)）。
- **piigen / piieval**：确定性合成标注集（span 级字节偏移）+ 评测，见下文。

已知缺口（详见实现说明 §4、§9、§12）：最终故障数字必须在 Linux 上跑 full profile；S5 尚未运行；没有 Prometheus、ENOSPC 只读模式；sweeper 全量扫描 L1；syslog TCP 输入未做；race detector 未在本机运行。

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

### PII 检测评测（合成数据）

结果：[bench/results/20261018-002220-pii/piieval.md](bench/results/20261018-002220-pii/piieval.md)（含误报原因、漏报变体、错误样例）。数据集：`piigen -seed 20261017 -n 10000`，10000 条（sshd / nginx / 应用 JSON / 中英文工单四种模板），16547 个计分正样本、约 3.2 万个困难负样本（校验位错、日期非法如 0230、省码非法、Luhn 失败、**Luhn 通过但无合法 BIN 的 19 位雪花 ID**、13 位毫秒时间戳、订单号、trace id、10–12 开头的 11 位数、未分配号段、版本号、随机 16–19 位数字 ID）。span 级**精确匹配**（起止和类型都要一致）。两个基线：`regex_naive` = `\d{17}[\dXx]`、`\d{16,19}`、`1[3-9]\d{9}`、e-mail 正则，无边界无校验；`regex_bounded` = 同样的正则两侧加 `\b`。三者用同样的重叠优先级。

| 系统 | 类型 | P | R | F1 | FP |
|---|---|---:|---:|---:|---:|
| **LogGate** | id_card | 1.0000 | 1.0000 | 1.0000 | 0 |
| | bank_card | 0.9780 | 1.0000 | 0.9889 | 83 |
| | phone | 1.0000 | 1.0000 | 1.0000 | 0 |
| | email | 1.0000 | 1.0000 | 1.0000 | 0 |
| | **micro** | **0.9950** | **1.0000** | **0.9975** | **83** |
| regex_naive | id_card | 0.2013 | 0.9294 | 0.3309 | 11234 |
| | bank_card | 0.3633 | 0.4736 | 0.4112 | 3063 |
| | phone | 0.5862 | 1.0000 | 0.7392 | 4512 |
| | email | 1.0000 | 1.0000 | 1.0000 | 0 |
| | **micro** | 0.4334 | 0.8696 | 0.5785 | 18809 |
| regex_bounded | id_card | 0.4681 | 0.8004 | 0.5907 | 2770 |
| | bank_card | 0.3345 | 0.6760 | 0.4475 | 4965 |
| | phone | 0.9364 | 0.9032 | 0.9195 | 392 |
| | email | 1.0000 | 1.0000 | 1.0000 | 0 |
| | **micro** | 0.6348 | 0.8536 | 0.7281 | 8127 |

怎么读这张表：
- **误报**从 18809 / 8127（两个基线）降到 83。LogGate 的 83 个误报全是银行卡：71 个是"像身份证但校验失败"的 18 位负样本，它们的省码前缀恰好落在卡 BIN 里（41–46 → Visa `4`，51–54 → Mastercard，62 → 银联，34/35/37 → Amex/JCB），又有约 1/10 能碰巧通过 Luhn；另外 12 个是随机 16–19 位数字 ID 恰好通过 BIN + Luhn（678 个里占 1.8%）。这是 BIN + Luhn 方案固有的误报，没有针对数据集调参去掉。
- **召回 1.0 不说明覆盖了真实世界的格式**：生成器和检测器共享同一套格式假设（分隔方式、边界规则），合成集上的召回主要验证实现正确。e-mail 一行三者相同，因为用的是几乎相同的正则。
- 基线的漏报来自分组卡号（`6222 0212 ...`）、19 位卡被 `\d{17}[\dXx]` 先吃掉、`%3D` / `+86` 前缀让 `\b` 失效。
- **不计分的已知局限**（单独列出）：全角数字 0/263、URL 编码的 `%40` 邮箱 0/418、紧贴 ASCII 字母的号码（如 `tel13812345678`）0/107——最后一项 naive 正则能找到 107/107，这是严格边界换低误报的代价。
- **残留检查**：16547 个正样本在 LogGate 对整行 `MaskString` 后原文残留 0 个；2522 条 JSON 记录走 `MaskEvent`（解码后整棵树）再编码，残留 0 个；`[REDACTED:error]` 0 次。

### 脱敏吞吐（testing.B）

原始输出：[bench/results/20261018-002220-pii/bench.txt](bench/results/20261018-002220-pii/bench.txt)（AMD Ryzen 9 5900HX，Windows 11，go1.27.0，单 goroutine，`-count 3`，下表取中间值）。`MaskEvent` 的输入是一个 OCSF Base Event，unmapped 里是若干 32 字节字符串字段，PII 密度 = 含一个 PII 值的字段比例（括号里是实际个数）；MB/s 只按被扫描的字符串字节算。PII > 0 的用例每轮还包括把被改写的字段恢复原值。

| 基准 | 大小 | PII 密度 | ns/op | MB/s | B/op | allocs/op |
|---|---:|---|---:|---:|---:|---:|
| MaskEvent | 200B | 0%（0/6） | 785 | 245 | 0 | 0 |
| MaskEvent | 200B | 10%（1/6） | 1082 | 177 | 64 | 2 |
| MaskEvent | 200B | 50%（3/6） | 1365 | 141 | 192 | 6 |
| MaskEvent | 1KB | 0%（0/32） | 2872 | 357 | 0 | 0 |
| MaskEvent | 1KB | 10%（3/32） | 4228 | 242 | 192 | 6 |
| MaskEvent | 1KB | 50%（16/32） | 11537 | 89 | 1258 | 44 |
| MaskEvent | 4KB | 0%（0/128） | 10440 | 392 | 0 | 0 |
| MaskEvent | 4KB | 10%（13/128） | 20073 | 204 | 1011 | 35 |
| MaskEvent | 4KB | 50%（64/128） | 45760 | 90 | 5036 | 176 |
| MaskString（一整行） | 1KB | 无 PII | 1753 | 584 | 0 | 0 |
| MaskString（一整行） | 1KB | 2 个 PII | 3758 | 272 | 1152 | 1 |
| 解码+归一化+编码 1KB JSON | 1KB | 10%，脱敏关 | 25643 | 52 | 15356 | 145 |
| 解码+归一化+编码 1KB JSON | 1KB | 10%，脱敏开 | 30234 | 44 | 15552 | 151 |

- 不含 PII 时 0 allocs（`MaskEvent` 和 `MaskString` 都是），有 `TestMaskEventNoAllocWithoutPII` 守住。
- 设计目标"1KB、10% 密度单核 ≥ 50MB/s"：实测 242 MB/s。整条摄入路径里，脱敏让 1KB 事件的 解码 + 归一化 + 编码 慢了约 18%（25.6 → 30.2 µs）；大头是 encoding/json。
- 每个 PII 字段 2 次分配：新字符串 + 装箱成 `any`。

### Fuzz

`FuzzMaskString`（任意字符串）和 `FuzzMaskEmbedded`（把算法生成的合法身份证 / 银行卡嵌进任意上下文），各跑 20s，约 296 万次和 309 万次执行，未发现失败（[fuzz.txt](bench/results/20261018-002220-pii/fuzz.txt)）。性质：不 panic；输出中不存在任何边界合法、且能通过身份证或银行卡校验的 token。判定用的 oracle 在 `fuzz_test.go` 里独立实现（正则切分 + `time.Date` 校验日期 + 单独的 Luhn/BIN），不复用被测扫描器。种子语料在 `internal/mask/testdata/fuzz/`。

## 运行

需要 Go 1.27 及以上。

```bash
go build -o bin/loggate ./cmd/loggate
export LOGGATE_IDEM_KEY='至少16字节的随机串'   # 不设置时，每个进程随机生成一个
export LOGGATE_HMAC_KEY='至少16字节的随机串'   # 脱敏 hash 策略的密钥；不设置时每个进程随机
./bin/loggate -config configs/example.yaml

curl -s localhost:8080/healthz
# JSON：event.type=auth → OCSF 3002，其余 → Base Event 0（合成号码）
curl -s -XPOST localhost:8080/v1/events -d '[{"event":{"type":"auth","outcome":"failure"},"user":"alice","src_ip":"192.0.2.1"},{"msg":"call 13812345678"}]'
# {"accepted":2,"first_seq":1}
# 纯文本：每行一个事件，sshd 认证行 → 3002
curl -s -XPOST -H 'Content-Type: text/plain' localhost:8080/v1/events --data-binary $'sshd[1]: Failed password for invalid user admin from 192.0.2.7 port 4242 ssh2\n'
curl -s localhost:8080/v1/stats            # 含 masking 命中计数、每目的地 route_level_blocked
tail -n 3 out/archive.jsonl                # 只有脱敏后的内容：138****5678
```

PII 评测和脱敏基准：

```bash
go run ./cmd/piigen -seed 20261017 -n 10000 -out pii.jsonl
go run ./cmd/piieval -in pii.jsonl              # 写到 bench/results/<时间戳>-pii/
go test ./internal/mask -run '^$' -bench . -count 3
go test ./internal/mask -run '^$' -fuzz FuzzMaskString -fuzztime 20s
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

- **脱敏 / 归一化**：MOD 11-2 已知向量（含 X、小写 x）、闰日 0229（2000/2004 合法，1900/2023 非法）、省码、Luhn 向量、BIN 白名单边界（2220/2221、2720/2721）、数字边界（紧挨字母或数字不匹配）、`%XX` 边界、分组卡号、区间冲突、字段策略（keep 子树中的 scan、`*`、数组、default drop）、fail-closed（注入 panic）、分级映射（跨字段组合、显式核心）、max_level 路由、0 allocs；sshd / JSON golden 文件 + OCSF 子集 schema 校验；WAL 原始字节中无 PII、400 响应不回显请求内容；端到端（file sink + fake sink）验证 data_dir 和 sink 输出里都没有原文、max_level 拦截计数正确。

本机（Windows）没有 gcc，跑不了 `-race`；并发正确性依赖的不变量列在实现说明 §2。

## 局限（脱敏与分级）

- 只识别格式可校验的 PII：**姓名、地址等识别不了**；不支持 15 位老身份证；手机号没有校验位，只靠号段表，误报风险高于身份证和银行卡。
- 严格边界：紧贴 ASCII 字母的号码（`tel13812345678`）、全角数字、URL 编码的 `%40` 邮箱都不识别（评测中单独列出）。银行卡 BIN + Luhn 对随机 16–19 位数字仍有约 2% 的误报，像身份证的 18 位数字也会被当成卡号。
- 合成数据 ≠ 真实分布：P/R/F1 只说明实现在给定格式假设下的表现；生成器和检测器的格式假设相同。
- DataClass / DataLevel 是可配置的技术标签，**不是法律意义上的合规判定**（GB/T 43697 的分级依赖数据规模和用途，单条事件判断不了）。
- hash 结果不可逆；HMAC 密钥更换后无法与历史值关联；未设置密钥时每个进程随机生成，重启后同一值的 hash 不同。
- 泄露面（§0.5 item 11）：WAL / L1 / DLQ 只存脱敏后的事件，400 错误和日志不回显内容；panic 栈、core dump、swap 中仍可能出现内存里的原文，需要在部署层面关闭 core dump、加密 swap。`masking.disabled: true` 只用于基准，此时 WAL 存原文。

## 目录

```
cmd/loggate/           主程序
cmd/mocksink/          基准：mock webhook 接收方
cmd/loadgen/           基准：开环负载
cmd/reconcile/         基准：独立对账
cmd/faultbench/        基准：场景编排
cmd/piigen/            合成 PII 标注集生成器
cmd/piieval/           PII 检测评测（LogGate vs 正则基线）
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
internal/route         路由（首个匹配，OCSF 顶层字段 / unmapped 键等值，max_level）
internal/normalize     OCSF 1.3.0 子集归一化（3002、Base Event 0）
internal/mask          脱敏引擎：策略 trie、检测器、掩码、分级
internal/piibench      合成标注集生成与评测
internal/sink          Sink / Envelope 接口
internal/sink/file     file sink
internal/sink/sqlite   sqlite sink（幂等）
internal/sink/webhook  webhook sink
internal/gateway       组装各组件（含 admin API、磁盘配额），main 和端到端测试共用
internal/bench/        基准实现：scenario、loadgen、mocksink、journal、reconcile、stats、faultbench
```

## License

MIT
