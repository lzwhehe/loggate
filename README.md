# LogGate

LogGate 是一个用 Go 编写的单机安全日志采集网关。它的目标是：已经 ack 的日志不丢；某个目的地失败时，只重投这个目的地；借助幂等键，在幂等 sink 上做到 effectively-once。丢失和重复用一套可复现的故障注入基准来量化。

设计见 [docs/DESIGN.md](docs/DESIGN.md)（§0.5 的 v0.2 修订优先于正文），红队评审见 [docs/REVIEW_01_红队评审.md](docs/REVIEW_01_红队评审.md)。

## 当前状态：M1（WAL + HTTP ingest + file sink）

已实现：
- **WAL**：分段追加（`%020d.seg`）、CRC32C 校验、单 writer 分配 Seq、组提交 fsync（按间隔或条数触发）。
  - fsync 成功之后才回调 ack。
  - 打开时截断撕裂的尾部，新 Seq 一定大于所有已存在的 Seq。
  - write 或 fsync 出错时：把当前段标为毒段，截断到上一个确认点，下一批写到新段，本批一律不 ack。
- **HTTP ingest**：`POST /v1/events` 接受单个 JSON 对象或对象数组，fsync 完成后才返回 200；WAL 出错返回 503；超过 `max_body` 返回 413。
  - 配置了 Bearer token 时，SourceID 由 token 决定，客户端不能自报。
  - 另有 `GET /healthz`。
- **幂等键**：由网关分配，`HMAC-SHA256(node | boot_epoch | seq)` 取前 16 字节，写入 WAL。
- **file sink**：每个事件写一行 JSON，每批 flush 一次（不 fsync）。这个 sink 不是幂等的。
- **投递**：单目的地直投。这是 M1 的占位实现，没有 L1/L2、checkpoint 和重启后重放，M2 会替换掉。

M2 及以后的计划见 DESIGN §13。M1 的已知缺口：
- 重启后，已提交但还没投递的事件不会重放。它们留在 WAL 里，要等 M2 的 checkpoint 和恢复来处理。
- 一个慢 sink 会通过反压拖慢整个 ingest。
- 还没有做脱敏和 OCSF 归一化。目前原始 JSON 放在 `fields.unmapped` 里，原文放在 `raw` 里，WAL 存的是未脱敏数据。
- WAL 段不会 GC。

目前还没有任何性能数据，后面的基准实测之后才会补上。

## 运行

需要 Go 1.27 及以上。

```bash
go build -o bin/loggate ./cmd/loggate
export LOGGATE_IDEM_KEY='至少16字节的随机串'   # 不设置时，每个进程随机生成一个
./bin/loggate -config configs/example.yaml

curl -s localhost:8080/healthz
curl -s -XPOST localhost:8080/v1/events -d '[{"msg":"sshd: Failed password for root"},{"msg":"ok"}]'
# {"accepted":2,"first_seq":1}
tail -n 2 out/archive.jsonl
```

收到 SIGINT 或 SIGTERM 时会优雅关闭：先停止接收 HTTP 请求，再把 WAL 刷盘，然后投递完已经提交的事件，最后关闭 sink。超时时间用 `-shutdown-timeout` 设置，默认 30s。

## 测试

```bash
go vet ./...
go test ./... -count=1
```

覆盖内容：
- **WAL**：往返读写、检测 CRC 损坏、截断撕裂尾部后重开（已 ack 的事件全部保留，新 Seq 大于所有旧 Seq）、段滚动。
- **故障注入**（FaultFS）：fsync 出错时触发毒段并滚到新段，ENOSPC 时不 ack。
- **ack 顺序**：用 FaultFS 记录的操作序列断言 ack 一定发生在 fsync 成功之后。
- **组提交**：用 FakeClock 驱动，验证组提交的时机。
- **模拟崩溃**：进程不正常关闭后重开 WAL，已 ack 的事件全部可读。
- **HTTP**：只有提交成功后才返回 200，WAL 失败返回 503，`max_body` 生效，token 鉴权。
- **端到端**：在进程内启动网关，发送 1000 个事件，检查 file sink 里每个 seq 恰好出现一次；然后在同一个数据目录上重启，确认 seq 不会复用。

## 目录

```
cmd/loggate/           主程序
configs/example.yaml   示例配置（只接受 M1 已实现的键，未知键会报错）
internal/event         LogEvent、DestMask、IdemKey
internal/config        YAML 配置、默认值和校验（支持 64MiB、10ms 这类写法）
internal/clock         Clock 接口、真实时钟和 FakeClock
internal/wal           WAL、FS 抽象、FaultFS
internal/ingest/http   HTTP 输入
internal/route         路由（M1 只支持 catch-all）
internal/sink          Sink / Envelope 接口
internal/sink/file     file sink
internal/dispatch      M1 直投循环（TODO(M2)）
internal/gateway       组装各组件，main 和端到端测试共用
```

## License

MIT
