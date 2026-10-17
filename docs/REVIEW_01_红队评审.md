# DESIGN v0.1 红队评审（2026-10-17）

结论：方向成立。下面列出会丢数据或卡死的点、说法过头的点，按优先级排列，每条后面是对应的修订决定（见 DESIGN.md §0.5）。

## P0（会丢数据、卡死，或让结论失效）

1. **截断撕裂尾部后 Seq 被重新分配，可能丢数据**
   - 问题：Dispatch 必须只处理已 fsync 的记录。否则截断撕裂尾部后，同一个 Seq 会分给另一条事件；如果这时 `ckpt` 已经越过这个 Seq，这条事件就会被跳过。
   - 决定：只 dispatch 已 fsync 的记录。恢复后，新 Seq 必须大于所有 ckpt 和 L1 中出现过的最大 Seq。
2. **空 tracker 的水位定义不清**
   - 问题：没有流量的目的地，ckpt 永远是 0，GC 就永远不会执行。如果改为"空 tracker 的水位等于 dispatch 游标"，又会在 Add 之前越过还没加入的 Seq。
   - 决定：水位 = min(队头 − 1, 已全部 Add 完成的游标)。
3. **Replace / Draining 时在途批次的归属不清**
   - 决定：旧消费者必须把当前批次跑完，并走正常的结果处理流程。v1 不做 Replace。
4. **非 Active 目的地在 L1 里的记录没人管**
   - 问题：已注销、拼错名字等非 Active 目的地，事件 Park 进 L1 后，没有任何机制评估它们的 TTL 和 max_age。
   - 决定：增加一个全局 L1 sweeper。
5. **转 Dead 与写 DLQ 不原子**
   - 问题：先提交转 Dead、DLQ 还没 fsync 时崩溃，事件就丢了。
   - 决定：先 fsync DLQ，再提交 bbolt 或 Resolve。
6. **IdemKey 设计问题**
   - inode+offset 在日志轮转和 inode 复用时会误合并；内容哈希会把重复的暴力破解日志合并掉，压低计数；对脱敏前原文做无盐哈希，泄露的风险还在。
   - 决定：默认由网关分配 `HMAC(node|epoch|seq)`。来源序号只用于吸收客户端重发，并绑定认证后的 SourceID。
7. **S5 测试必失败，fsync 出错后的处理未定义**
   - 问题：64MB tmpfs 配 64MB 分段，分段永远滚动不了；fsync 出错（fsyncgate）后的处理也没写。
   - 决定：S5 场景的分段改为 4MB，磁盘配额把 bbolt 和 DLQ 一起计算。fsync 出错时，把当前段标为毒段，截断后滚动，本批全部不 ack。

## P1

8. **"原地 80B"的说法不成立**
   - 问题：bbolt 是写时复制，改一条记录会重写整页，"原地 80B"不成立。另外，`Batch` 的 fn 可能被重复执行，而追加 Slot 不幂等。
   - 决定：单个 writer 用 `db.Update` 自己攒批；文档如实写写放大。
9. **一个目的地长时间宕机会拖垮全局**
   - 问题：L1 满后整个网关返回 429；QueueFull 的 Park 也会消耗重试次数；新流量多时，redeliver 会被饿死。
   - 决定：每个目的地单独设 L1 配额，满了可选 block-all / DLQ-overflow；只有真实发送失败才计入 attempts；redeliver 至少保留 20% 份额。
10. **X-Source-Seq 可伪造，导致日志被压制**
    - 决定：SourceID 绑定认证 token，不接受客户端自报。
11. **脱敏覆盖面和误报**
    - raw_data 和 unmapped 也要扫描。
    - 列出以下泄露面：错误信息、panic、core dump、swap，以及 demo 里的"脱敏前"展示。
    - 默认策略是 scan，并非 fail-closed，措辞要改准。
    - 银行卡的 BIN 改为必需（19 位雪花 ID 约有 1/10 能通过 Luhn）。
    - test_seq 设为 keep。
    - 15 位老身份证不在本次范围内，写进 Limitations。
12. **基准方法**
    - 健康 sink 的延迟要由独立 mocksink 测量，不能由网关自报。
    - p99 的基线是同 seed 的无故障运行。
    - 延迟从计划发送时间算起（避免 coordinated omission）。
    - recovery_s 定义为：故障结束前已 ack 的事件全部送达的时刻。
    - routed_to 由独立 oracle 计算。
    - soak 的验收看 WAL 段数，不看磁盘总大小（bbolt 文件不会收缩）。
    - kill -9 不等于掉电；数据放在 Linux 文件系统上。
13. **对 Vector 的说法需要实测**
    - 问题：Vector 的 disk buffer 是每个 sink 各一份，健康 sink 未必会收到重复。
    - 决定：调研里关于 Vector、Fluent Bit 的结论不预设，以实测为准。对照实验中客户端重试行为保持一致。

## P2
- DLQ 计入磁盘配额；drop_oldest 选项删掉。
- Windows 上删除打开的段文件需要引用计数，并延迟删除。
- file sink 被 kill 后留下的半行，由 reconcile 容忍。
- "优雅重启时 dup=0"的前提，是没有超时含糊不清的请求。
- half-open 时新事件也放行。

## 削减范围（v1 必保主线）
- **必保**：WAL、tracker/ckpt、L1 + sweeper、恢复、GC；file / sqlite / mock webhook 三种 sink；reconcile；场景 S1、S2、S3、S3b、S5、S6；身份证 / 银行卡 / 手机号 + piieval。
- **砍掉或降级**：
  - 砍掉：Replace（S4 推后，保留 S4b）；Postgres / Kafka / UDP / file tail。
  - 降级：
    - gRPC 改为 HTTP JSON + /healthz；
    - 性能矩阵缩小为 1KB × 脱敏开/关，重复 3 次；
    - OCSF 只做 3002 和 0；
    - 分级只保留 max_level；
    - dashboard 改为报告页 + 对账面板；
    - 与 Vector 对照最多只跑 S1。

## 面试官最可能追问
1. 为什么 Park 要在 bbolt 提交之后再 Resolve？ckpt 和 L1 为什么不放在同一个事务里？
2. 一个目的地宕机 24 小时，会发生什么？和 Fluent Bit、Vector 相比如何取舍？
3. kill -9 之后的重复从哪里来？上界是多少？实测值是多少？
4. 幂等键能不能被攻击者利用来压制告警？
5. 雪花 ID 很多的日志里，误报率是多少？
