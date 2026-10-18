# LogGate 3 分钟演示脚本

演示全部在一个进程里完成，只用合成数据，不访问外部网络。

## 准备（演示前）

```bash
go build -o bin/loggate ./cmd/loggate
./bin/loggate --demo            # Windows：.\bin\loggate.exe --demo
```

启动后终端会打印四个地址：

| 用途 | 地址 |
|---|---|
| 控制台 dashboard | http://127.0.0.1:8081/ui/ |
| Prometheus 指标 | http://127.0.0.1:9091/metrics |
| 网关接收端口 | http://127.0.0.1:8080/v1/events |
| 模拟 SOAR 接收端 | http://127.0.0.1:9100/ingest |

进程内组件：
- 网关：3 个目的地。archive 写文件（非幂等）；siem 写 sqlite（按 idem_key 幂等）；soar 是 webhook，指向进程内的模拟 SOAR 接收端，`max_level: general`。
- 负载生成器：约 50 eps，每个请求一条事件，只用合成数据。sshd 认证日志和 JSON 认证事件路由到三个目的地；JSON 应用事件只路由到 archive 和 siem。PII 由 piibench 按算法生成。
- 对账器：每秒对比两边的数据。一边是负载端收到 200 的事件（已 fsync 进 WAL）；另一边是 soar 接收端实际收到的、archive 文件里的、sqlite 表里的，以及 DLQ。

常用参数：`-demo-eps 200` 调整速率；`-demo-dir D:\tmp\lg` 指定目录（必须是新目录）；`-demo-keep` 退出后保留目录。默认目录在系统临时目录下，退出时删除。Ctrl+C 停止。

演示按钮只接受本机请求：来源和 Host 都必须是回环地址，并且带 `X-LogGate-Demo: 1` 头。

## 0:00–0:30 正常流量

打开控制台，介绍几个区域：
- 顶部计数器：已接收、已确认、已投递（分目的地）、L1 暂存、死信、反压。
- 目的地表：三行都显示"运行 · active"和"闭合 · closed"，p99 端到端延迟在几十毫秒。
- 实时对账：三个目的地的"丢失"都是绿色的 **✓ 0**。

> 讲解词：网关先把事件写入 WAL 并 fsync，然后才给客户端 ack。每个目的地有自己独立的队列和熔断器。

## 0:30–1:15 目的地宕机：只影响它自己

在 soar 行点 **停止接收 Kill sink**。模拟接收端会关闭监听端口，网关看到的是连接被拒绝。

可以看到：
- soar 的熔断器变为"断开 · open"，最近错误显示"可重试 retryable"，L1 暂存持续上涨，趋势图里 soar 的曲线在爬升。
- archive 和 siem 照常投递，p99 不变。
- 对账面板中 soar 显示"收敛中 settling"，未达数量在增加；另外两行仍是 ✓ 0。

> 讲解词：失败的事件按目的地暂存到 L1（bbolt 位图）。健康的目的地不会收到重复，也不会被拖慢。

## 1:15–1:45 恢复：积压回落到 0

点 **恢复 Restore**。

约几秒后：熔断器转为半开再到闭合，soar 的 L1 回落到 0。操作记录里会出现一行"L1 积压已清空 … peak N events, X s"，这就是恢复时间。对账中 soar 的未达变成 0，丢失为 **✓ 0**，重复 0。

可选：点 **变慢 Slow 500ms**，展示 soar 的 p99 升到约 1 秒，而 archive 和 siem 的 p99 保持不变；然后再点恢复。

## 1:45–2:30 模拟崩溃

点右上角的红色按钮 **模拟崩溃 Simulate crash**。

这一步会调用 `Gateway.Crash()`，丢弃所有内存状态：未提交的 L1 操作、L2 队列、在途批次的结果，并且不写最终 checkpoint。随后在同一个数据目录上新建网关，走 WAL 恢复和重放。

可以看到：
- 状态标签显示"boot #2"。操作记录里有"模拟崩溃并重启 … replayed N WAL records"。
- 对账中 archive（文件）的原始重复约等于重放数，属于预期，因为 file sink 不幂等；siem 的存储重复为 0；soar 有原始重复，但存储重复为 0，因为重发沿用同一个 idem_key。
- 三个目的地的丢失都是 **✓ 0**。负载端可能有 1 条请求在崩溃瞬间失败，它没有被确认，所以不计入期望集合。

> 必须说明：这是进程内模拟。WAL 文件句柄正常关闭，等价于"最后一次 fsync 之后崩溃"。真实的 kill -9 由 faultbench 的 S3 场景覆盖，见 README 的基准结果。

## 2:30–3:00 脱敏与指标

- 滚动到"脱敏样例"，这里显示最近 20 条事件**脱敏后**的内容：手机号显示为 `138****5678` 的形式，身份证、银行卡保留首尾，用户名是 HMAC 哈希。每条带 OCSF 类别、分级标签（一般 / 重要）、DataClass 和命中数。原文从不落盘，也不在面板展示。
- 同时含身份证和银行卡的认证事件被分级为"重要"，按 `max_level: general` 不投给 soar。目的地表里 soar 一行会显示"max_level 拦截 blocked N"。
- 可选：打开 `http://127.0.0.1:9091/metrics`，展示 `loggate_l1_pending{dest="soar"}`、`loggate_delivered_total{dest,origin}`（origin 分为 live / replay / redeliver）、`loggate_mask_hits_total{type}`。

## 备选操作

- **注销 / 注册**：注销 soar 后，它的事件会 Park 到 L1，原因为 NotActive 或 NoDest，不消耗重试次数。重新注册后，这些事件由 redeliver 投递。超过 `orphan_ttl`（演示中为 10 分钟）仍未注册，事件会进入 DLQ。
- 演示参数刻意调短，让现象在几秒内出现：退避上限 2s，熔断 3 次失败后断开 3s，重试上限设得很大，演示中不会进入 DLQ。生产默认值见 `configs/example.yaml`。
