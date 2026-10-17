# LogGate fault-injection results

- Profile: **quick**
- Environment: windows/amd64, Windows 10.0.26200.9711, CPU AMD Ryzen 9 5900HX with Radeon Graphics (16 logical), go1.27.0
- Commit: `1adcd085540f`; started 2026-10-17T23:46:01+09:00
- Windows run: smoke test only. Final numbers come from Linux (DESIGN §0.5 item 12).
- kill = TerminateProcess (os.Process.Kill); graceful stop = POST /v1/admin/shutdown.

Values are mean ± 95% CI half-width (Student t) across repetitions; a single repetition shows the raw value. lost = acked ∩ routed but neither delivered nor in the DLQ. Latency = first arrival at the sink − scheduled send time (ms). recovery_s = time after the fault window ends until every event acked before that end has arrived. file/sqlite arrival times come from an independent poller (50 ms resolution).

## Overview

| scenario | title | reps | verdict | lost (all dests) | notes |
|---|---|---:|---|---:|---|
| S0 | baseline (no fault) | 1 | PASS | 0 |  |
| S1 | sink down | 1 | PASS | 0 |  |
| S2 | slow sink (500ms, timeout 2s) | 1 | PASS | 0 |  |
| S2b | slow sink beyond timeout (3s, timeout 2s) | 1 | PASS | 0 |  |
| S3 | hard kill under load (3 cycles) | 1 | PASS | 0 |  |
| S3b | graceful restart | 1 | PASS | 0 |  |
| S4b | unregister destination, orphan TTL -> DLQ | 1 | PASS | 0 |  |
| S5 | disk quota exhausted while a sink is down (Linux only) | 0 | SKIPPED | - | not supported on windows (platforms: [linux]) |
| S6 | non-retryable errors (10% HTTP 400) | 1 | PASS | 0 |  |

## S0: baseline (no fault)

Same seed, rate and topology as the fault scenarios with no fault injected. Its p99 in the reference window is the baseline for the "healthy sink p99 increase" criteria (REVIEW_01 item 12).

Load 200 events/s for 1m0s. Timeline: none fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.3 | 80.3 | 80.3 | - |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.01 | 32.6 | 32.5 | 32.5 | - |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.4 | 80.3 | 80.3 | - |
| soar | 6113 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.5 | 32.4 | 32.5 | - |

- rep 1 (`S0-r1-07eb3259`): pass; gateway starts 1; drained true after 0.5s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

## S1: sink down

soar's receiver closes its listener (connection refused) at 30s and comes back at 150s. Events for soar are parked in L1 and redelivered; archive, siem and probe must be unaffected.

Load 200 events/s for 1m0s. Timeline: 6s fault soar=down; 30s fault soar=ok; fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.3 | 80.2 | 80.3 | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.7 | 32.4 | 32.4 | -0% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.3 | 80.2 | 80.3 | -0% |
| soar | 6113 | 0 | 0 | 0 | 0 | 0 | 6.56 | 32.6 | 30085.5 | 6308.6 | +92743% |

- rep 1 (`S1-r1-20a46a87`): pass; gateway starts 1; drained true after 0.3s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

## S2: slow sink (500ms, timeout 2s)

soar answers every request after 500 ms (below the 2 s client timeout) from 30s to 150s. Only soar's own queue should back up (L1 rises, then returns to 0); healthy sinks keep their latency.

Load 200 events/s for 1m0s. Timeline: 6s fault soar=slow(500ms); 30s fault soar=ok; fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.4 | 80.3 | 80.3 | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.01 | 32.5 | 32.5 | 32.5 | +0% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.1 | 80.3 | 80.3 | -0% |
| soar | 6113 | 0 | 0 | 0 | 0 | 0 | 0.01 | 32.5 | 1024.9 | 32.5 | +3063% |

- rep 1 (`S2-r1-124552b1`): pass; gateway starts 1; drained true after 0.3s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.2 ms

## S2b: slow sink beyond timeout (3s, timeout 2s)

soar answers after 3 s while the gateway gives up after 2 s. The receiver still processes every request it got, so soar sees duplicates; that is expected and reported (dup_raw), not a failure. A receiver honouring Idempotency-Key would store none of them (dup_stored).

Load 200 events/s for 1m0s. Timeline: 6s fault soar=slow(3000ms); 30s fault soar=ok; fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.4 | 80.3 | 80.4 | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.01 | 33.8 | 32.5 | 32.5 | +0% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.4 | 80.2 | 80.3 | -0% |
| soar | 6113 | 0 | 908 | 0 | 0 | 0 | 8.49 | 33.8 | 25790.6 | 7937.3 | +79489% |

- rep 1 (`S2b-r1-2ef53cdc`): pass; gateway starts 1; drained true after 0.5s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.2 ms

## S3: hard kill under load (3 cycles)

The gateway process is hard-killed (SIGKILL on Linux, TerminateProcess on Windows) at 60s, 120s and 180s and restarted 10 s later each time, on the same data dir. Requests during the outage fail at the client and are not in the promised set. Replay after each restart may duplicate up to about one checkpoint interval of events on non-idempotent sinks (archive raw, webhook raw); siem must store none. kill -9 is not power loss: the page cache survives.

Load 200 events/s for 1m0s. Timeline: 12s kill; 14s start; 24s kill; 26s start; 36s kill; 38s start; fault window 12s..38s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 10739 | 0 | 461 | 461 | 0 | 3 | 0.15 | 80.3 | 80.3 | 80.4 | -0% |
| probe | 8672 | 0 | 361 | 0 | 0 | 2 | 0.12 | 32.6 | 32.5 | 32.5 | +0% |
| siem | 8672 | 0 | 0 | 0 | 0 | 2 | 0.15 | 80.3 | 80.4 | 80.3 | +0% |
| soar | 5470 | 0 | 240 | 0 | 0 | 2 | 0.12 | 32.4 | 32.5 | 32.5 | +0% |

- rep 1 (`S3-r1-f3705fa2`): pass; gateway starts 4; drained true after 0.5s; acked 10739/12000 map[200:10739 error:1261], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

## S3b: graceful restart

Graceful stop at 60s (SIGTERM on Linux; POST /v1/admin/shutdown on Windows, which has no SIGTERM), restart at 70s. A graceful stop finishes in-flight batches, parks the rest and writes a final checkpoint, so nothing is replayed: dup must be 0 on every sink (assuming no request times out ambiguously during the stop).

Load 200 events/s for 1m0s. Timeline: 12s stop; 14s start; fault window 12s..14s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 11583 | 0 | 0 | 0 | 0 | 0 | 0.35 | 80.4 | 2350.4 | 80.3 | +2826% |
| probe | 9356 | 0 | 0 | 0 | 0 | 0 | 0.31 | 32.5 | 2310.6 | 32.4 | +7007% |
| siem | 9356 | 0 | 0 | 0 | 0 | 0 | 0.35 | 80.4 | 2350.4 | 80.3 | +2826% |
| soar | 5903 | 0 | 0 | 0 | 0 | 0 | 0.31 | 32.3 | 2309.6 | 32.4 | +7027% |

- rep 1 (`S3b-r1-75004672`): pass; gateway starts 2; drained true after 0.5s; acked 11583/12000 map[200:11583 error:417], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

## S4b: unregister destination, orphan TTL -> DLQ

soar is unregistered through the admin API at 60s and never comes back. Its in-flight batch completes, the rest of its queue and every later event for it are parked (NotActive) and, after orphan_ttl = 30 s, moved to the DLQ by the sweeper with reason orphan_ttl_expired. Nothing may be lost; the other sinks are unaffected. (The TTL is not scaled by the profile.)

Load 200 events/s for 1m0s. Timeline: 12s unregister soar; fault window 12s..1m0s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.3 | 80.3 | - | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.3 | 32.3 | - | -1% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.3 | 80.2 | - | -0% |
| soar | 6113 | 0 | 0 | 0 | 4902 | 0 | 0.00 | 32.3 | - | - | - |

- rep 1 (`S4b-r1-05e642f3`): pass; gateway starts 1; drained true after 31.3s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms; soar dlq map[orphan_ttl_expired:4902]

## S6: non-retryable errors (10% HTTP 400)

From 30s to 150s soar answers 10% of requests with HTTP 400 (seeded, deterministic per request index). A 400 is non-retryable: the whole batch of that request goes to the DLQ (fsync first). The DLQ set must equal the set of events the mock rejected with 4xx; everything else is delivered.

Load 200 events/s for 1m0s. Timeline: 6s fault soar=ok(400@0.1); 30s fault soar=ok; fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.3 | 80.3 | 80.3 | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.5 | 32.4 | 32.4 | -0% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.4 | 80.4 | 80.2 | +0% |
| soar | 6113 | 0 | 0 | 0 | 241 | 0 | 0.00 | 32.4 | 32.3 | 32.3 | -0% |

- rep 1 (`S6-r1-6919ace3`): pass; gateway starts 1; drained true after 0.5s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms; soar dlq map[non_retryable:241]; soar dlq==rejected4xx true

