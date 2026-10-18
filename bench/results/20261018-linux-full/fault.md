# LogGate fault-injection results

- Profile: **full**
- Environment: linux/amd64, Linux 5.15.0-92-generic, CPU Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz (32 logical), go1.27.0
- Commit: `29bff0e85ee5`; started 2026-10-18T14:03:53+09:00
- kill = SIGKILL; graceful stop = SIGTERM. kill -9 is not power loss (page cache survives).

Values are mean ± 95% CI half-width (Student t) across repetitions; a single repetition shows the raw value. lost = acked ∩ routed but neither delivered nor in the DLQ. Latency = first arrival at the sink − scheduled send time (ms). recovery_s = time after the fault window ends until every event acked before that end has arrived. file/sqlite arrival times come from an independent poller (50 ms resolution).

## Overview

| scenario | title | reps | verdict | lost (all dests) | notes |
|---|---|---:|---|---:|---|
| S0 | baseline (no fault) | 5 | PASS | 0 |  |
| S1 | sink down | 5 | PASS | 0 |  |
| S2b | slow sink beyond timeout (3s, timeout 2s) | 5 | PASS | 0 |  |
| S2 | slow sink (500ms, timeout 2s) | 5 | PASS | 0 |  |
| S3b | graceful restart | 5 | PASS | 0 |  |
| S3 | hard kill under load (3 cycles) | 5 | PASS | 0 |  |
| S4b | unregister destination, orphan TTL -> DLQ | 5 | PASS | 0 |  |
| S6 | non-retryable errors (10% HTTP 400) | 5 | PASS | 0 |  |

## S0: baseline (no fault)

Same seed, rate and topology as the fault scenarios with no fault injected. Its p99 in the reference window is the baseline for the "healthy sink p99 increase" criteria (REVIEW_01 item 12).

Load 2000 events/s for 5m0s. Timeline: none fault window 30s..2m30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 84.4 ± 0.8 | 84.5 ± 0.3 | 84.4 ± 0.2 | - |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.01 | 45.4 ± 0.7 | 45.2 ± 0.3 | 45.2 ± 0.4 | - |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.04 ± 0.03 | 84.6 ± 0.3 | 85.6 ± 0.4 | 85.5 ± 0.4 | - |
| soar | 299502 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.01 | 43.3 ± 1.2 | 43.1 ± 0.4 | 43.1 ± 0.3 | - |

- rep 1 (`S0-r1-3d6f1aca`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.8 ms
- rep 2 (`S0-r2-47d6e23d`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 41, inflight-cap hits 0, ack p99 17.7 ms
- rep 3 (`S0-r3-9d13ad58`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 4, inflight-cap hits 0, ack p99 17.7 ms
- rep 4 (`S0-r4-e449b6d3`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.5 ms
- rep 5 (`S0-r5-f34fc3df`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.6 ms

## S1: sink down

soar's receiver closes its listener (connection refused) at 30s and comes back at 150s. Events for soar are parked in L1 and redelivered; archive, siem and probe must be unaffected.

Load 2000 events/s for 5m0s. Timeline: 30s fault soar=down; 2m30s fault soar=ok; fault window 30s..2m30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 84.9 ± 0.8 | 83.0 ± 0.3 | 82.5 ± 0.3 | -2% |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.02 ± 0.01 | 45.8 ± 0.4 | 44.1 ± 0.2 | 43.1 ± 0.3 | -2% |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 85.1 ± 0.4 | 84.3 ± 0.2 | 84.0 ± 0.3 | -2% |
| soar | 299502 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 10.04 ± 0.23 | 44.0 ± 0.3 | 122297.0 ± 911.2 | 43.1 ± 1.5 | +283965% |

- rep 1 (`S1-r1-2101ac0c`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.1 ms
- rep 2 (`S1-r2-e172f87c`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.0 ms
- rep 3 (`S1-r3-c62fa1bf`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.0 ms
- rep 4 (`S1-r4-0dfc97df`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.2 ms
- rep 5 (`S1-r5-6664b026`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 2, inflight-cap hits 0, ack p99 17.2 ms

## S2b: slow sink beyond timeout (3s, timeout 2s)

soar answers after 3 s while the gateway gives up after 2 s. The receiver still processes every request it got, so soar sees duplicates; that is expected and reported (dup_raw), not a failure. A receiver honouring Idempotency-Key would store none of them (dup_stored).

Load 2000 events/s for 5m0s. Timeline: 30s fault soar=slow(3000ms); 2m30s fault soar=ok; fault window 30s..2m30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.04 ± 0.03 | 84.1 ± 0.6 | 82.3 ± 0.2 | 82.3 ± 0.1 | -3% |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.01 | 45.1 ± 0.6 | 43.0 ± 0.3 | 42.8 ± 0.2 | -5% |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.03 ± 0.03 | 84.5 ± 0.8 | 83.1 ± 0.4 | 83.4 ± 0.3 | -3% |
| soar | 299502 ± 0 | 0 ± 0 | 1270 ± 21 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 18.17 ± 0.27 | 42.9 ± 0.8 | 130354.8 ± 147.5 | 15479.7 ± 246.5 | +302681% |

- rep 1 (`S2b-r1-71f7ff81`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.7 ms
- rep 2 (`S2b-r2-c0bf7139`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 43, inflight-cap hits 0, ack p99 16.6 ms
- rep 3 (`S2b-r3-812279ca`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.9 ms
- rep 4 (`S2b-r4-02351372`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 16, inflight-cap hits 0, ack p99 16.8 ms
- rep 5 (`S2b-r5-3ee0fc46`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.6 ms

## S2: slow sink (500ms, timeout 2s)

soar answers every request after 500 ms (below the 2 s client timeout) from 30s to 150s. Only soar's own queue should back up (L1 rises, then returns to 0); healthy sinks keep their latency.

Load 2000 events/s for 5m0s. Timeline: 30s fault soar=slow(500ms); 2m30s fault soar=ok; fault window 30s..2m30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.03 ± 0.03 | 84.5 ± 1.1 | 80.7 ± 0.1 | 82.7 ± 0.2 | -4% |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.01 | 45.4 ± 0.3 | 38.9 ± 0.1 | 43.1 ± 0.2 | -14% |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 84.9 ± 0.8 | 80.0 ± 0.6 | 83.9 ± 0.4 | -7% |
| soar | 299502 ± 0 | 0 ± 0 | 1 ± 3 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 4.11 ± 0.13 | 43.5 ± 0.3 | 70552.2 ± 432.2 | 43.2 ± 1.0 | +163775% |

- rep 1 (`S2-r1-89474512`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.3 ms
- rep 2 (`S2-r2-63fa1c0a`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.3 ms
- rep 3 (`S2-r3-489f5464`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.3 ms
- rep 4 (`S2-r4-55cc546c`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 24, inflight-cap hits 0, ack p99 16.5 ms
- rep 5 (`S2-r5-0c69a090`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 16.5 ms

## S3b: graceful restart

Graceful stop at 60s (SIGTERM on Linux; POST /v1/admin/shutdown on Windows, which has no SIGTERM), restart at 70s. A graceful stop finishes in-flight batches, parks the rest and writes a final checkpoint, so nothing is replayed: dup must be 0 on every sink (assuming no request times out ambiguously during the stop).

Load 2000 events/s for 5m0s. Timeline: 1m0s stop; 1m10s start; fault window 1m0s..1m10s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 579513 ± 49 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.49 ± 0.03 | 84.6 ± 0.5 | 10491.1 ± 27.8 | 84.4 ± 0.3 | +12322% |
| probe | 463549 ± 40 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.48 ± 0.04 | 45.7 ± 0.7 | 10474.5 ± 35.2 | 45.1 ± 0.3 | +23094% |
| siem | 463549 ± 40 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.49 ± 0.03 | 85.5 ± 0.6 | 10490.6 ± 27.8 | 85.5 ± 0.3 | +12160% |
| soar | 289316 ± 28 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.37 ± 0.26 | 43.6 ± 0.6 | 10459.6 ± 178.5 | 43.1 ± 0.2 | +24195% |

- rep 1 (`S3b-r1-26708644`): pass; gateway starts 2; drained true after 0.3s; acked 579489/600000 map[200:579489 error:20511], client behind 1, inflight-cap hits 0, ack p99 17.9 ms
- rep 2 (`S3b-r2-df89515d`): pass; gateway starts 2; drained true after 0.3s; acked 579574/600000 map[200:579574 error:20426], client behind 26, inflight-cap hits 0, ack p99 17.8 ms
- rep 3 (`S3b-r3-bdb782a7`): pass; gateway starts 2; drained true after 0.3s; acked 579515/600000 map[200:579515 error:20485], client behind 0, inflight-cap hits 0, ack p99 17.7 ms
- rep 4 (`S3b-r4-7d190f42`): pass; gateway starts 2; drained true after 0.3s; acked 579468/600000 map[200:579468 error:20532], client behind 0, inflight-cap hits 0, ack p99 17.8 ms
- rep 5 (`S3b-r5-ccd207cd`): pass; gateway starts 2; drained true after 0.3s; acked 579517/600000 map[200:579517 error:20483], client behind 0, inflight-cap hits 0, ack p99 17.8 ms

## S3: hard kill under load (3 cycles)

The gateway process is hard-killed (SIGKILL on Linux, TerminateProcess on Windows) at 60s, 120s and 180s and restarted 10 s later each time, on the same data dir. Requests during the outage fail at the client and are not in the promised set. Replay after each restart may duplicate up to about one checkpoint interval of events on non-idempotent sinks (archive raw, webhook raw); siem must store none. kill -9 is not power loss: the page cache survives.

Load 2000 events/s for 5m0s. Timeline: 1m0s kill; 1m10s start; 2m0s kill; 2m10s start; 3m0s kill; 3m10s start; fault window 1m0s..3m10s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 538705 ± 96 | 0 ± 0 | 3792 ± 77 | 3792 ± 77 | 0 ± 0 | 16 ± 21 | 0.18 ± 0.06 | 84.6 ± 0.2 | 84.4 ± 0.1 | 84.5 ± 0.4 | -0% |
| probe | 430925 ± 73 | 0 ± 0 | 3080 ± 104 | 0 ± 0 | 0 ± 0 | 12 ± 16 | 0.16 ± 0.03 | 45.3 ± 0.4 | 45.3 ± 0.5 | 45.3 ± 0.4 | +0% |
| siem | 430925 ± 73 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 12 ± 16 | 0.17 ± 0.06 | 85.0 ± 0.4 | 85.7 ± 0.4 | 85.8 ± 0.3 | +0% |
| soar | 268875 ± 48 | 0 ± 0 | 1973 ± 42 | 0 ± 0 | 0 ± 0 | 7 ± 9 | 0.16 ± 0.04 | 43.3 ± 0.5 | 43.3 ± 0.5 | 43.3 ± 0.2 | +0% |

- rep 1 (`S3-r1-f0dcef20`): pass; gateway starts 4; drained true after 0.3s; acked 538801/600000 map[200:538801 error:61199], client behind 42, inflight-cap hits 0, ack p99 17.6 ms
- rep 2 (`S3-r2-7fe51283`): pass; gateway starts 4; drained true after 0.3s; acked 538620/600000 map[200:538620 error:61380], client behind 0, inflight-cap hits 0, ack p99 17.6 ms
- rep 3 (`S3-r3-c7099380`): pass; gateway starts 4; drained true after 0.3s; acked 538767/600000 map[200:538767 error:61233], client behind 0, inflight-cap hits 0, ack p99 17.8 ms
- rep 4 (`S3-r4-ea6dedd9`): pass; gateway starts 4; drained true after 0.3s; acked 538691/600000 map[200:538691 error:61309], client behind 4, inflight-cap hits 0, ack p99 18.0 ms
- rep 5 (`S3-r5-0bf7513a`): pass; gateway starts 4; drained true after 0.3s; acked 538648/600000 map[200:538648 error:61352], client behind 3, inflight-cap hits 0, ack p99 17.8 ms

## S4b: unregister destination, orphan TTL -> DLQ

soar is unregistered through the admin API at 60s and never comes back. Its in-flight batch completes, the rest of its queue and every later event for it are parked (NotActive) and, after orphan_ttl = 30 s, moved to the DLQ by the sweeper with reason orphan_ttl_expired. Nothing may be lost; the other sinks are unaffected. (The TTL is not scaled by the profile.)

Load 2000 events/s for 5m0s. Timeline: 1m0s unregister soar; fault window 1m0s..5m0s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.04 ± 0.03 | 84.4 ± 0.4 | 83.4 ± 0.1 | - | -1% |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.00 | 45.5 ± 0.7 | 44.5 ± 0.2 | - | -2% |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 85.0 ± 0.3 | 85.2 ± 0.4 | - | -0% |
| soar | 299502 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 239610 ± 14 | 0 ± 0 | 0.00 ± 0.00 | 43.4 ± 0.6 | - | - | - |

- rep 1 (`S4b-r1-02e428b1`): pass; gateway starts 1; drained true after 36.3s; acked 600000/600000 map[200:600000], client behind 37, inflight-cap hits 0, ack p99 17.7 ms; soar dlq map[orphan_ttl_expired:239615]
- rep 2 (`S4b-r2-b4b02d99`): pass; gateway starts 1; drained true after 36.5s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.8 ms; soar dlq map[orphan_ttl_expired:239595]
- rep 3 (`S4b-r3-6eea26b2`): pass; gateway starts 1; drained true after 36.6s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.9 ms; soar dlq map[orphan_ttl_expired:239602]
- rep 4 (`S4b-r4-10553958`): pass; gateway starts 1; drained true after 36.0s; acked 600000/600000 map[200:600000], client behind 4, inflight-cap hits 0, ack p99 17.7 ms; soar dlq map[orphan_ttl_expired:239620]
- rep 5 (`S4b-r5-c286a5fb`): pass; gateway starts 1; drained true after 36.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.7 ms; soar dlq map[orphan_ttl_expired:239620]

## S6: non-retryable errors (10% HTTP 400)

From 30s to 150s soar answers 10% of requests with HTTP 400 (seeded, deterministic per request index). A 400 is non-retryable: the whole batch of that request goes to the DLQ (fsync first). The DLQ set must equal the set of events the mock rejected with 4xx; everything else is delivered.

Load 2000 events/s for 5m0s. Timeline: 30s fault soar=ok(400@0.1); 2m30s fault soar=ok; fault window 30s..2m30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 600000 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.05 ± 0.00 | 84.1 ± 0.8 | 84.2 ± 0.1 | 84.3 ± 0.4 | -0% |
| probe | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.01 ± 0.01 | 45.4 ± 0.3 | 44.9 ± 0.1 | 45.0 ± 0.1 | -0% |
| siem | 479917 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0.04 ± 0.03 | 84.2 ± 0.8 | 85.2 ± 0.3 | 85.4 ± 0.3 | -0% |
| soar | 299502 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 | 12676 ± 155 | 0 ± 0 | 0.01 ± 0.01 | 43.1 ± 0.8 | 43.0 ± 0.2 | 43.0 ± 0.1 | -0% |

- rep 1 (`S6-r1-4a2c5dd8`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 1, inflight-cap hits 0, ack p99 17.4 ms; soar dlq map[non_retryable:12719]; soar dlq==rejected4xx true
- rep 2 (`S6-r2-b94fb316`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 6, inflight-cap hits 0, ack p99 17.7 ms; soar dlq map[non_retryable:12566]; soar dlq==rejected4xx true
- rep 3 (`S6-r3-4e843c5b`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.5 ms; soar dlq map[non_retryable:12827]; soar dlq==rejected4xx true
- rep 4 (`S6-r4-2cbd622c`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 0, inflight-cap hits 0, ack p99 17.6 ms; soar dlq map[non_retryable:12529]; soar dlq==rejected4xx true
- rep 5 (`S6-r5-3e8d55b5`): pass; gateway starts 1; drained true after 0.3s; acked 600000/600000 map[200:600000], client behind 11, inflight-cap hits 0, ack p99 17.6 ms; soar dlq map[non_retryable:12741]; soar dlq==rejected4xx true

