# Hypnos spike results

Provisional. Measured in a Debian bookworm container on Docker Desktop's arm64 Linux VM (Apple Silicon host), not on the target PC. `synchronous=FULL` on actor commits, so the write numbers include fsync. These numbers do not close the kill gate.

```
wasmtime 49.0.2  arch aarch64  os linux
flags: asimd
Linux version 6.10.14-linuxkit
glibc 2.36
```

Guest is QuickJS via rquickjs 0.14, compiled to `wasm32-wasip1`. Javy was not needed. A baseline x86_64 `.cwasm` (no inferred AVX) compiles here and is rejected by this host, which is the expected mismatch.

## Wake

Times are microseconds, p50 / p95 / p99. Cold is 30 iterations after `drop_caches`. Hot and warm are 1000. Read is `SELECT`. Write is `INSERT` plus the full fsync.

| Class | Handler | Meter | Open | Instantiate | Eval | Handler | Total |
|---|---|---|---|---|---|---|---|
| Hot | read | on | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 15 / 24 / 42 | 27 / 41 / 67 |
| Warm | read | on | 47 / 67 / 108 | 3 / 6 / 12 | 192 / 258 / 379 | 19 / 31 / 57 | 281 / 384 / 604 |
| Cold | read | on | 245 / 488 / 532 | 15 / 28 / 53 | 362 / 433 / 494 | 52 / 104 / 167 | 821 / 1190 / 1280 |
| Hot | write | on | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 33 / 51 / 106 | 412 / 1699 / 1918 |
| Warm | write | on | 69 / 146 / 206 | 7 / 17 / 23 | 262 / 406 / 558 | 44 / 84 / 129 | 1327 / 3492 / 4021 |
| Cold | write | on | 267 / 402 / 1581 | 18 / 25 / 26 | 384 / 459 / 480 | 67 / 95 / 104 | 1936 / 3145 / 3460 |
| Hot | read | off | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 14 / 20 / 34 | 16 / 23 / 39 |
| Warm | read | off | 47 / 68 / 116 | 3 / 6 / 13 | 187 / 260 / 352 | 18 / 32 / 57 | 263 / 377 / 556 |
| Cold | read | off | 231 / 373 / 382 | 10 / 22 / 26 | 322 / 411 / 421 | 44 / 73 / 83 | 606 / 905 / 909 |
| Hot | write | off | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 27 / 44 / 94 | 374 / 1656 / 1823 |
| Warm | write | off | 61 / 92 / 146 | 5 / 10 / 16 | 217 / 311 / 374 | 36 / 57 / 93 | 1231 / 2595 / 3379 |
| Cold | write | off | 296 / 602 / 683 | 11 / 25 / 40 | 316 / 407 / 506 | 63 / 130 / 156 | 1808 / 4289 / 4464 |

The heap check passed. After three writes the counters were heap 1, 2, 3 and sql 1, 2, 3. After sleep the live-store count was 0, and the next wake was heap 1, sql 4. A 50 ms sweep then dropped the instance.

Eval is the engine cost. Instantiate is a few microseconds. The write path is slower than the read path because the actor file uses `synchronous=FULL`. That gap is disk, not the interpreter.

The meter adds about 0.1 ms at warm-write p50 (1327 vs 1231) and about 0.6 ms at p99 (4021 vs 3379). It is visible and it is not the wake.

## Lines locked from this run

Recorded before any tuning, so a later change cannot move them.

The line fixed before the run passes. Warm-write p50 is 1.3 ms, under 10 ms. Instantiate plus eval is about 0.27 ms at p50, under half of that total.

For later runs on this same container:

- Warm-write with the meter stays under 3 ms p50 and 10 ms p99.
- Instantiate plus eval stays under 2 ms p99.
- A miss is a regression. It does not set a new baseline.

A real PC still has to repeat the measurement. If instantiate plus eval alone is tens of milliseconds there, the design is wrong. Faster storage will not fix it.

## Other checks

Traps, all in one process. A heap blowup dies at the 64 MiB cap. An infinite loop dies on the epoch deadline. Deep recursion dies on the wasm stack. A thrown handler and a syntax error both roll the actor transaction back, write one request row and one cpu row, and the next request on that actor succeeds.

Agent. A mock model answered after 5 seconds. While it waited, a second actor's p99 was 0.7 ms against an idle p99 of 1.8 ms. The agent's cpu row was 0.87 ms. A disallowed host, and the same host on another port, never reached the mock. The API key was not in guest memory. A write before `env.AI.fetch` survived a later trap. The write after it did not.

Crash. 100 aborts at each of four points, plus 500 ready-gated SIGKILLs. Every trial passed `integrity_check`, had `0 <= requests - cpus <= 1`, and kept every acknowledged actor write.

Churn. 5,000 actors, 5 rounds. Anonymous RSS stayed at 26,244 KB with the pooling allocator off and about 26,990 KB with it on. No growth after round 2.

Boot scan. This is a design signal, not a tuning knob.

| Files | Warm | Cold | Per file, cold |
|---|---|---|---|
| 1,000 | 61 ms | 179 ms | 179 us |
| 10,000 | 608 ms | 1.7 s | 169 us |
| 100,000 | 15.8 s | 34.5 s | 345 us |

100,000 files take tens of seconds, cold or warm. The alarm row in each actor file is too slow to scan at startup once the directory is large. The fallback, if this is still true on the real box, is an alarm index in `system.sqlite` used as a cache and rebuilt from the files only when it is missing or marked dirty. That is a design change. It is not a spike fix, and it does not touch the wake path.

## Verdict

GO, provisional, for this container only. The wake is in the millisecond class, and the engine is the small part of it. The boot scan is the thing that will not scale as "one SQLite file per actor" without an alarm index. The Arch PC section below is the measurement that counts.

## Arch PC

The first `sudo wake-bench` on `/tmp/wake`. Intel Core Ultra 7 355, x86_64, Linux 7.2.4-arch1-2, glibc 2.44, rustc 1.99.0, flags sse4.1, sse4.2, avx, avx2. Totals only: this table has no open, instantiate, eval, or handler split and no iteration count. Times are p50 / p95 / p99 in microseconds.

| Class | Total µs |
|---|---|
| Hot read | 57 / 115 / 127 |
| Warm read | 544 / 590 / 760 |
| Cold read | 946 / 1001 / 1007 |
| Hot write | 41 / 46 / 55 |
| Warm write | 585 / 609 / 647 |
| Cold write | 985 / 1028 / 1049 |

`fixed-line warm-write p50_us 585 engine_us 397 OUTSIDE` and `PASS wake-bench`. `OUTSIDE` means QuickJS at 397 µs is more than half of the 585 µs warm write, because this disk is faster than the container where the fsync dominated. The NO-GO line is an engine time above 20 ms. This engine time is 397 µs.

On that same run the agent checks passed. Reject passed. Idle p99 was 128 µs, live p99 was 94 µs, and CPU was 1.15 ms during a 5 s model wait. The commit point passed.

A later bench on the same `/tmp/wake` printed `FAIL warm heap check got {"heap":1,"sql":6}, want heap 1 sql 1`. The guest reset (`heap: 1`). The SQL file still held five rows from the first run, so the next insert was 6. That failure does not replace the first bench. Leave `/tmp/wake` alone.

## Verdict on the Arch PC

The engine kill gate passes. 397 µs is under 20 ms. The warm write is still sub-millisecond. One SQLite file per actor stays the wake path. An alarm index is still later work, for the boot scan, and it does not belong on this path.
