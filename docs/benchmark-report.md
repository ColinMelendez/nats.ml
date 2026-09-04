# Protocol optimizer benchmark

This report compares the pure protocol and client state-machine hot paths when
the complete library is compiled with OCaml 5.5.0 and Flambda under three
profiles:

| Profile | Native-code flags |
|---|---|
| No optimization | `-Oclassic` |
| O3 | `-O3` |
| O3 with closure unboxing | `-O3 -unbox-closures` |

`-Oclassic` is OCaml's practical low-optimization native-code baseline; it does
not claim that every mandatory compiler transformation is disabled.

## Pre-optimization reference run

- Date: 2026-09-03
- Machine: Apple M2 Pro, 10 cores, 16 GiB RAM
- Platform: Darwin 23.6.0 arm64
- OCaml: 5.5.0, Flambda enabled, flat-float-array disabled
- Dune: 3.24.2
- Cases: 19 packet parsing, codec, encoding, and client-delivery operations

> **Exploratory timing only.** The three profiles were measured at host loads
> of 10.5, 10.0, and 10.2 on 10 cores. Thumper therefore marked every timing
> run as loaded. The runner rejects this condition by default; it was overridden
> to obtain a provisional comparison after repeated quiet-host attempts failed.
> These timings must not be used as release regression baselines. Allocation
> counts are exact and remain suitable for comparing the generated programs.

### Wall time

Negative percentages are faster.

| Case | No optimization | O3 | O3 vs no optimization | O3 + closure unboxing | Unboxing vs no optimization | Unboxing vs O3 |
|---|---:|---:|---:|---:|---:|---:|
| `client/deliver-1024-subscriptions` | 7.26 us | 6.23 us | -14.2% | 5.08 us | -30.1% | -18.6% |
| `client/deliver-one-subscription` | 1.28 us | 869.90 ns | -31.9% | 1.07 us | -16.5% | +22.7% |
| `client/unknown-sid-1024-subscriptions` | 3.78 us | 2.71 us | -28.3% | 2.46 us | -35.0% | -9.3% |
| `codec/hmsg-4096-headers-16` | 16.40 us | 16.52 us | +0.7% | 16.80 us | +2.4% | +1.7% |
| `codec/msg-4096` | 6.49 us | 6.54 us | +0.7% | 6.33 us | -2.5% | -3.1% |
| `codec/msg-64` | 871.50 ns | 828.80 ns | -4.9% | 842.20 ns | -3.4% | +1.6% |
| `codec/msg-64-coalesced-16` | 26.26 us | 25.38 us | -3.4% | 26.10 us | -0.6% | +2.8% |
| `codec/ping` | 498.10 ns | 607.90 ns | +22.0% | 789.40 ns | +58.5% | +29.9% |
| `encode/wire-hpub-4096-headers-16` | 3.30 us | 4.71 us | +42.7% | 3.65 us | +10.4% | -22.6% |
| `encode/wire-pub-1m` | 175.50 us | 178.80 us | +1.9% | 163.50 us | -6.8% | -8.6% |
| `encode/wire-pub-4096` | 1.66 us | 1.60 us | -3.4% | 1.64 us | -1.5% | +2.0% |
| `encode/wire-pub-64` | 240.00 ns | 238.40 ns | -0.7% | 245.50 ns | +2.3% | +3.0% |
| `packet/hmsg-4096-headers-16` | 6.84 us | 7.06 us | +3.2% | 7.21 us | +5.4% | +2.2% |
| `packet/msg-0` | 660.40 ns | 894.20 ns | +35.4% | 847.30 ns | +28.3% | -5.2% |
| `packet/msg-1m` | 1.12 ms | 1.07 ms | -4.5% | 1.09 ms | -2.4% | +2.1% |
| `packet/msg-4096` | 8.04 us | 6.79 us | -15.6% | 6.19 us | -23.0% | -8.8% |
| `packet/msg-4096-fragmented-1024` | 3.48 us | 3.17 us | -9.0% | 3.39 us | -2.6% | +7.0% |
| `packet/msg-64` | 1.50 us | 710.40 ns | -52.8% | 992.70 ns | -34.0% | +39.7% |
| `packet/ping` | 457.10 ns | 447.80 ns | -2.0% | 466.70 ns | +2.1% | +4.2% |

The loaded run suggests that O3 helps the larger state-machine lookups and
4 KiB packet path, while closure unboxing may help the many-subscription and
unknown-subscription cases. The contradictory sub-microsecond results and wide
confidence intervals show why no timing conclusion should be frozen yet.

### Allocation

| Case | No optimization | O3 | O3 + closure unboxing |
|---|---:|---:|---:|
| `client/deliver-1024-subscriptions` | 3,818 words | 3,790 words | 3,769 words |
| `client/deliver-one-subscription` | 749 words | 721 words | 700 words |
| `client/unknown-sid-1024-subscriptions` | 701 words | 683 words | 667 words |
| `codec/hmsg-4096-headers-16` | 4,597 words | 4,573 words | 4,552 words |
| `codec/msg-4096` | 3,227 words | 3,215 words | 3,199 words |
| `codec/msg-64` | 686 words | 674 words | 658 words |
| `codec/msg-64-coalesced-16` | 16,502 words | 16,310 words | 16,054 words |
| `codec/ping` | 570 words | 564 words | 548 words |
| `encode/wire-hpub-4096-headers-16` | 2,663 words | 2,658 words | 2,650 words |
| `encode/wire-pub-1m` | 393,259 words | 393,259 words | 393,259 words |
| `encode/wire-pub-4096` | 1,575 words | 1,575 words | 1,575 words |
| `encode/wire-pub-64` | 61 words | 61 words | 61 words |
| `packet/hmsg-4096-headers-16` | 3,373 words | 3,364 words | 3,348 words |
| `packet/msg-0` | 613 words | 605 words | 589 words |
| `packet/msg-1m` | 655,979 words | 655,971 words | 655,955 words |
| `packet/msg-4096` | 3,178 words | 3,170 words | 3,154 words |
| `packet/msg-4096-fragmented-1024` | 2,791 words | 2,783 words | 2,767 words |
| `packet/msg-64` | 637 words | 629 words | 613 words |
| `packet/ping` | 568 words | 564 words | 548 words |

O3 allocates no more than the low-optimization profile in every case. Explicit
closure unboxing reduces allocations further in every closure-bearing case and
is equal on the three wire-encoding cases. Across one invocation of all 19
cases, the totals are 1,095,547 words, 1,095,169 words, and 1,094,666 words.
The large payload copies dominate those totals; the client-delivery reductions
are more meaningful locally.

## Tidy-pass results

The September 3 tidy pass removed repeated scans and intermediate strings from
the packet, codec, drain, and reconnect paths. Allocation counts below are exact
for the O3 profile; timing samples were taken on a loaded host and are therefore
directional only.

| Case | Before | After | Change |
|---|---:|---:|---:|
| Decode 1 MiB packet | 655,971 words | 524,894 words | -20.0% |
| Decode 4 KiB message | 3,215 words | 2,698 words | -16.1% |
| Encode 1 MiB publish | 393,259 words | 131,117 words | -66.7% |
| Encode 4 KiB publish | 1,575 words | 553 words | -64.9% |
| Encode 4 KiB header publish | 2,658 words | 1,513 words | -43.1% |
| Render 1 MiB packet | 393,225 words | 131,083 words | -66.7% |
| Drain 1,024 subscriptions | 47,147 words | 37,932 words | -19.5% |
| Replay 1,024 subscriptions | 58,594 words | 55,522 words | -5.2% |

The packet-boundary scan now stops at the first CRLF instead of continuing over
the whole payload. Loaded-host samples showed the 1 MiB packet case falling from
roughly 1.1 ms to 0.18 ms, but that timing should be repeated on a quiet host
before it becomes a regression threshold. The benchmark suite now includes 22
cases, adding packet rendering plus large subscription drain and reconnect
replay coverage.

## Reproducing and accepting a baseline

Run the matrix from a new or empty directory as documented in the README. A
release baseline should only be accepted when all profiles complete without
the `measured under load` warning and their confidence intervals are narrow.
The generated directory contains the raw text, JSON, and Thumper baseline files
needed to audit or repeat the comparison. Loaded-host baseline files from this
reference run are intentionally not committed.
