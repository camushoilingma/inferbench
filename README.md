# Inferbench

<p align="center">
  <img src="assets/inferbench-logo.png" alt="Inferbench" width="120">
</p>

Load generator for long-context, multi-turn LLM endpoints.

- Open-loop QPS ramp: fire on schedule, drop late arrivals, count in-window completions
- Closed-loop concurrency ramp (`run.py`)
- Multi-turn prefix cache with automatic session affinity
- Fixed or laddered prompt-size schedules for comparable levels
- Per-request and pre-run plan logs for diagnosis
- `--max-ctx` clamp (default `heavy` draws up to 780k tokens)

## Run

```bash
pip install -e .

export MYAPI_BASE_URL="https://your-host/v1"
export MYAPI_MODEL="your-model"
export MYAPI_API_KEY="sk-..."

python3 inferbench/run_openloop.py --provider myapi --mode real \
    --max-ctx 128000 --arrival poisson --seed 7 --dur 600 --warmup 60 \
    --drain-grace 90 --levels 2,4,6,8,10 --fixed-dist --stop-on-explode \
    --out results-seed-7.jsonl
```

Other flags: `--help`.

## Reading The Table

`OfferedQPS` is the rate you asked for. `DoneQPS` is successes that finished inside the measurement window, divided by that window.

A level held when `DoneQPS` is near `OfferedQPS`, `Growth` is near `0`, `Dropped = 0`, and `Left = 0`.

You pick the SLO. Goodput is the highest offered QPS that held and whose latency, usually `E2Ep95`, is under that SLO.

```text
OfferedQPS  DoneQPS  Growth  Dropped  Left  E2Ep95
      2.00     2.00       0        0     0     12s   held, under a 20s p95 SLO
      4.00     3.10      40        0     0     45s   missed the rate and the SLO
```

Latency and token metrics use the same population as `Done`. Drain is leftover work after arrivals stop; finishing later does not raise `DoneQPS`.

## Useful Flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--provider` | required | `PROVIDERS` key, or env: `<NAME>_BASE_URL` / `_MODEL` / `_API_KEY` |
| `--levels` | `1,2,3,4,5` | Offered QPS points |
| `--dur` | `90` | Seconds per level; longer means stabler tails |
| `--warmup` | `30` | Discard first N seconds from metrics |
| `--max-ctx` | off | Clamp every prompt to this many tokens |
| `--timeout` | `1800` | Per-request HTTP timeout in seconds; `0` disables it |
| `--token-param` | `both` | Send `max_tokens`, `max_completion_tokens`, or both |
| `--fixed-dist` | off | Same deterministic distribution-shaped size schedule every level |
| `--size-ladder` | off | Equal-share explicit sizes, e.g. `10000,25000,50000` |
| `--max-turns` | `8` | Turns per conversation before it retires |
| `--request-log` | off | Write one CSV row per completed request |
| `--plan-log` | off | Write each level's planned arrivals and cold-conversation sizes |
| `--out` | off | Write per-level rows to `.csv`, `.jsonl`, or `.json` |
| `--append` | off | Append to `--out`, useful for multiple seeds |
| `--arrival` | `uniform` | `poisson` or `uniform` |
| `--mode` | `real` | Cache breaks: `real`, `fixed`, or `broken` |
| `--pool` | `40` | Size of the fixed or ladder schedule |

Env: `INFERBENCH_CACHE_FRACTION` (default `0.59`), `INFERBENCH_REASONING_EFFORT`, `INFERBENCH_SESSION_HEADER`, `INFERBENCH_TOKEN_PARAM`. Optional: `INFERBENCH_ENV=/path/to/file` loads `KEY=value` lines into the process; real env vars win.

## Things That Bite

Every conversation sends its own id in `X-Session-ID` on every turn. Override the header with `INFERBENCH_SESSION_HEADER` or `<NAME>_SESSION_HEADER` if your router expects another name. A router that ignores the header is unaffected.

Ids are tagged with the seed and the run start time, so a rerun does not reuse an id for a different conversation and inherit the earlier replica cache.

`--max-ctx` clamps every prompt. Against a 32k or 128k model, uncapped `heavy` is errors or silent truncation, not a smaller workload.

`INFERBENCH_CACHE_FRACTION` is prompt composition, stable versus fresh, not a traffic-weighted hit. `--mode fixed` means never break the stable prefix; turn 1 is still cold.

Report `cache_hit_mean`, not just `cache_hit_p50`. The median can stay pinned near the design target while the mean moves with cold-turn share and routing misses.

`--timeout` defaults to 1800s. The heavy output tail is 35k tokens, so one decode can outlast a shorter timeout and look like an outage. Use `--timeout 0` to disable it.

The output cap is sent as both `max_tokens` and `max_completion_tokens`. If an API rejects one field, pass `--token-param max_tokens` or `--token-param max_completion_tokens`. The `openai` provider already sends only `max_completion_tokens`.

`Dropped > 0` means this process missed the schedule. It may be the client rather than the endpoint. Any drop halts the ramp.

`--request-log` helps separate throttling from failures and client queueing from endpoint latency. `http_status`, `retry_after`, `request_id`, `inflight`, `arrival_lag_ms`, and `finish_reason` are all preserved.

`--plan-log` writes the arrival times and cold-conversation sizes a level is about to produce before it runs. It does not change what is sent.

`--fixed-dist` preserves the role distribution shape. `--size-ladder` gives every explicit size rung an equal share, so compare per-rung rather than treating the pooled median as production-shaped.

## Multi-Seed Summary

```bash
python -m inferbench.summarize results-seed-*.jsonl --out summary.csv
```

The summary reports run count plus median, minimum, maximum, and spread for every numeric metric at each offered QPS. It does not grade SLOs or select a knee.

## Other Scripts

- `locked_hit.py` - pin cache hit, then ramp
- `scaling_probe.py` - prefill TTFT vs context length
- `decode_probe.py` - decode tok/s
- `run.py` - closed-loop concurrency ramp
- `validate_comp.py` - smoke-check warm-turn cache hit under affinity

Edit `ROLE_PROFILES` and `INFERBENCH_CACHE_FRACTION` to match your traffic.

## Tests

```bash
python tests/test_run_openloop.py
```

Contributions are welcome: open an issue or send a focused pull request. For measurement changes, include the command you used and what you observed.

If Inferbench helps you size or compare serving stacks, please [star the repository](https://github.com/Asad-Ismail/inferbench) or share it with others doing the same work.
