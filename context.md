# inferbench session context

## Current Session (2026-08-28)
- Local repo: `/Users/camusma/code/Prosus-tokenhub/inferbench`.
- Origin: `https://github.com/camushoilingma/inferbench.git`.
- Upstream: `https://github.com/Asad-Ismail/inferbench`.
- Fetched both remotes and merged `upstream/main` into local `main`.
- Merge commit: `1e0caf3` (`Merge upstream inferbench changes`).
- Resolved conflicts in README, provider, run, open-loop runner, and workload files.
- Preserved local request logging, fixed distributions, size ladders, turn caps, and automatic `X-Session-ID`.
- Took upstream `DoneQPS`/in-window accounting, `locked_hit.py`, `utils.py`, `--max-ctx`, `--timeout`, and `--token-param`.
- Validation passed: `PYTHONDONTWRITEBYTECODE=1 python3 tests/test_run_openloop.py` (25/25) and compileall with `PYTHONPYCACHEPREFIX=/private/tmp/inferbench-pycache`.
- Pushed to `origin/main` on `https://github.com/camushoilingma/inferbench.git`: `b1c8902..1e0caf3`.
- Local `main` remains ahead of `upstream/main` by 5 commits; `context.md` committed to fork.

## 2026-08-28 — CVM Benchmark Runs and Skill Creation
- Created CVMs: GZ `ins-5xdrbm4o` (114.132.240.191, ap-guangzhou, sg-80hz55d1), SG `ins-glon4twk` (43.128.69.31, ap-singapore, sg-6dclvazg). Both S5.LARGE8, Ubuntu 24.04 (img-mmytdhbn).
- SSH key pair: `camus-keypair` (local key `~/.ssh/id_ed25519`, user `ubuntu`).
- API keys: GZ `~/code/Prosus-tokenhub/gzapi` (sk-7wyC... — EXPIRED, all 401), SG `~/code/Prosus-tokenhub/10_m_api` (sk-AG8M... — VALID, 0 errors).
- Security groups: Both had SSH open to 0.0.0.0/0. LOCKED DOWN to user IP 81.249.144.139/32 via Tencent Python SDK.
- Merged harness requires env vars: `TENCENT_BASE_URL=https://tokenhub-intl.tencentcloudmaas.com/v1`, `TENCENT_MODEL=glm-5.2`, `TENCENT_API_KEY=<key>`.
- Run command: `python3 -u run_openloop.py --provider tencent --mode real --levels 1 --dur 1800 --arrival poisson --seed 24 --max-turns 8 --fixed-dist --pool 120 --token-param both --out ... --request-log ... --plan-log ...`
- SG run (20260828_183244Z): 1789/1800 done, 0 errors, done_qps 1.011, TTFT p50 3.6s, E2E p50 7.3s, cache_hit_mean 0.515. SUCCESS.
- GZ run (20260828_183243Z): 0/1800 done, 1800 errors (all 401). API key expired. NEEDS NEW KEY.
- CodeBuddy skill "tencent-cvm-benchmark" created at `~/code/Prosus-tokenhub/.codebuddy/skills/tencent-cvm-benchmark/` with SKILL.md, scripts/create_cvm_with_ssh_lockdown.py, references/run_scripts.md. Auto-locks SSH to user IP on every CVM creation.
- Committed context.md to fork: `a002839` on `origin/main`.
