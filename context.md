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
- Local `main` remains ahead of `upstream/main` by 5 commits; `context.md` is untracked session notes.
