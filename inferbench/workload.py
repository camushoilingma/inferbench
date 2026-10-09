"""Long-context, multi-turn size distribution and prefix-cache composition.

Replace ROLE_PROFILES (quantile, tokens) points to match your traffic.
  total  = full context per turn
  output = completion tokens per turn
"""
import hashlib
import math
import os
import random

try:
    from inferbench.utils import VOCAB
except ImportError:
    from utils import VOCAB

ROLE_PROFILES = {
    "heavy": {
        "total":   [(0.0, 2000), (0.05, 13000), (0.25, 32000), (0.5, 80000), (0.75, 160000),
                    (0.90, 270000), (0.95, 350000), (0.99, 500000), (1.0, 780000)],
        "output":  [(0.0, 20), (0.5, 230), (0.95, 2300), (0.99, 5600), (1.0, 35000)],
        "break_rate": 0.12,
    },
    "light": {
        "total":   [(0.0, 1000), (0.5, 68000), (0.9, 156000), (0.99, 250000), (1.0, 400000)],
        "output":  [(0.0, 20), (0.5, 630), (0.95, 2000), (1.0, 6000)],
        "break_rate": 0.02,
    },
    # Prosus production GLM-5.2 planner, 7d to 2026-09-24.
    # input  p50=131k  p95=299k  p99=501k,  output  p50=240  p95=2,929  p99=7,517.
    # Cache 0.99 designed; Tencent returned 0.988-0.989 per request, 0.70-0.76 token-weighted.
    "planner_glm52": {
        "total":   [(0.0, 2000), (0.05, 25000), (0.25, 74000), (0.50, 131000),
                    (0.75, 195000), (0.90, 262000), (0.95, 299000), (0.99, 501000),
                    (1.0, 850000)],
        "output":  [(0.0, 10), (0.10, 60), (0.50, 240), (0.75, 750),
                    (0.90, 1600), (0.95, 2929), (0.99, 7517), (1.0, 25000)],
        "break_rate": 0.01,   # 0.99 designed cache — only 1 in 100 turns breaks deliberately
    },
}

SIZE_SCALE = 1.0  # set < 1 for cheap dry runs
MAX_CTX = 0       # clamp sampled totals; 0 = no clamp
FIXED_PREFIX_TOKENS = 25000
# Designed warm-turn cache fraction. A stable-prefix-only model hits ~95% (too high).
CACHE_FRACTION = float(os.environ.get("INFERBENCH_CACHE_FRACTION", "0.59"))

_FIXED_PREFIX_WORDS = None


def clamp_ctx(tokens):
    if MAX_CTX and MAX_CTX > 0:
        return min(float(tokens), float(MAX_CTX))
    return float(tokens)


def quantile_at(points, u):
    """Log-interpolated value at quantile u in [0, 1]."""
    for i in range(1, len(points)):
        q0, v0 = points[i - 1]
        q1, v1 = points[i]
        if u <= q1:
            f = (u - q0) / (q1 - q0) if q1 > q0 else 0.0
            lo, hi = math.log(max(v0, 1)), math.log(max(v1, 1))
            return math.exp(lo + f * (hi - lo))
    return float(points[-1][1])


def sample_pct(points, rng):
    return quantile_at(points, rng.random())


def ladder_totals(rungs, n, seed=0):
    """A FIXED, deterministic list of n request sizes dealt from `rungs`, where every
    consecutive block of len(rungs) entries contains each rung exactly once, shuffled within
    the block so the order is not a repeating pattern. n is rounded up to a whole number of
    blocks, so cycling the list keeps the mix exactly balanced for any request count.

    Differs from stratified_totals, which samples evenly-spaced QUANTILES and therefore
    inherits the distribution's shape -- the tail stays as rare as it is in production (~4%
    above 400k on the default profile). A ladder gives every rung an EQUAL share instead.
    Use it when the largest requests dominate the SLO and the natural tail is too thin to
    measure: it fixes both the "each level rolled its own dice" ambiguity and the "the
    slowest traffic is the least sampled" problem in one step.

    Cost: the size mix is deliberately NOT representative of production traffic, so pooled
    medians from a ladder run describe the ladder. Compare per-rung, not pooled.
    """
    rungs = [clamp_ctx(r) for r in rungs]
    if not rungs:
        raise ValueError("ladder_totals needs at least one rung")
    k = len(rungs)
    blocks = max(1, -(-int(n) // k))
    out = []
    for b in range(blocks):
        deck = list(rungs)
        random.Random(f"ladder:{seed}:{b}").shuffle(deck)
        out += deck
    return out


def _van_der_corput(k):
    """k-th point of the base-2 low-discrepancy sequence: 0, .5, .25, .75, .125, ...
    Any prefix of it covers [0,1) about as evenly as that many points can."""
    x, f, r = int(k), 0.5, 0.0
    while x:
        r += f * (x & 1)
        x >>= 1
        f *= 0.5
    return r


def stratified_totals(role, n):
    """A FIXED, deterministic list of n request sizes spanning the distribution (evenly-spaced
    quantiles, tail included), so the heavy-tailed SHAPE is preserved exactly while the set of
    sizes is pinned -- every level walks the identical schedule and cannot draw a luckier
    sample than its neighbour.

    Dealt in van der Corput order so EVERY PREFIX is spread across the whole quantile range.
    That matters because levels differ in length, and a prefix has to be representative in
    SHAPE, not just in mean. Ordering greedily on the running mean gets the mean right but
    front-loads mid-sizes: the first 80 of 120 then contain no small and no very large
    requests at all, which is the same sample-mix bias the stratification exists to remove.
    """
    prof = ROLE_PROFILES[role]["total"]
    remaining, ordered = list(range(n)), []
    for k in range(n):
        target = _van_der_corput(k)
        s = min(remaining, key=lambda i: abs((i + 0.5) / n - target))
        remaining.remove(s)
        ordered.append(clamp_ctx(quantile_at(prof, (s + 0.5) / n)))
    return ordered


def diverse_text(approx_tokens, rng):
    """Non-repeating text so two conversations do not share cacheable blocks."""
    n = max(1, int(approx_tokens * SIZE_SCALE))
    return " ".join(rng.choice(VOCAB) for _ in range(n))


def _fixed_words():
    global _FIXED_PREFIX_WORDS
    if _FIXED_PREFIX_WORDS is None:
        r = random.Random(424242)
        _FIXED_PREFIX_WORDS = [r.choice(VOCAB) for _ in range(FIXED_PREFIX_TOKENS)]
    return _FIXED_PREFIX_WORDS


def fixed_prefix(n_tokens=FIXED_PREFIX_TOKENS):
    """Leading slice of the global system prefix (so small requests still share a prefix)."""
    words = _fixed_words()
    k = max(0, min(len(words), int(n_tokens * SIZE_SCALE)))
    return "[SYSTEM PROMPT + TOOL SCHEMA] " + " ".join(words[:k])


class Conversation:
    """Stable prefix + per-turn fresh block. Cache-break prepends a nonce to the system prompt."""

    def __init__(self, role, rng, total=None):
        assert role in ROLE_PROFILES
        self.role = role
        self.rng = rng
        prof = ROLE_PROFILES[role]
        self.total = clamp_ctx(sample_pct(prof["total"], rng) if total is None else total)
        cache_budget = int(CACHE_FRACTION * self.total)
        self.fixed_tokens = min(FIXED_PREFIX_TOKENS, cache_budget)
        self.stable_tokens = max(0, cache_budget - self.fixed_tokens)
        self.fresh_tokens = max(1, int((1.0 - CACHE_FRACTION) * self.total))
        self.stable_block = diverse_text(self.stable_tokens, rng) if self.stable_tokens else ""
        self.turn = 0
        self.nonce = ""

    def next_turn(self, break_cache=False):
        self.turn += 1
        if break_cache:
            self.nonce = hashlib.md5(f"{self.turn}:{self.rng.random()}".encode()).hexdigest()
        system = (self.nonce + "\n" if self.nonce else "") + fixed_prefix(self.fixed_tokens)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": "[context] " + self.stable_block},
            {"role": "assistant", "content": "(ack — continuing)"},
            {"role": "user", "content": f"[turn {self.turn}] " + diverse_text(self.fresh_tokens, self.rng)},
        ]
        max_out = int(sample_pct(ROLE_PROFILES[self.role]["output"], self.rng))
        return messages, max_out


def should_break(role, rng, mode):
    """fixed = never, broken = always, real = per-role rate."""
    if mode == "fixed":
        return False
    if mode == "broken":
        return True
    return rng.random() < ROLE_PROFILES[role]["break_rate"]
