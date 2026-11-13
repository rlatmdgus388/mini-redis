from bench.xfetch import run, to_markdown
from mini_redis.config import LEASE_RETRY_MS, ORIGIN_DELAY_MS

ARGS = dict(clients=50, interval_ms=1000, duration_s=30, ttl_s=5)  # 초당 50건: 시드와 무관하게 안정적인 영역


def test_xfetch_bench_lease_only_waits_at_each_expiry_and_never_refreshes_early():
    r = run(False, **ARGS)
    assert r["early"] == 0
    assert r["waited"] > 0
    assert r["max"] <= ORIGIN_DELAY_MS + LEASE_RETRY_MS  # 기다림은 원본 한 번 + 재시도 한 번 안쪽


def test_xfetch_bench_with_xfetch_nobody_waits_because_refresh_happens_before_expiry():
    r = run(True, **ARGS)
    assert r["early"] > 0
    assert r["waited"] == 0
    assert r["max"] == 0


def test_xfetch_bench_with_xfetch_nobody_waits_for_every_seed_at_enough_traffic():
    assert all(run(True, seed=s, **ARGS)["waited"] == 0 for s in (1, 2, 3))


def test_xfetch_bench_low_traffic_xfetch_can_miss_a_cycle_but_never_waits_more_than_lease_only():
    # 초당 20건에서는 만료 전에 당첨이 안 나는 주기가 있다 (시드 3). 그래도 lease만보다 더 기다리지는 않는다
    low = dict(clients=20, interval_ms=1000, duration_s=30, ttl_s=5)
    base = run(False, **low)["waited"]
    waits = [run(True, seed=s, **low)["waited"] for s in (1, 2, 3)]
    assert max(waits) > 0  # 확률적이라 놓치는 주기가 실제로 있다
    assert all(w <= base for w in waits)


def test_xfetch_bench_origin_calls_about_one_per_ttl_cycle_for_both_modes():
    cycles = ARGS["duration_s"] // ARGS["ttl_s"]
    for use in (False, True):
        r = run(use, **ARGS)
        assert cycles - 1 <= r["origin_calls"] <= cycles + 1  # XFetch가 낭비해서 호출을 늘리지 않는다


def test_xfetch_bench_same_seed_gives_same_numbers():
    assert run(True, **ARGS) == run(True, **ARGS)


def test_xfetch_bench_markdown_has_comparison_and_sweep_tables():
    class A:
        clients, interval_ms, duration_s, ttl_s = 50, 1000, 30, 5

    results = {False: run(False, **ARGS), True: run(True, **ARGS)}
    md = to_markdown(A, results, [(10, 10, 20, [0, 1, 0, 0, 2])])
    assert "| lease만 |" in md and "| lease + XFetch |" in md
    assert "요청률별" in md and "| 10 (10) | 20 | 0.6 | 2 | 3 / 5 |" in md
