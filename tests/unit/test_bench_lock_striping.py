import re
import sys
import threading
from pathlib import Path

import pytest

import bench.lock_striping as ls
from bench.lock_striping import StripedTable, default_out, main, run_cell
from mini_redis.hashtable import fnv1a_64


class _RecLock:
    """잠금 대신 끼워 넣는 기록용 객체. with로 들어올 때 자기 번호를 log에 적는다."""

    def __init__(self, idx, log):
        self.idx = idx
        self.log = log

    def __enter__(self):
        self.log.append(self.idx)
        return self

    def __exit__(self, *exc):
        return False


def _recording(table):
    # 표의 잠금 목록을 기록용 객체로 통째로 바꿔 끼우고, 기록이 쌓일 log를 돌려준다.
    log = []
    table._locks = [_RecLock(i, log) for i in range(len(table._locks))]
    return log


def _expected_lock(key, capacity, stripes):
    return (fnv1a_64(key) & (capacity - 1)) & (stripes - 1)


def test_striped_table_set_then_get_returns_value():
    t = StripedTable(16, 4)
    t.set("a", 1)
    t.set("b", 2)
    assert t.get("a") == 1 and t.get("b") == 2


def test_striped_table_set_existing_key_updates_in_place():
    t = StripedTable(16, 4)
    t.set("a", 1)
    t.set("a", 99)
    assert t.get("a") == 99


def test_striped_table_get_missing_key_returns_none():
    t = StripedTable(16, 4)
    assert t.get("nope") is None


@pytest.mark.parametrize(
    "capacity, stripes",
    [(12, 4), (16, 3), (16, 0), (32, 64), (0, 1)],
)
def test_striped_table_invalid_sizes_raise_value_error(capacity, stripes):
    with pytest.raises(ValueError):
        StripedTable(capacity, stripes)


@pytest.mark.parametrize("capacity, stripes", [(64, 8), (64, 1), (16, 16), (32, 2)])
def test_get_and_set_acquire_the_expected_stripe_lock(capacity, stripes):
    t = StripedTable(capacity, stripes)
    log = _recording(t)
    for i in range(40):
        key = f"k:{i}"
        want = _expected_lock(key, capacity, stripes)
        log.clear()
        t.set(key, i)
        assert log == [want]
        log.clear()
        t.get(key)
        assert log == [want]


@pytest.mark.parametrize("capacity, stripes", [(64, 8), (16, 4)])
def test_get_at_and_set_at_acquire_lock_of_bucket_index(capacity, stripes):
    t = StripedTable(capacity, stripes)
    log = _recording(t)
    for i in range(40):
        key = f"k:{i}"
        idx = fnv1a_64(key) & (capacity - 1)
        log.clear()
        t.set_at(idx, key, i)
        assert log == [idx & (stripes - 1)]
        log.clear()
        t.get_at(idx, key)
        assert log == [idx & (stripes - 1)]


def test_striped_table_single_stripe_uses_lock_zero_for_every_key():
    t = StripedTable(64, 1)
    assert len(t._locks) == 1
    log = _recording(t)
    for i in range(50):
        t.get(f"k:{i}")
        t.set(f"k:{i}", i)
    assert set(log) == {0} and len(log) == 100


def test_striped_table_keys_spread_over_more_than_one_lock():
    t = StripedTable(64, 8)
    log = _recording(t)
    for i in range(100):
        t.get(f"k:{i}")
    assert len(set(log)) > 1


def test_get_at_set_at_round_trip():
    t = StripedTable(16, 4)
    idx = fnv1a_64("a") & 15
    t.set_at(idx, "a", 1)
    assert t.get_at(idx, "a") == 1
    t.set_at(idx, "a", 2)
    assert t.get_at(idx, "a") == 2
    assert t.get_at(idx, "missing") is None


def test_get_equals_get_at_with_hashed_bucket_index():
    t = StripedTable(32, 4)
    for i in range(60):
        t.set(f"k:{i}", i)
    for i in range(70):
        key = f"k:{i}"
        assert t.get(key) == t.get_at(fnv1a_64(key) & 31, key)


def test_set_equals_set_at_with_hashed_bucket_index():
    a, b = StripedTable(32, 4), StripedTable(32, 4)
    for i in range(60):
        key = f"k:{i}"
        a.set(key, i)
        b.set_at(fnv1a_64(key) & 31, key, i)
    for i in range(60):
        assert a.get(f"k:{i}") == b.get(f"k:{i}") == i


def test_build_ops_carries_precomputed_bucket_indexes():
    # 구현 쪽에 build_ops(rng_seed, n_ops, keys, mix_read_pct, capacity) -> [(is_read, key, bucket_index)]가 있어야 한다.
    keys = [f"k:{i}" for i in range(50)]
    cap = 64
    ops = ls.build_ops(7, 500, keys, 90, cap)
    assert len(ops) == 500
    for is_read, key, idx in ops:
        assert isinstance(is_read, bool)
        assert key in keys
        assert idx == fnv1a_64(key) & (cap - 1)


def test_build_ops_same_seed_same_list_and_mix_extremes():
    keys = [f"k:{i}" for i in range(20)]
    a = ls.build_ops(3, 2000, keys, 100, 32)
    assert a == ls.build_ops(3, 2000, keys, 100, 32)
    assert all(op[0] for op in a)
    assert not any(op[0] for op in ls.build_ops(3, 200, keys, 0, 32))


def test_striped_table_four_threads_disjoint_keys_all_read_back():
    # 버킷 4개에 키 수백 개라 체인이 길게 겹친다. 스위치 간격을 극단으로 줄여 스레드가 자주 바뀌게 한다.
    t = StripedTable(4, 2)
    per_thread = 300
    old = sys.getswitchinterval()

    def work(tid):
        for i in range(per_thread):
            t.set(f"t{tid}:{i}", tid * 1000 + i)

    sys.setswitchinterval(1e-6)
    try:
        workers = [threading.Thread(target=work, args=(tid,)) for tid in range(4)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
    finally:
        sys.setswitchinterval(old)
    for tid in range(4):
        for i in range(per_thread):
            assert t.get(f"t{tid}:{i}") == tid * 1000 + i


@pytest.mark.parametrize("mix", [100, 90, 50])
def test_run_cell_each_mix_reports_ops_and_consistent_rate(mix):
    keys = [f"k:{i}" for i in range(100)]
    t = StripedTable(256, 4)
    for k in keys:
        t.set(k, 0)
    r = run_cell(t, mix, 2, 0.05, keys, 27)
    assert r["ops"] > 0
    assert r["elapsed_s"] > 0
    assert r["ops_per_s"] == pytest.approx(r["ops"] / r["elapsed_s"])


def _cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def test_main_small_run_writes_every_row_and_summary(tmp_path):
    out = tmp_path / "small.md"
    main([
        "--keys", "1000", "--threads", "1,2", "--stripes", "1,4",
        "--mixes", "100,50", "--seconds", "0.05", "--repeat", "2",
        "--out", str(out),
    ])
    assert out.exists()
    text = out.read_text(encoding="utf-8")

    # 숫자로 시작하는 표 행만 모은다. 앞 16줄 = 표1(반복마다 한 줄), 뒤 8줄 = 표2(요약).
    rows = []
    for line in text.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = _cells(line)
        nums = [re.search(r"\d+", c) for c in cells[:4]]
        if cells and nums[0] and nums[1] and nums[2]:
            rows.append([m.group() if m else None for m in nums])
    assert len(rows) == 16 + 8

    table1 = {tuple(int(x) for x in r[:4]) for r in rows[:16]}
    expected1 = {
        (mix, th, k, rep)
        for mix in (100, 50) for th in (1, 2) for k in (1, 4) for rep in (1, 2)
    }
    # 반복 번호가 0부터인지 1부터인지는 정하지 않았으니, 번호 차이만 맞는지 본다.
    reps = sorted({t[3] for t in table1})
    assert len(reps) == 2
    shift = reps[0] - 1
    assert {(a, b, c, d - shift) for a, b, c, d in table1} == expected1

    table2 = {tuple(int(x) for x in r[:3]) for r in rows[16:]}
    assert table2 == {
        (mix, th, k) for mix in (100, 50) for th in (1, 2) for k in (1, 4)
    }


def test_main_small_run_does_not_touch_bench_results_dir(tmp_path):
    results = Path(__file__).resolve().parents[2] / "bench" / "results"

    def snap():
        if not results.exists():
            return []
        return sorted(
            (p.name, p.stat().st_mtime_ns, p.stat().st_size)
            for p in results.glob("lock_striping*")
        )

    before = snap()
    main([
        "--keys", "200", "--threads", "1", "--stripes", "1",
        "--mixes", "100", "--seconds", "0.02", "--repeat", "1",
        "--out", str(tmp_path / "x.md"),
    ])
    assert before == snap()


def test_default_out_gil_enabled_points_to_gil_file(monkeypatch):
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    assert Path(default_out()).name == "lock_striping_gil.md"


def test_default_out_gil_disabled_points_to_nogil_file(monkeypatch):
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert Path(default_out()).name == "lock_striping_nogil.md"
