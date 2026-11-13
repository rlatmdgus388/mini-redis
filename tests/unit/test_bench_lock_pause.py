"""bench/lock_pause.py 스모크 테스트 (#20). 시간 값은 확인하지 않고 구조와 개수만 본다.

구현이 맞춰야 하는 이름:
- ROW_LABELS: {행 id: 표에 적는 이름} 9개(#26에서 나눠서 지우는 두 행이 prefix_delete 바로 뒤에 붙는다). 순서는 아래 ROW_IDS와 같다.
- build_store(row_id, n_keys, prefix_share) -> Store: 그 행을 재기 직전 상태의 Store.
- run_row(row_id, n_keys, prefix_share, use_gc=False) -> dict: 한 번 재고 "victim_calls"(구간 안에서 시작한
  다른 요청 수)를 담는다. prefix_delete 행은 "removed"(지워진 키 수)도 담는다.
- main(argv): --keys, --repeat, --prefix-share, --gc, --out.
- victim_stats(victim, t0, t1): victim는 count/starts/lats를 가진 객체. 겹치는 호출이 없으면 victim_calls 0,
  max_ns·p99_ns None (대체값 없음). 겹침 = 호출 끝 >= t0 그리고 호출 시작 <= t1. p99는 최근접 순위.
- run_row의 resize 행은 "initial_size", "final_size", "puts"(메인 스레드의 SET 수)도 담는다.
"""
from array import array
from types import SimpleNamespace

from bench.lock_pause import ROW_LABELS, build_store, main, run_row, victim_stats
from bench.rehash_latency import resize_points
from mini_redis.config import REHASH_BUCKETS_PER_STEP

ROW_IDS = [
    "control_a", "control_b", "prefix_delete", "prefix_delete_batched_1024", "prefix_delete_batched_16384",
    "keys", "snapshot", "resize_incremental", "resize_bulk",
]
BATCHED = ("prefix_delete_batched_1024", "prefix_delete_batched_16384")
N = 2000
SHARE = 0.1
BULK = 1 << 62
RESIZE = ("resize_incremental", "resize_bulk")


def test_lock_pause_row_labels_cover_the_nine_rows_in_order():
    assert list(ROW_LABELS) == ROW_IDS
    assert len(set(ROW_LABELS.values())) == 9


def test_lock_pause_row_labels_name_the_batched_rows():
    assert ROW_LABELS["prefix_delete_batched_1024"] == "prefix 삭제 (나눠서, 버킷 1,024개)"
    assert ROW_LABELS["prefix_delete_batched_16384"] == "prefix 삭제 (나눠서, 버킷 16,384개)"


def test_lock_pause_run_row_batched_rows_record_removed_victim_calls_and_no_scan():
    for row_id in BATCHED:
        r = run_row(row_id, N, SHARE)
        assert r["removed"] == int(N * SHARE), row_id
        assert "victim_calls" in r, row_id
        assert r["scan_ns"] is None, row_id


def test_lock_pause_main_small_run_lists_batched_rows_with_dash_in_scan_column(tmp_path):
    out = tmp_path / "lock_pause.md"

    main(["--keys", str(N), "--repeat", "1", "--out", str(out)])

    rows = _table_rows(out.read_text(encoding="utf-8"), "희생자 호출 수")
    for row_id in BATCHED:
        row = next(r for r in rows if r.startswith(f"| {ROW_LABELS[row_id]} |"))
        cells = [c.strip() for c in row.strip("|").split("|")]
        assert cells[3] == "-"  # 열 순서: 작업, 반복, 작업 시간, 스캔


def test_lock_pause_main_small_run_writes_results_with_every_row(tmp_path):
    out = tmp_path / "lock_pause.md"

    main(["--keys", str(N), "--repeat", "1", "--out", str(out)])

    md = out.read_text(encoding="utf-8")
    for label in ROW_LABELS.values():
        assert label in md
    assert f"{N:,}" in md  # 헤더에 키 수


def test_lock_pause_main_with_out_does_not_touch_default_results_dir(tmp_path):
    from pathlib import Path

    default = Path(__file__).resolve().parents[2] / "bench" / "results" / "lock_pause.md"
    before = default.read_bytes() if default.exists() else None

    main(["--keys", "500", "--repeat", "1", "--out", str(tmp_path / "x.md")])

    assert (default.read_bytes() if default.exists() else None) == before


def test_lock_pause_build_store_prefix_delete_has_share_of_keys_under_prefix():
    store = build_store("prefix_delete", N, SHARE)

    assert store.size() == N
    assert len(store.keys("user:")) == int(N * SHARE)
    assert len(store.keys("key:")) == N - int(N * SHARE)


def test_lock_pause_run_row_prefix_delete_removes_share_of_keys():
    r = run_row("prefix_delete", N, SHARE)

    assert r["removed"] == int(N * SHARE)


def test_lock_pause_build_store_resize_incremental_stops_just_below_last_threshold():
    store = build_store("resize_incremental", N, SHARE)
    last_put, _ = resize_points(N)[-1]

    assert store.size() == last_put - 1
    assert store._table._old is None  # 아직 리해싱을 시작하지 않았다
    assert store._table._rehash_buckets == REHASH_BUCKETS_PER_STEP
    assert store._max_keys > N * 4  # 리해싱이 끝날 때까지 CapacityExceeded가 나지 않을 만큼


def test_lock_pause_build_store_resize_bulk_swaps_in_bulk_rehash_table():
    store = build_store("resize_bulk", N, SHARE)
    last_put, _ = resize_points(N)[-1]

    assert store.size() == last_put - 1
    assert store._table._rehash_buckets == BULK
    assert store._max_keys > N * 4


def test_lock_pause_build_store_resize_bulk_next_put_finishes_resize_at_once():
    store = build_store("resize_bulk", N, SHARE)

    store.set("new:0", "v")

    assert store._table._old is None  # 한 put이 다 옮겼다


def test_lock_pause_build_store_resize_incremental_next_put_starts_rehash_and_keeps_it_pending():
    store = build_store("resize_incremental", N, SHARE)

    store.set("new:0", "v")

    assert store._table._old is not None  # 점진적이라 한 put으로는 안 끝난다


def _victim(calls):
    """(시작, 지연) 목록으로 만든 가짜 희생자. 시작 순서대로 넣는다."""
    return SimpleNamespace(
        count=len(calls),
        starts=array("q", [c[0] for c in calls]),
        lats=array("q", [c[1] for c in calls]),
    )


def test_lock_pause_run_row_rows_with_long_windows_record_victim_calls():
    # 스모크 크기에서 구간이 충분히 긴 행만: 대조 A(고정 길이)와 리사이즈(희생자 GET이 리해싱을 같이 한다)
    for row_id in ("control_a", "resize_incremental", "resize_bulk"):
        r = run_row(row_id, N, SHARE)
        assert r["victim_calls"] > 0, row_id


def test_lock_pause_victim_stats_no_call_overlaps_window_reports_zero_calls_and_none():
    # 한 호출은 구간 앞에서 끝났고, 다른 호출은 구간이 끝난 뒤에 시작했다
    stats = victim_stats(_victim([(0, 5), (30, 5)]), 10, 20)

    assert stats["victim_calls"] == 0
    assert stats["max_ns"] is None
    assert stats["p99_ns"] is None


def test_lock_pause_victim_stats_empty_victim_reports_zero_calls_and_none():
    stats = victim_stats(_victim([]), 10, 20)

    assert stats["victim_calls"] == 0
    assert stats["max_ns"] is None
    assert stats["p99_ns"] is None


def test_lock_pause_victim_stats_call_straddling_window_start_is_counted():
    # 시작 8, 끝 13: t0=10 앞에서 시작해 구간 안에서 끝났다
    stats = victim_stats(_victim([(0, 5), (8, 5), (30, 5)]), 10, 20)

    assert stats["victim_calls"] == 1
    assert stats["max_ns"] == 5


def test_lock_pause_victim_stats_call_straddling_window_end_is_counted():
    stats = victim_stats(_victim([(18, 50), (40, 1)]), 10, 20)

    assert stats["victim_calls"] == 1
    assert stats["max_ns"] == 50


def test_lock_pause_victim_stats_boundaries_end_at_t0_and_start_at_t1_are_both_counted():
    # (5, 5)는 정확히 t0에 끝나고, (20, 7)은 정확히 t1에 시작한다
    stats = victim_stats(_victim([(5, 5), (20, 7), (21, 9)]), 10, 20)

    assert stats["victim_calls"] == 2
    assert stats["max_ns"] == 7


def test_lock_pause_victim_stats_p99_is_nearest_rank_of_100_samples():
    calls = [(10 + i, 1) for i in range(99)] + [(109, 1000)]

    stats = victim_stats(_victim(calls), 0, 1000)

    assert stats["victim_calls"] == 100
    assert stats["p99_ns"] == 1  # ceil(0.99 * 100) = 99번째 값
    assert stats["max_ns"] == 1000


def test_lock_pause_victim_stats_p99_of_single_sample_is_that_sample():
    stats = victim_stats(_victim([(12, 33)]), 10, 20)

    assert stats["p99_ns"] == 33


def _table_rows(md, header_word):
    """헤더 줄에 header_word가 든 표의 데이터 줄(구분선 다음부터 표가 끝날 때까지)."""
    lines = md.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("|") and header_word in ln)
    rows = []
    for ln in lines[start + 2:]:
        if not ln.startswith("|"):
            break
        rows.append(ln)
    return rows


def test_lock_pause_main_results_list_every_repeat_as_its_own_row(tmp_path):
    out = tmp_path / "lock_pause.md"

    main(["--keys", str(N), "--repeat", "2", "--out", str(out)])

    md = out.read_text(encoding="utf-8")
    rows = _table_rows(md, "희생자 호출 수")
    for label in ROW_LABELS.values():
        assert len([r for r in rows if r.startswith(f"| {label} |")]) == 2, label
    assert len(rows) == 2 * len(ROW_LABELS)
    header = next(ln for ln in md.splitlines() if "희생자 호출 수" in ln)
    for column in ("작업", "반복", "작업 시간", "스캔", "최대 대기", "p99", "1ms 넘은 요청"):
        assert column in header


def test_lock_pause_main_results_have_min_median_max_summary_per_operation(tmp_path):
    out = tmp_path / "lock_pause.md"

    main(["--keys", str(N), "--repeat", "3", "--out", str(out)])

    md = out.read_text(encoding="utf-8")
    header = next(ln for ln in md.splitlines() if ln.startswith("|") and "최솟값" in ln)
    for word in ("최대 대기", "최솟값", "중앙값", "최댓값"):
        assert word in header
    rows = _table_rows(md, "최솟값")
    assert len(rows) == len(ROW_LABELS)  # 작업마다 한 줄
    for label in ROW_LABELS.values():
        assert len([r for r in rows if r.startswith(f"| {label} |")]) == 1, label


def test_lock_pause_main_results_do_not_keep_per_statistic_minimum_wording(tmp_path):
    out = tmp_path / "lock_pause.md"

    main(["--keys", "500", "--repeat", "1", "--out", str(out)])

    assert "통계마다 가장 작은 값" not in out.read_text(encoding="utf-8")


def test_lock_pause_run_row_resize_rows_record_sizes_and_counts():
    last_put, _ = resize_points(N)[-1]
    for row_id in RESIZE:
        r = run_row(row_id, N, SHARE)
        assert r["initial_size"] == last_put - 1, row_id
        assert r["puts"] > 0, row_id
        assert r["final_size"] == r["initial_size"] + r["puts"], row_id  # 새 키만 넣으니 SET 수만큼 늘어난다
        assert r["victim_calls"] > 0, row_id  # 희생자 GET 수


def test_lock_pause_main_results_name_resize_rows_and_show_their_own_initial_size(tmp_path):
    out = tmp_path / "lock_pause.md"
    last_put, _ = resize_points(N)[-1]

    main(["--keys", str(N), "--repeat", "1", "--out", str(out)])

    md = out.read_text(encoding="utf-8")
    assert "GET이 같이 옮겨 주는 리해싱, 두 스레드" in md
    assert f"{last_put - 1:,}" in md  # 리사이즈 행의 초기 크기는 N과 다르다
    assert last_put - 1 != N
