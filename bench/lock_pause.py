"""락 멈춤 측정 (#20): O(N) 작업이 전역 락을 잡는 동안 다른 요청이 얼마나 기다리는지 잰다.

한 Store에 키 N개를 넣고, 다른 스레드(희생자)가 존재하는 키 하나를 계속 get하게 둔다.
메인 스레드가 O(N) 작업을 하는 구간에서 시작한 희생자 호출의 지연이 곧 "다른 요청이 락 앞에서 기다린 시간"이다.
bench/rehash_latency.py(#24)는 HashTable.put 하나의 시간만 쟀다. 그 README가 "락을 기다리는 다른 요청"을 이 이슈에 넘겨서,
여기서는 put 시간을 다시 재지 않고 기다리는 쪽만 본다.

행:
- 대조 A(희생자 혼자): 메인 스레드가 아무 일도 안 한다. 구간 길이는 그 반복에서 가장 긴 작업과 같다(최대 10초).
- 대조 B(GIL만): 락 없이 파이썬 작업만 O(N)으로 한다. GIL 하나가 희생자에게 주는 영향을 따로 본다.
- prefix 삭제 / KEYS / 스냅샷 복사: Store의 O(N) 연산. 락을 끝까지 잡는다.
- 리사이즈(점진적, main) / 리사이즈(일괄, 비교용): 마지막 리사이즈 문턱 바로 아래까지 채운 Store에 새 키를 넣어 리사이즈가 끝날 때까지 간다.

기본은 GC를 끄고 잰다(#24와 같은 이유: GC 멈춤이 작업 효과를 가린다). --gc로 켤 수 있다.
벽시계 값이라 행을 번갈아 --repeat번 돌리고, 결과에는 반복마다 한 줄씩 그대로 적는다(합쳐서 만든 최솟값은 쓰지 않는다).
작업마다 최대 대기의 최솟값 / 중앙값 / 최댓값을 따로 요약한다.
Store 단계의 측정이다. HTTP는 자기 오버헤드를 더하고, GIL 넘겨받기 비용은 대조 B가 보여 준다.

레포 루트에서:  python -m bench.lock_pause [--keys 1000000] [--repeat 3] [--prefix-share 0.1] [--gc] [--out PATH]
"""
import argparse
import gc
import os
import platform
import statistics
import sys
import threading
import time
from array import array
from pathlib import Path

from bench.rehash_latency import BULK, resize_points
from mini_redis.config import REHASH_BUCKETS_PER_STEP
from mini_redis.hashtable import HashTable
from mini_redis.store import Store

OUT = Path(__file__).parent / "results" / "lock_pause.md"
PREFIX = "user:"
HOT_KEY = "key:0"  # 희생자가 읽는 키. 모든 행에서 존재한다(접두사 키는 user:, 나머지는 key:)
WARMUP_S = 0.05
TAIL_S = 0.05
DEFAULT_WINDOW_S = 0.2  # 대조 A를 단독으로 부를 때의 구간 길이
MAX_CONTROL_WINDOW_S = 10.0
CAPACITY = 6_000_000  # 희생자가 기록할 수 있는 호출 수. 미리 잡아 두어 기록 중 할당이 끼지 않게 한다
OVER_NS = 1_000_000  # 1ms
SMALL_N = 100_000  # 이보다 작은 키 수에서는 리사이즈 행 구간이 너무 짧다

ROW_LABELS = {
    "control_a": "대조 A: 희생자 혼자",
    "control_b": "대조 B: GIL만 (락 없는 파이썬 작업)",
    "prefix_delete": "prefix 삭제 (DEL prefix)",
    "prefix_delete_batched_1024": "prefix 삭제 (나눠서, 버킷 1,024개)",
    "prefix_delete_batched_16384": "prefix 삭제 (나눠서, 버킷 16,384개)",
    "keys": "KEYS prefix",
    "snapshot": "스냅샷 복사",
    "resize_incremental": "리사이즈 (점진적, main)",
    "resize_bulk": "리사이즈 (일괄, 비교용)",
}
RESIZE_ROWS = ("resize_incremental", "resize_bulk")
BATCHED_ROWS = {"prefix_delete_batched_1024": 1024, "prefix_delete_batched_16384": 16384}  # #26: 행 id -> 덩어리당 버킷 수
RESIZE_NAME = "GET이 같이 옮겨 주는 리해싱, 두 스레드"
NO_OVERLAP = "겹친 요청 없음"


def build_store(row_id, n_keys, prefix_share):
    """그 행을 재기 직전 상태의 Store."""
    if row_id in RESIZE_ROWS:
        points = resize_points(n_keys)
        if not points:
            raise ValueError(f"키 {n_keys}개 안에는 리사이즈가 없어요 (--keys를 늘려 주세요)")
        fill = points[-1][0] - 1  # 마지막 리사이즈를 일으키는 put 바로 앞까지
        # 점진적 리해싱은 마지막 배열을 다 옮기려면 (배열 크기 / 버킷 수)번 명령이 필요해서 MAX_KEYS를 넘는다.
        # CapacityExceeded가 끼어 측정이 끊기지 않게 상한을 충분히 크게 준다.
        store = Store(max_keys=n_keys * 4 + 1)
        if row_id == "resize_bulk":
            # 측정용으로 비공개 필드를 바꿔 끼운다: 옛 _resize처럼 한 put이 다 옮기는 테이블.
            store._table = HashTable(rehash_buckets=BULK)
        for i in range(fill):
            store.set(f"key:{i}", "v")
        return store
    store = Store()
    n_prefix = int(n_keys * prefix_share) if row_id in ("prefix_delete", "keys", *BATCHED_ROWS) else 0
    for i in range(n_prefix):
        store.set(f"{PREFIX}{i}", "v")
    for i in range(n_keys - n_prefix):
        store.set(f"key:{i}", "v")
    return store


class Victim(threading.Thread):
    """존재하는 키를 계속 get하며 (시작 시각, 지연)을 미리 잡아 둔 배열에 적는다."""

    def __init__(self, store):
        super().__init__(daemon=True)
        self._store = store
        self.starts = array("q", bytes(8 * CAPACITY))
        self.lats = array("q", bytes(8 * CAPACITY))
        self.count = 0
        self.overflow = False
        self.stop_flag = False

    def run(self):
        get, now, starts, lats, i = self._store.get, time.perf_counter_ns, self.starts, self.lats, 0
        while not self.stop_flag:
            if i >= CAPACITY:
                self.overflow = True
                break
            t = now()
            get(HOT_KEY)
            lats[i] = now() - t
            starts[i] = t
            i += 1
            self.count = i


def victim_stats(victim, t0, t1):
    """작업 구간 [t0, t1]과 겹치는 호출의 지연 통계. 겹침 = 호출 끝 >= t0 그리고 호출 시작 <= t1.

    겹치는 호출이 없으면 victim_calls = 0, 나머지는 None이다(대체값을 만들지 않는다).
    구간이 아주 짧아 메인이 GIL을 쥔 채 끝나면 실제로 그럴 수 있다.
    p99는 최근접 순위(ceil(0.99 * n)번째로 작은 값, 최소 1번째)로 bench/cache_effect.py와 같은 정의다.
    """
    n = victim.count
    starts, lats = victim.starts, victim.lats
    first = 0
    while first < n and starts[first] + lats[first] < t0:  # 끝 시각은 시작 순서대로 늘어난다
        first += 1
    window = []
    for i in range(first, n):
        if starts[i] > t1:
            break
        window.append(lats[i])
    window.sort()
    k = len(window)
    if k == 0:
        return {"victim_calls": 0, "max_ns": None, "p99_ns": None, "over_1ms": None}
    rank = max(1, (99 * k + 99) // 100)  # ceil(0.99 * k)
    return {
        "victim_calls": k,
        "max_ns": window[-1],
        "p99_ns": window[rank - 1],
        "over_1ms": sum(x > OVER_NS for x in window),
    }


def _op(row_id, store, n_keys):
    """행마다 메인 스레드가 하는 작업을 돌려주는 함수. 반환값은 행별 부가 정보(dict)."""
    if row_id == "control_b":
        names = [f"key:{i}" for i in range(n_keys)]

        def work():
            for name in names:
                name.startswith(PREFIX)
            return {}
        return work
    if row_id == "prefix_delete":
        return lambda: {"removed": store.delete_prefix(PREFIX)}
    if row_id in BATCHED_ROWS:
        buckets = BATCHED_ROWS[row_id]
        return lambda: {"removed": store.delete_prefix_batched(PREFIX, buckets)}
    if row_id == "keys":
        return lambda: {"found": len(store.keys(PREFIX))}
    if row_id == "snapshot":
        return lambda: {"copied": len(store.copy_for_snapshot())}
    if row_id in RESIZE_ROWS:
        cap = resize_points(n_keys)[-1][1]
        limit = cap // REHASH_BUCKETS_PER_STEP * 2 + 100  # 리해싱이 끝나는 데 필요한 put 수의 두 배쯤

        def work():
            i = 0
            while True:
                store.set(f"new:{i}", "v")
                i += 1
                if i == 1 and n_keys < SMALL_N:
                    # 스모크처럼 작은 크기에서는 구간이 GIL 전환 간격(5ms)보다 짧아 희생자가 한 번도 못 돌 수 있다.
                    # 그래서 첫 SET 뒤 1ms 쉬어 희생자가 구간 안에서 돌게 한다. 실제 크기(SMALL_N 이상)에는 넣지 않는다.
                    time.sleep(0.001)
                if store._table._old is None:  # 리해싱이 끝났다
                    return {"puts": i}
                if i > limit:
                    raise RuntimeError(f"put {i}번 안에 리해싱이 끝나지 않았어요")
        return work
    raise ValueError(f"모르는 행: {row_id}")


def run_row(row_id, n_keys, prefix_share, use_gc=False, window_s=None):
    """한 번 재서 dict를 돌려준다. window_s는 대조 A의 구간 길이(초)."""
    store = build_store(row_id, n_keys, prefix_share)
    scan_ns = None
    if row_id == "prefix_delete":
        # 스캔만 따로 잰다. 비공개 메서드를 쓰는 이유: delete_prefix 시간 중 순회가 얼마인지 나누려는 것이다.
        # 만료 키가 없어 스캔은 상태를 바꾸지 않으므로 같은 Store에서 이어서 삭제를 잰다.
        t = time.perf_counter_ns()
        with store._lock:
            store._live_keys_with_prefix(PREFIX)
        scan_ns = time.perf_counter_ns() - t
    work = None if row_id == "control_a" else _op(row_id, store, n_keys)
    initial_size = store.size()

    gc.collect()
    was_enabled = gc.isenabled()
    if not use_gc:
        gc.disable()
    victim = Victim(store)
    try:
        victim.start()
        time.sleep(WARMUP_S)
        t0 = time.perf_counter_ns()
        if work is None:
            time.sleep(window_s if window_s is not None else DEFAULT_WINDOW_S)
            extra = {}
        else:
            extra = work()
        t1 = time.perf_counter_ns()
        time.sleep(TAIL_S)
    finally:
        victim.stop_flag = True
        victim.join()
        if was_enabled:
            gc.enable()
    if victim.overflow:
        raise RuntimeError(f"희생자 기록 칸({CAPACITY:,})이 찼어요")
    result = {"duration_ns": t1 - t0, "scan_ns": scan_ns, **extra, **victim_stats(victim, t0, t1)}
    if row_id in RESIZE_ROWS:
        # 이 행들은 N이 아니라 마지막 리사이즈 문턱 바로 아래에서 시작하므로 크기를 따로 남긴다.
        result["initial_size"] = initial_size
        result["final_size"] = store.size()
    return result


ORDER = ("control_b", "prefix_delete", *BATCHED_ROWS, "keys", "snapshot", "resize_incremental", "resize_bulk")


def measure(n_keys, repeat, prefix_share, use_gc):
    """{행 id: [반복별 결과]}. 행을 번갈아 돌리고, 대조 A는 그 반복의 가장 긴 작업과 같은 길이로 마지막에 돌린다."""
    runs = {row_id: [] for row_id in ROW_LABELS}
    for _ in range(repeat):
        for row_id in ORDER:
            runs[row_id].append(run_row(row_id, n_keys, prefix_share, use_gc))
        longest_s = max(runs[r][-1]["duration_ns"] for r in ORDER) / 1e9
        window = min(longest_s, MAX_CONTROL_WINDOW_S)
        runs["control_a"].append(run_row("control_a", n_keys, prefix_share, use_gc, window_s=window))
    return runs


def _ms(ns):
    return NO_OVERLAP if ns is None else f"{ns / 1e6:.2f}"


def _summary(rs):
    """반복들의 최대 대기 (최솟값, 중앙값, 최댓값). 겹친 요청이 없던 반복은 뺀다. 하나도 없으면 None."""
    vals = [r["max_ns"] for r in rs if r["max_ns"] is not None]
    if not vals:
        return None
    return min(vals), statistics.median(vals), max(vals)


def to_markdown(n_keys, repeat, prefix_share, use_gc, runs):
    ms, us = 1e6, 1e3
    n_prefix = int(n_keys * prefix_share)
    lines = [
        "# 락 멈춤 측정 결과",
        "",
        f"한 Store에 키 {n_keys:,}개를 넣고, 다른 스레드가 `{HOT_KEY}`를 계속 get하는 동안 메인 스레드가 O(N) 작업을 한다.",
        "\"최대 대기\"는 작업 구간과 겹친 그 get들(끝 >= 작업 시작, 시작 <= 작업 끝)의 지연이다.",
        f"겹친 요청이 하나도 없으면 호출 수는 0이고 지연 칸은 \"{NO_OVERLAP}\"이다.",
        "Store 단계의 측정이다. HTTP는 자기 오버헤드를 더하고, GIL 넘겨받기 비용은 대조 B가 보여 준다.",
        "`python -m bench.lock_pause`가 이 파일을 다시 만든다.",
        "",
        f"- 기계: {platform.platform()}, {platform.processor()}, CPU {os.cpu_count()}개, Python {platform.python_version()}",
        f"- 키 수 N: {n_keys:,} (prefix 행은 `{PREFIX}` 키 {n_prefix:,}개 = 비율 {prefix_share}, 나머지는 `key:`)",
        f"- 반복: 행을 번갈아 {repeat}번, 반복마다 한 줄씩 그대로 적는다",
        f"- GC: {'켬' if use_gc else '끔'}",
        f"- sys.getswitchinterval(): {sys.getswitchinterval()}초",
        "",
        "## 반복별 결과",
        "",
        "| 작업 | 반복 | 작업 시간(ms) | (prefix만) 스캔(ms) | 최대 대기(ms) | p99(µs) | 1ms 넘은 요청 | 희생자 호출 수 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row_id, label in ROW_LABELS.items():
        for i, r in enumerate(runs[row_id], 1):
            scan = "-" if r["scan_ns"] is None else f"{r['scan_ns'] / ms:.2f}"
            p99 = NO_OVERLAP if r["p99_ns"] is None else f"{r['p99_ns'] / us:.1f}"
            over = NO_OVERLAP if r["over_1ms"] is None else f"{r['over_1ms']:,}"
            lines.append(
                f"| {label} | {i} | {r['duration_ns'] / ms:.2f} | {scan} | {_ms(r['max_ns'])} "
                f"| {p99} | {over} | {r['victim_calls']:,} |"
            )
    lines += [
        "",
        "## 작업별 최대 대기 요약(ms)",
        "",
        "| 작업 | 최대 대기 최솟값 | 최대 대기 중앙값 | 최대 대기 최댓값 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row_id, label in ROW_LABELS.items():
        summ = _summary(runs[row_id])
        cells = [NO_OVERLAP] * 3 if summ is None else [f"{x / ms:.2f}" for x in summ]
        lines.append(f"| {label} | {cells[0]} | {cells[1]} | {cells[2]} |")
    lines += [
        "",
        f"## {RESIZE_NAME}",
        "",
        f"리사이즈 두 행은 {RESIZE_NAME}이다. 희생자의 GET도 버킷을 옮기므로 점진적 행의 리해싱은 두 스레드가 나눠 한다.",
        "이 행들의 처음 크기는 N과 다르다. 마지막 리사이즈 문턱 바로 아래까지만 채운 Store에서 시작해, 새 키를 넣어 리사이즈가 끝날 때까지 가기 때문이다.",
        "",
        "| 작업 | 반복 | 처음 크기 | 최종 크기 | 메인 스레드 SET 수 | 희생자 GET 수 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row_id in RESIZE_ROWS:
        for i, r in enumerate(runs[row_id], 1):
            lines.append(
                f"| {ROW_LABELS[row_id]} | {i} | {r['initial_size']:,} | {r['final_size']:,} "
                f"| {r['puts']:,} | {r['victim_calls']:,} |"
            )
    lines += [
        "",
        "대조 A의 \"작업 시간\"은 구간 길이(그 반복에서 가장 긴 작업, 최대 10초)다.",
        "prefix 삭제의 스캔은 `_live_keys_with_prefix`만 락 안에서 돌린 시간이고, 같은 Store에서 이어서 삭제를 쟀다.",
    ]
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description="락 멈춤 측정: O(N) 작업 중 다른 요청의 대기 시간")
    p.add_argument("--keys", type=int, default=1_000_000)
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--prefix-share", type=float, default=0.1)
    p.add_argument("--gc", action="store_true", help="측정 중에도 GC를 켠다")
    p.add_argument("--out", type=Path, default=OUT)
    args = p.parse_args(argv)

    runs = measure(args.keys, args.repeat, args.prefix_share, args.gc)
    md = to_markdown(args.keys, args.repeat, args.prefix_share, args.gc, runs)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(md)


if __name__ == "__main__":
    main()
