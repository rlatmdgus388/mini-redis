"""락 쪼개기 처리량 측정 (#27): 전역 락 하나와 버킷별 락(striped lock)에서 스레드를 늘렸을 때 초당 연산 수를 비교한다.

Store와 HashTable은 건드리지 않고, 이 파일 안에 고정 크기 해시 표(StripedTable)를 따로 만들어 잰다.
락이 K개면 버킷 번호의 아래 비트(bucket & (K-1))로 락을 고른다. K=1이면 같은 코드에 락이 하나라서 전역 락 기준선이 된다.
리사이즈와 삭제는 없다. 락 개수 말고 다른 변수가 끼지 않게 하려는 것이다.

CPython 3.14(GIL 있음)와 free-threaded 3.14t(GIL 없음)에서 각각 따로 돌린다. 한 번에 한 인터프리터씩, 다른 일을 멈추고 돌린다.
벽시계 값이라 셀마다 --repeat번 돌려 최소/중앙/최대를 같이 적는다.

레포 루트에서:  python -m bench.lock_striping [--keys 100000] [--threads 1,2,4,8] [--stripes 1,16,256]
"""
import argparse
import os
import platform
import random
import statistics
import sys
import threading
import time
from pathlib import Path

from mini_redis.hashtable import fnv1a_64

RESULTS = Path(__file__).parent / "results"
OPS_LEN = 200_000  # 스레드마다 미리 만들어 두는 연산 목록 길이. 끝나면 처음부터 다시 돈다.
CHECK_EVERY = 256  # 시계는 이 횟수마다 한 번만 본다.
LOAD_FACTOR = 0.75


class _Node:
    __slots__ = ("key", "value", "next")

    def __init__(self, key, value, next):
        self.key = key
        self.value = value
        self.next = next


def _is_pow2(n):
    return n >= 1 and (n & (n - 1)) == 0


class StripedTable:
    """고정 크기 체이닝 해시 표. 락 stripes개가 버킷을 나눠 맡는다."""

    def __init__(self, capacity, stripes):
        if not _is_pow2(capacity) or not _is_pow2(stripes) or stripes > capacity:
            raise ValueError("capacity와 stripes는 2의 거듭제곱이어야 하고 stripes <= capacity여야 해요")
        self._mask = capacity - 1
        self._stripe_mask = stripes - 1
        self._buckets = [None] * capacity
        self._locks = [threading.Lock() for _ in range(stripes)]

    def get_at(self, i, key):
        """버킷 번호 i를 이미 계산해 둔 읽기. 락은 호출 때마다 self._locks에서 고른다."""
        with self._locks[i & self._stripe_mask]:
            node = self._buckets[i]
            while node is not None:
                if node.key == key:
                    return node.value
                node = node.next
        return None

    def set_at(self, i, key, value):
        """버킷 번호 i를 이미 계산해 둔 쓰기."""
        with self._locks[i & self._stripe_mask]:
            node = self._buckets[i]
            while node is not None:
                if node.key == key:
                    node.value = value
                    return
                node = node.next
            self._buckets[i] = _Node(key, value, self._buckets[i])

    def get(self, key):
        return self.get_at(fnv1a_64(key) & self._mask, key)

    def set(self, key, value):
        self.set_at(fnv1a_64(key) & self._mask, key, value)


def build_ops(rng_seed, n_ops, keys, mix_read_pct, capacity):
    """(is_read, key, bucket_index) 목록. 해시는 여기서(재기 전에) 미리 계산해서 재는 구간에는 해시가 없다."""
    rng = random.Random(rng_seed)
    n = len(keys)
    mask = capacity - 1
    ops = []
    for _ in range(n_ops):
        is_read = rng.randrange(100) < mix_read_pct
        key = keys[rng.randrange(n)]
        ops.append((is_read, key, fnv1a_64(key) & mask))
    return ops


_ops_cache = {}


def _make_ops(keys, mix_read_pct, seed, capacity):
    """같은 인자면 같은 목록이라 캐시해서 셀마다 다시 만들지 않는다."""
    ck = (id(keys), len(keys), mix_read_pct, seed, capacity)
    ops = _ops_cache.get(ck)
    if ops is None:
        ops = build_ops(seed, OPS_LEN, keys, mix_read_pct, capacity)
        if len(_ops_cache) > 64:
            _ops_cache.clear()
        _ops_cache[ck] = ops
    return ops


def run_cell(table, mix_read_pct, threads, seconds, keys, seed):
    """스레드 threads개가 seconds초 동안 get/set을 섞어 부른다. {"ops", "elapsed_s", "ops_per_s"}."""
    capacity = table._mask + 1
    op_lists = [_make_ops(keys, mix_read_pct, seed + i, capacity) for i in range(threads)]
    shared = {}
    perf = time.perf_counter

    def release():  # 배리어가 열리는 순간 한 스레드가 한 번 실행한다
        shared["t0"] = perf()
        shared["deadline"] = shared["t0"] + seconds

    barrier = threading.Barrier(threads, action=release)
    counts = [0] * threads
    ends = [0.0] * threads
    def worker(idx):
        # 바운드 메서드는 스레드마다 따로 만든다. 하나를 같이 쓰면 GIL 없는 빌드에서 그 객체의 참조 횟수를
        # 모든 스레드가 함께 고치느라 K와 상관없이 줄을 설 수 있어요 (해시를 미리 계산하는 것과 같은 이유).
        get_at, set_at = table.get_at, table.set_at
        ops = op_lists[idx]
        n = len(ops)
        pos = 0
        done = 0
        barrier.wait()
        deadline = shared["deadline"]
        while perf() < deadline:
            for _ in range(CHECK_EVERY):
                is_read, key, bi = ops[pos]
                pos += 1
                if pos == n:
                    pos = 0
                if is_read:
                    get_at(bi, key)
                else:
                    set_at(bi, key, done)
            done += CHECK_EVERY
        counts[idx] = done
        ends[idx] = perf()

    workers = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = max(ends) - shared["t0"]
    total = sum(counts)
    return {"ops": total, "elapsed_s": elapsed, "ops_per_s": total / elapsed}


def _gil_enabled():
    f = getattr(sys, "_is_gil_enabled", None)
    return True if f is None else bool(f())


def default_out():
    name = "lock_striping_gil.md" if _gil_enabled() else "lock_striping_nogil.md"
    return RESULTS / name


def _capacity_for(n_keys):
    cap = 1
    while cap < n_keys / LOAD_FACTOR:
        cap *= 2
    return cap


def _ints(text):
    return [int(x) for x in text.split(",") if x]


def _build(keys, capacity, stripes):
    table = StripedTable(capacity, stripes)
    for k in keys:
        table.set(k, 0)
    return table


def to_markdown(args, capacity, rows):
    gil = _gil_enabled()
    lines = [
        "# 락 쪼개기 처리량 측정 결과 (GIL 있음)" if gil else "# 락 쪼개기 처리량 측정 결과 (GIL 없음)",
        "",
        f"- 컴퓨터: {platform.platform()}, CPU 논리 코어 {os.cpu_count()}개",
        f"- 파이썬: {' '.join(sys.version.split())}",
        f"- GIL: {'켜짐' if gil else '꺼짐(free-threaded)'}",
        f"- 키 {args.keys:,}개(`k:0`부터, 고르게 뽑음), 표 {capacity:,}칸(고정, 리사이즈 없음)",
        f"- 셀마다 {args.seconds}초, 반복 {args.repeat}번, 시드 {args.seed}",
        f"- 읽기 비율(%): {args.mixes}, 스레드: {args.threads}, 락 개수 K: {args.stripes}",
        "",
        "`python -m bench.lock_striping`이 이 파일을 다시 만든다. 벽시계 값이라 돌릴 때마다 조금씩 다르다.",
        "",
        "## 반복마다 나온 값",
        "",
        "| 읽기% | 스레드 | K | 반복 | 연산 수 | 걸린 시간(s) | 연산/초 |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for (mix, th, k, rep), r in rows:
        lines.append(f"| {mix} | {th} | {k} | {rep} | {r['ops']:,} | {r['elapsed_s']:.3f} | {r['ops_per_s']:,.0f} |")
    by_cell = {}
    for (mix, th, k, _), r in rows:
        by_cell.setdefault((mix, th, k), []).append(r["ops_per_s"])
    lines += [
        "",
        "## 셀별 요약",
        "",
        "속도 배수는 같은 읽기%에서 스레드 1, K=1의 중앙값을 1로 둔 값이다.",
        "",
        "| 읽기% | 스레드 | K | 최소 | 중앙 | 최대 | 속도 배수(중앙) |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for (mix, th, k), vals in by_cell.items():
        med = statistics.median(vals)
        base = statistics.median(by_cell[(mix, 1, 1)]) if (mix, 1, 1) in by_cell else None
        sp = f"{med / base:.2f}x" if base else "-"
        lines.append(f"| {mix} | {th} | {k} | {min(vals):,.0f} | {med:,.0f} | {max(vals):,.0f} | {sp} |")
    lines += [
        "",
        "## 한계",
        "",
        "- 리사이즈와 삭제가 없는 고정 크기 표다. 실제 HashTable은 리사이즈 중에 락을 더 넓게 잡아야 한다.",
        "- 한 프로세스 안의 스레드만 잰다. HTTP, JSON, Store 검증 비용은 들어 있지 않다.",
        "- 해시(fnv1a)는 재기 전에 미리 계산해 재는 구간에서 뺐다. free-threaded에서 재는 구간 안에서 해시를 부르면 K와 상관없이 스레드가 직렬화됐다(추측: 모듈 전역 정수 객체의 참조 횟수를 여럿이 다퉈서). 그래서 뺐다.",
        "- 그래도 표와 락 객체는 모든 스레드가 같이 쓴다. 이 공유가 이 측정이 보려는 진짜 경합이다.",
        "- `threading.Lock`의 획득/해제는 파이썬 수준 비용이라 K가 커져도 그 비용은 그대로다.",
        "- 키는 고르게(uniform) 뽑는다. 특정 키에 몰리는(hot key) 부하에서는 락을 쪼개도 이득이 작다.",
        "- 벽시계 값이라 다른 프로세스의 CPU 사용에 흔들린다.",
    ]
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description="전역 락 vs 버킷별 락 처리량 (#27)")
    p.add_argument("--keys", type=int, default=100_000)
    p.add_argument("--threads", default="1,2,4,8")
    p.add_argument("--stripes", default="1,16,256")
    p.add_argument("--mixes", default="100,90,50", help="읽기 비율(%%) 목록")
    p.add_argument("--seconds", type=float, default=1.0)
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--seed", type=int, default=27)
    p.add_argument("--out", default=None)
    args = p.parse_args(argv)

    threads_list, stripes_list, mixes = _ints(args.threads), _ints(args.stripes), _ints(args.mixes)
    keys = [f"k:{i}" for i in range(args.keys)]
    capacity = _capacity_for(args.keys)
    out = Path(args.out) if args.out else default_out()

    rows = []
    for rep in range(1, args.repeat + 1):
        for mix in mixes:
            for th in threads_list:
                for k in stripes_list:
                    table = _build(keys, capacity, k)
                    r = run_cell(table, mix, th, args.seconds, keys, args.seed)
                    rows.append(((mix, th, k, rep), r))
                    print(f"rep {rep} mix {mix} threads {th} K {k}: done", flush=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(to_markdown(args, capacity, rows), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
