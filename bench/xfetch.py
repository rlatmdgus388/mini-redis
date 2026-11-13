"""XFetch 조기 갱신 측정 (#23): 인기 키를 계속 읽는 클라이언트들에게 lease만 쓸 때와 XFetch까지 쓸 때를 비교한다.

lease만으로는 만료 순간 한 명이 원본을 부르는 동안(ORIGIN_DELAY_MS) 나머지가 모두 miss를 받고 기다린다.
XFetch는 만료 전에 한 명이 미리 갱신해서 이 기다림을 없앤다. 원본 호출 수는 둘 다 TTL 주기마다 한 번이라
거의 같고, 달라지는 것은 기다린 요청 수와 꼬리 지연이다.

lease_scenarios(#19)와 같은 방식이다. 가짜 시계 위 이벤트 시뮬레이션이라 실제 스레드 없이 요청이 Store에 닿는 순서를
정확히 만들고, 캐시 요청은 TestClient로 실제 HTTP API(라우트, GET의 lease 필드, 404 본문, 409)를 거친다.
요청 자체는 0ms로 친다. 난수는 시드를 고정해서 매번 같은 숫자가 나온다.

클라이언트 규약 (이 벤치가 보여 주는 사용법):
  GET /keys/hot?lease=1
    200 + "lease" 없음 -> 평범한 히트. 값을 쓴다.
    200 + "lease" 있음 -> 값은 그대로 쓰고, 별도로 원본을 불러 {"lease": 토큰}으로 PUT해 미리 갱신한다.
    404 + "lease"      -> miss. 원본을 불러 PUT한다.
    404 + retry_after_ms -> 다른 클라이언트가 갱신 중. 그 시간 뒤 다시 GET한다.

레포 루트에서:  python -m bench.xfetch [--clients 100] [--interval-ms 1000] [--duration-s 120] [--ttl-s 10] [--no-sweep]
"""
import argparse
import random
from pathlib import Path

from fastapi.testclient import TestClient

from bench.lease_scenarios import KEY, T0, Origin, simulate
from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.config import LEASE_RETRY_MS, ORIGIN_DELAY_MS, XFETCH_BETA
from mini_redis.store import Store

OUT = Path(__file__).parent / "results"
SEED = 23


class Stats:
    def __init__(self):
        self.latencies = []  # 요청이 값을 받기까지 걸린 ms
        self.early = 0  # XFetch로 조기 갱신을 맡은 횟수
        self.misses = 0  # 404를 받은 횟수(기다림 시작)

    def record(self, ms):
        self.latencies.append(ms)


def client(cache, origin, clock, stats, ttl_s, interval_ms, end_ms):
    """interval_ms마다 인기 키를 한 번 읽는다. 위의 규약대로 lease를 쓴다."""
    while clock.now_ms < end_ms:
        started = clock.now_ms
        token = None  # 내가 원본을 불러 PUT해야 할 때만 있다
        waited = False  # 값을 못 받고 기다렸는가
        while True:
            r = cache.get(KEY, params={"lease": 1})
            body = r.json()
            if r.status_code == 200:
                if "lease" in body:  # XFetch 당첨: 값은 지금 쓰고, 갱신은 따로 한다
                    stats.early += 1
                    token = body["lease"]
                if not waited or token is None:
                    stats.record(clock.now_ms - started)
                break
            if not waited:
                stats.misses += 1
            waited = True
            if "lease" in body:
                token = body["lease"]
                break
            yield body["retry_after_ms"]
        if token is not None:
            value = origin.read()
            yield ORIGIN_DELAY_MS
            cache.put(KEY, json={"value": value, "ttl": ttl_s, "lease": token})
            if waited:  # miss를 받은 쪽은 갱신이 끝나야 값을 쓴다
                stats.record(clock.now_ms - started)
        yield interval_ms


def run(use_xfetch, clients, interval_ms, duration_s, ttl_s, seed=SEED):
    """clients개가 interval_ms마다 읽는 인기 키를 duration_s초 동안 돌린다."""
    clock = FakeClock(T0)
    beta = XFETCH_BETA if use_xfetch else 0.0  # beta=0이면 만료 전에는 절대 당첨되지 않아 lease만 쓴 것과 같다
    store = Store(clock=clock, random_fn=random.Random(seed).random, xfetch_beta=beta)
    cache = TestClient(create_app(store))
    sent = []
    cache.event_hooks["request"].append(sent.append)
    origin = Origin()

    # 워밍업: 처음 한 번을 lease로 채워 서버가 delta를 재게 한다 (두 방식 모두 같다)
    miss = cache.get(KEY, params={"lease": 1}).json()
    origin.read()
    clock.advance(ORIGIN_DELAY_MS)
    cache.put(KEY, json={"value": "v1", "ttl": ttl_s, "lease": miss["lease"]})
    origin.calls = 0
    sent.clear()

    stats = Stats()
    end_ms = clock.now_ms + duration_s * 1000
    # 클라이언트를 interval 안에 고르게 흩어 놓는다(같은 ms에 몰리지 않게)
    procs = [
        (i * interval_ms // clients, client(cache, origin, clock, stats, ttl_s, interval_ms, end_ms))
        for i in range(clients)
    ]
    simulate(clock, procs)
    lat = sorted(stats.latencies)
    pct = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))]
    return {
        "requests": len(lat),
        "waited": sum(1 for x in lat if x > 0),
        "p50": pct(0.50),
        "p99": pct(0.99),
        "max": lat[-1],
        "origin_calls": origin.calls,
        "early": stats.early,
        "cache_requests": len(sent),
    }


SWEEP_CLIENTS = (10, 20, 50, 100)  # interval 1000ms에서 초당 요청 수와 같다
SWEEP_SEEDS = (1, 2, 3, 4, 5)
SWEEP_DURATION_S = 60


def sweep(interval_ms, ttl_s):
    """요청률별로 lease만과 XFetch(시드 5개)의 기다린 요청 수를 잰다. XFetch의 보장은 확률적이라 한 시드로 말하지 않는다."""
    rows = []
    for clients in SWEEP_CLIENTS:
        only = run(False, clients, interval_ms, SWEEP_DURATION_S, ttl_s)["waited"]
        waits = [run(True, clients, interval_ms, SWEEP_DURATION_S, ttl_s, seed=s)["waited"] for s in SWEEP_SEEDS]
        rows.append((clients, clients * 1000 // interval_ms, only, waits))
    return rows


def to_markdown(args, results, sweep_rows=None):
    cycles = args.duration_s / args.ttl_s
    lines = [
        "# XFetch 조기 갱신 측정 결과",
        "",
        f"클라이언트 {args.clients}개가 각각 {args.interval_ms}ms마다 인기 키 하나를 읽는다(초당 약 {args.clients * 1000 // args.interval_ms}건).",
        f"캐시 TTL {args.ttl_s}초, 원본 지연 {ORIGIN_DELAY_MS}ms, lease 대기 간격 {LEASE_RETRY_MS}ms, XFetch beta {XFETCH_BETA}, {args.duration_s}초({cycles:g}주기).",
        "가짜 시계 위 이벤트 시뮬레이션이라 시드(23)가 같으면 매번 같은 숫자가 나온다. 캐시 요청은 0ms로 친다.",
        "`python -m bench.xfetch`가 이 파일을 다시 만든다.",
        "",
        "| 방식 | 요청 | 기다린 요청 | 기다린 비율 | p99(ms) | 최대(ms) | 원본 호출 | 조기 갱신 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, r in (("lease만", results[False]), ("lease + XFetch", results[True])):
        lines.append(
            f"| {label} | {r['requests']:,} | {r['waited']:,} | {r['waited'] / r['requests']:.2%} | "
            f"{r['p99']} | {r['max']} | {r['origin_calls']:,} | {r['early']:,} |"
        )
    lines += [
        "",
        "기다린 요청은 값을 받기까지 0ms보다 오래 걸린 요청이다. 원본 호출은 두 방식 모두 TTL 주기마다 한 번이라",
        "거의 같다. XFetch가 바꾸는 것은 호출 수가 아니라 그 호출이 일어나는 동안 기다리는 사용자다.",
        "조기 갱신은 만료 직전에 값을 이미 가진 요청 하나가 미리 원본을 부른 횟수다.",
    ]
    if sweep_rows:
        lines += [
            "",
            "## 요청률별: XFetch는 확률적이라 요청이 드물면 놓친다",
            "",
            f"같은 설정에서 {SWEEP_DURATION_S}초({SWEEP_DURATION_S / args.ttl_s:g}주기), 클라이언트 수만 바꾼다. XFetch는 시드 {len(SWEEP_SEEDS)}개로 돌린 값이다.",
            "만료 전에 추첨이 한 번도 당첨되지 않으면 그 주기는 평범한 miss가 된다. 당첨 확률은 `exp(-남은시간/delta)`이고",
            "요청이 드물면 만료 직전 delta 안에 들어오는 요청 수 자체가 적다.",
            "",
            "| 클라이언트 (초당 요청) | lease만: 기다린 요청 | XFetch: 평균 | XFetch: 최악 시드 | XFetch가 한 명도 안 기다린 실행 |",
            "| ---: | ---: | ---: | ---: | ---: |",
        ]
        for clients, rate, only, waits in sweep_rows:
            lines.append(
                f"| {clients} ({rate}) | {only:,} | {sum(waits) / len(waits):.1f} | {max(waits):,} | "
                f"{sum(1 for w in waits if w == 0)} / {len(waits)} |"
            )
    lines += [
        "",
        "## 한계",
        "",
        "- 단일 키, 일정한 요청 속도의 이상적인 부하다. 실제 트래픽은 들쭉날쭉하다.",
        "- XFetch는 확률적이라 \"기다린 요청 0\"을 보장하지 않는다. 위 요청률별 표처럼 요청이 드물면 놓치는 주기가 있다.",
        "- 요청 처리 시간을 0ms로 친다. Mini Redis가 느려지는 효과(락 대기)는 들어 있지 않다.",
        f"- delta는 서버가 lease 발급부터 PUT까지로 잰 값이라 이 벤치에서는 항상 {ORIGIN_DELAY_MS}ms다. 원본 지연이 들쭉날쭉하면 확률이 달라진다.",
    ]
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser(description="lease만과 lease + XFetch의 기다린 요청 수 비교")
    p.add_argument("--clients", type=int, default=100)
    p.add_argument("--interval-ms", type=int, default=1000)
    p.add_argument("--duration-s", type=int, default=120)
    p.add_argument("--ttl-s", type=int, default=10)
    p.add_argument("--no-sweep", action="store_true", help="요청률별 비교(느림, 약 1분)를 건너뛴다")
    args = p.parse_args()

    results = {
        use: run(use, args.clients, args.interval_ms, args.duration_s, args.ttl_s) for use in (False, True)
    }
    OUT.mkdir(exist_ok=True)
    rows = None if args.no_sweep else sweep(args.interval_ms, args.ttl_s)
    md = to_markdown(args, results, rows)
    (OUT / "xfetch.md").write_text(md, encoding="utf-8")
    print(md)


if __name__ == "__main__":
    main()
