"""캐시 효과 측정 (#18): 같은 샘플 앱을 캐시 끈 채로, 켠 채로 돌려 요청 지연을 비교한다.

캐시를 끄면 모든 요청이 느린 가짜 원본(200ms 지연)까지 가고, 켜면 첫 요청만 원본까지 가고 이후엔 Mini Redis에서 답한다.
이 차이가 Mini Redis를 캐시로 쓰는 이유라서, 진짜 HTTP 프로세스로 재현해 숫자를 남긴다.

모드마다 새 프로세스 세 개를 띄운다(Mini Redis는 빈 채로 시작한다).
  - Mini Redis:  python -m uvicorn mini_redis.api.app:app   (cwd = 새 임시 폴더, 영속 파일이 레포에 안 생기게)
  - 가짜 원본:    python -m uvicorn sample_app.origin:app
  - 샘플 앱:      python -m uvicorn sample_app.app:app      (SAMPLE_USE_CACHE=1 또는 0)
단계는 둘이다.
  - 첫 요청: 아이템 id를 한 번씩, 섞어서 요청한다(캐시가 비어 있을 때).
  - 이후 요청: 같은 아이템에서 --requests개를 고르게 뽑는다(캐시를 켰다면 이미 채워진 뒤).
각 단계에서 요청마다 perf_counter_ns로 지연을 재고, 비-200 응답은 에러로 세어 따로 적는다.

레포 루트에서:  python -m bench.cache_effect [--items 100] [--requests 2000] [--concurrency 16] [--repeat 1]
"""
import argparse
import math
import os
import platform
import queue
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import httpx

from mini_redis import config

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(__file__).parent / "results" / "cache_effect.md"
SEED = 18
READY_DEADLINE_S = 30.0
MODES = (("끔", False), ("켬", True))
PHASES = (("cold", "첫 요청"), ("warm", "이후 요청"))


def percentile(values, pct):
    """최근접 순위(nearest-rank): 정렬했을 때 ceil(pct/100 * n)번째 값(최소 1번째). 빈 목록은 ValueError."""
    if not values:
        raise ValueError("빈 목록의 백분위수는 없어요")
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def summarize(latencies_ms, errors, hits, misses, wall_s):
    """한 단계의 지연 목록(성공한 요청만)과 카운터를 표 한 줄 분량의 통계로 줄인다."""
    total = hits + misses
    return {
        "count": len(latencies_ms),
        "errors": errors,
        "p50": percentile(latencies_ms, 50) if latencies_ms else 0.0,
        "p95": percentile(latencies_ms, 95) if latencies_ms else 0.0,
        "max": max(latencies_ms) if latencies_ms else 0.0,
        "throughput": len(latencies_ms) / wall_s if wall_s > 0 else 0.0,
        "hit_ratio": hits / total if total else 0.0,
    }


def phase_wall_seconds(started, completions):
    """단계의 벽시계 시간. 출발 직전 시각부터 워커들의 마지막 요청 완료 시각 중 가장 늦은 것까지다.
    클라이언트를 닫고 스레드를 join하는 시간은 넣지 않는다."""
    return max(completions) - started


def server_command(target, port):
    """uvicorn 실행 명령. --workers 1로 서버마다 프로세스를 하나로 고정한다(여러 개면 캐시 hit 집계와 처리량이 달라진다)."""
    return [
        sys.executable, "-m", "uvicorn", target,
        "--port", str(port), "--workers", "1", "--log-level", "warning",
    ]


def server_env(base_env, extra):
    """자식 프로세스 환경. 환경 변수 WEB_CONCURRENCY가 --workers를 덮어쓰지 못하게 빼고 extra를 얹는다(base_env는 안 바꾼다)."""
    env = {k: v for k, v in base_env.items() if k != "WEB_CONCURRENCY"}
    env.update(extra)
    return env


def free_port():
    """0번 포트에 bind해 운영체제가 고른 빈 포트를 읽고 닫는다."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _tail(path, n=2000):
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")[-n:]
    except OSError:
        return ""


def _wait_ready(url, proc, log_path, name):
    deadline = time.monotonic() + READY_DEADLINE_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{name}이(가) 먼저 끝났어요(코드 {proc.returncode}):\n{_tail(log_path)}")
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"{name}이(가) {READY_DEADLINE_S:.0f}초 안에 준비되지 않았어요:\n{_tail(log_path)}")


@contextmanager
def servers(use_cache):
    """세 프로세스를 띄우고 준비될 때까지 기다린 뒤 (샘플 앱 URL, 원본 URL)을 준다. 나갈 때 항상 정리한다."""
    ports = [free_port() for _ in range(3)]
    while len(set(ports)) < 3:  # 닫은 포트를 다시 받는 일은 드물지만, 겹치면 다시 고른다
        ports = [free_port() for _ in range(3)]
    redis_port, origin_port, sample_port = ports
    redis_url = f"http://127.0.0.1:{redis_port}"
    origin_url = f"http://127.0.0.1:{origin_port}"
    sample_url = f"http://127.0.0.1:{sample_port}"
    base_env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONIOENCODING": "utf-8"}
    specs = [
        ("Mini Redis", "mini_redis.api.app:app", redis_port, {}),
        ("가짜 원본", "sample_app.origin:app", origin_port, {}),
        (
            "샘플 앱",
            "sample_app.app:app",
            sample_port,
            {
                "MINI_REDIS_URL": redis_url,
                "ORIGIN_URL": origin_url,
                "SAMPLE_USE_CACHE": "1" if use_cache else "0",
            },
        ),
    ]
    procs = []
    with tempfile.TemporaryDirectory(prefix="cache_effect_", ignore_cleanup_errors=True) as tmp:
        tmp = Path(tmp)
        redis_cwd = tmp / "redis_cwd"  # 영속 파일(data/)이 여기에 생긴다. 레포에는 안 생긴다.
        redis_cwd.mkdir()
        try:
            for name, target, port, extra in specs:
                log_path = tmp / f"{port}.log"
                log = open(log_path, "wb")
                proc = subprocess.Popen(
                    server_command(target, port),
                    cwd=str(redis_cwd),  # 세 프로세스 모두 임시 폴더에서 시작한다. 코드는 PYTHONPATH로 찾는다.
                    env=server_env(base_env, extra),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                procs.append((name, proc, log, log_path))
            _wait_ready(f"{redis_url}/health", procs[0][1], procs[0][3], procs[0][0])
            _wait_ready(f"{origin_url}/stats", procs[1][1], procs[1][3], procs[1][0])
            _wait_ready(f"{sample_url}/stats", procs[2][1], procs[2][3], procs[2][0])
            yield sample_url, origin_url
        finally:
            for _, proc, _, _ in procs:
                if proc.poll() is None:
                    proc.terminate()
            for _, proc, log, _ in procs:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                log.close()


def run_phase(sample_url, origin_url, ids, concurrency, calls_key="calls"):
    """ids를 스레드 concurrency개가 나눠 요청하고 그 단계의 통계와 부가 정보를 돌려준다."""
    httpx.post(f"{sample_url}/stats/reset", timeout=5).raise_for_status()
    httpx.post(f"{origin_url}/stats/reset", timeout=5).raise_for_status()
    work = queue.Queue()
    for item_id in ids:
        work.put(item_id)
    workers = max(1, min(concurrency, len(ids)))
    barrier = threading.Barrier(workers + 1)
    results = [([], 0, Counter()) for _ in range(workers)]  # 스레드마다 (지연 ms, 에러 수, X-Cache 집계)
    last_done = [None] * workers  # 스레드마다 마지막 요청이 끝난 perf_counter 시각

    def worker(slot):
        latencies, tally, errors = [], Counter(), 0
        try:
            with httpx.Client(base_url=sample_url, timeout=30.0) as client:  # keep-alive
                barrier.wait()
                while True:
                    try:
                        item_id = work.get_nowait()
                    except queue.Empty:
                        break
                    start = time.perf_counter_ns()
                    try:
                        response = client.get(f"/items/{item_id}")
                        status = response.status_code
                    except httpx.HTTPError:
                        status = None
                    elapsed_ms = (time.perf_counter_ns() - start) / 1e6
                    last_done[slot] = time.perf_counter()
                    if status == 200:
                        latencies.append(elapsed_ms)
                        tally[response.headers.get("X-Cache", "-")] += 1
                    else:
                        errors += 1
        finally:
            results[slot] = (latencies, errors, tally)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    started = time.perf_counter()  # 배리어를 풀기 직전에 잡는다(풀린 뒤에 잡으면 첫 요청 일부가 빠진다)
    barrier.wait()
    for t in threads:
        t.join()
    completions = [t for t in last_done if t is not None] or [started]
    wall_s = phase_wall_seconds(started, completions)  # 마지막 요청이 끝난 시각까지만, join/정리 시간은 뺀다

    latencies = [x for lat, _, _ in results for x in lat]
    errors = sum(e for _, e, _ in results)
    tally = sum((c for _, _, c in results), Counter())
    stats = httpx.get(f"{sample_url}/stats", timeout=5).json()
    origin_calls = httpx.get(f"{origin_url}/stats", timeout=5).json()[calls_key]
    summary = summarize(latencies, errors, stats["hits"], stats["misses"], wall_s)
    summary.update(
        wall_s=wall_s, xcache=dict(tally), origin_calls=origin_calls,
        started=started, ended=max(completions),
    )
    return summary


def run_mode(use_cache, items, requests, concurrency):
    """모드 하나(프로세스 새로 띄움)를 돌려 {"cold": 통계, "warm": 통계}를 돌려준다."""
    rng = random.Random(SEED)
    item_ids = [str(i) for i in range(items)]
    cold_ids = item_ids[:]
    rng.shuffle(cold_ids)
    warm_ids = [rng.choice(item_ids) for _ in range(requests)]
    with servers(use_cache) as (sample_url, origin_url):
        return {
            "cold": run_phase(sample_url, origin_url, cold_ids, concurrency),
            "warm": run_phase(sample_url, origin_url, warm_ids, concurrency),
        }


def ttl_interval(result):
    """캐시 켬 한 번의 TTL 구간: 첫 요청 단계 출발 직전부터 이후 요청 단계의 마지막 완료까지 이어진 하나의 구간이다.
    그 사이의 리셋, 통계 읽기, 큐와 클라이언트 준비 시간이 모두 들어 있다. (시작, 끝, 길이) 초를 돌려준다."""
    begin = result["cold"]["started"]
    end = result["warm"]["ended"]
    return begin, end, end - begin


def to_markdown(args, runs, warnings):
    lines = [
        "# 캐시 효과 측정 결과",
        "",
        "샘플 앱을 캐시 끈 채로, 켠 채로 진짜 HTTP로 돌려 요청 지연을 비교했다. `python -m bench.cache_effect`가 이 파일을 다시 만든다.",
        "",
        f"- 컴퓨터: {platform.platform()}, {platform.processor()}, CPU {os.cpu_count()}개, Python {platform.python_version()}",
        f"- 아이템 {args.items}개, 이후 요청 {args.requests}개, 동시 요청 {args.concurrency}개, 반복 {args.repeat}회 (시드 {SEED})",
        f"- 가짜 원본 지연 {config.ORIGIN_DELAY_MS}ms, 샘플 캐시 TTL {config.SAMPLE_CACHE_TTL_S}초",
        "- 모드마다 프로세스 세 개를 새로 띄운다. 모든 반복의 행을 그대로 적는다(최솟값이나 평균을 내지 않는다).",
        "- 비-200 응답은 에러 열에 센다. 요청 열은 성공한 요청 수다.",
    ]
    for warning in warnings:
        lines.append(f"- 경고: {warning}")
    header = (
        "| 모드 | 반복 | 요청 | 에러 | p50(ms) | p95(ms) | 최대(ms) | 처리량(req/s) | hit ratio | 원본 호출 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    for key, title in PHASES:
        lines += ["", f"## {title}", "", *header]
        for label, repeat_no, result in runs:
            r = result[key]
            lines.append(
                f"| {label} | {repeat_no} | {r['count']} | {r['errors']} | {r['p50']:.1f} | {r['p95']:.1f} | "
                f"{r['max']:.1f} | {r['throughput']:.1f} | {r['hit_ratio']:.3f} | {r['origin_calls']} |"
            )
    lines += ["", "캐시 켬 실행별 만료 점검 구간(첫 요청 출발 직전부터 이후 요청 마지막 완료까지, 시작을 0으로 둔 초):", ""]
    for label, repeat_no, result in runs:
        if label == "켬":
            begin, end, length = ttl_interval(result)
            lines.append(f"- {label} {repeat_no}회차 TTL 구간: 0.0 ~ {length:.1f}초 (길이 {length:.1f}초)")
    lines += ["", "X-Cache 헤더 집계(단계별):", ""]
    for label, repeat_no, result in runs:
        for key, title in PHASES:
            lines.append(f"- {label} {repeat_no}회차 {title}: {result[key]['xcache']}")
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description="캐시 효과 측정: 샘플 앱 캐시 끔 vs 켬")
    p.add_argument("--items", type=int, default=100)
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--out", type=Path, default=OUT)
    args = p.parse_args(argv)

    runs, warnings = [], []
    for repeat_no in range(1, args.repeat + 1):
        for label, use_cache in MODES:  # 모드를 번갈아 돌려 다른 프로세스의 간섭이 한쪽에 몰리지 않게 한다
            result = run_mode(use_cache, args.items, args.requests, args.concurrency)
            runs.append((label, repeat_no, result))
            if use_cache:
                total_s = ttl_interval(result)[2]  # 두 단계 사이의 준비 시간까지 포함한 이어진 구간
                if total_s > config.SAMPLE_CACHE_TTL_S - 5:
                    msg = (
                        f"캐시 켬 {repeat_no}회차가 TTL({config.SAMPLE_CACHE_TTL_S}초)에 가까울 만큼 길어서 "
                        "캐시가 만료돼 hit가 miss로 바뀌었을 수 있어요"
                    )
                    warnings.append(msg)
                    print("경고:", msg, file=sys.stderr)
    md = to_markdown(args, runs, warnings)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(f"결과를 {args.out}에 썼어요.")


if __name__ == "__main__":
    main()
