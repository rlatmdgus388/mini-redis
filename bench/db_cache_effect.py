"""실제 PostgreSQL 앞의 캐시 효과 측정: 같은 앱을 캐시 끈 채로(DB 직접), 켠 채로(Mini Redis 먼저) 돌려 비교한다.

`bench/cache_effect.py`는 sleep으로 200ms를 흉내 낸 가짜 원본을 썼다. 여기서는 지연을 만들지 않고
진짜 DB 쿼리가 걸리는 시간을 그대로 잰다. 쿼리 무게별로 따로 잰다.
  - light: 기본키로 사용자 한 명 읽기
  - heavy: 주문 200만 건에서 사용자 한 명의 카테고리별 합계(인덱스 없는 집계)

준비:  docker로 PostgreSQL을 5433 포트에 띄우고 `python -m bench.pg_seed`로 데이터를 만든다(bench/README.md).
모드마다 새 프로세스 두 개(Mini Redis, 샘플 앱)를 띄운다. DB는 이미 떠 있는 것을 쓴다.

레포 루트에서:  python -m bench.db_cache_effect [--items 100] [--requests 2000] [--concurrency 16] [--repeat 3]
"""
import argparse
import os
import platform
import random
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

import httpx
import psycopg

from bench import cache_effect as ce
from mini_redis import config

OUT = Path(__file__).parent / "results" / "db_cache_effect.md"
DSN = os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench")
QUERIES = (("light", "가벼운 쿼리 (기본키로 한 건 읽기)"), ("heavy", "무거운 쿼리 (주문 200만 건 집계)"))


@contextmanager
def servers(use_cache, query):
    """Mini Redis와 PG 샘플 앱을 띄우고 샘플 앱 URL을 준다. 나갈 때 항상 정리한다."""
    ports = [ce.free_port() for _ in range(2)]
    while len(set(ports)) < 2:
        ports = [ce.free_port() for _ in range(2)]
    redis_port, app_port = ports
    redis_url = f"http://127.0.0.1:{redis_port}"
    app_url = f"http://127.0.0.1:{app_port}"
    base_env = {**os.environ, "PYTHONPATH": str(ce.ROOT), "PYTHONIOENCODING": "utf-8"}
    specs = [
        ("Mini Redis", "mini_redis.api.app:app", redis_port, {}),
        (
            "PG 샘플 앱", "sample_app.pg_app:app", app_port,
            {"MINI_REDIS_URL": redis_url, "SAMPLE_USE_CACHE": "1" if use_cache else "0", "PG_QUERY": query, "PG_DSN": DSN, "PG_APP_BUILD": "1"},
        ),
    ]
    procs = []
    with tempfile.TemporaryDirectory(prefix="db_cache_effect_", ignore_cleanup_errors=True) as tmp:
        tmp = Path(tmp)
        cwd = tmp / "cwd"
        cwd.mkdir()
        try:
            for name, target, port, extra in specs:
                log_path = tmp / f"{port}.log"
                log = open(log_path, "wb")
                proc = subprocess.Popen(
                    ce.server_command(target, port), cwd=str(cwd), env=ce.server_env(base_env, extra),
                    stdout=log, stderr=subprocess.STDOUT,
                )
                procs.append((name, proc, log, log_path))
            ce._wait_ready(f"{redis_url}/health", procs[0][1], procs[0][3], procs[0][0])
            ce._wait_ready(f"{app_url}/stats", procs[1][1], procs[1][3], procs[1][0])
            yield app_url
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


def run_mode(use_cache, query, items, requests, concurrency):
    """모드 하나를 돌려 {"cold": 통계, "warm": 통계}를 돌려준다. 아이템 번호는 사용자 id(1부터)다."""
    rng = random.Random(ce.SEED)
    item_ids = [str(i) for i in range(1, items + 1)]
    cold_ids = item_ids[:]
    rng.shuffle(cold_ids)
    warm_ids = [rng.choice(item_ids) for _ in range(requests)]
    with servers(use_cache, query) as app_url:
        return {
            "cold": ce.run_phase(app_url, app_url, cold_ids, concurrency, calls_key="db_calls"),
            "warm": ce.run_phase(app_url, app_url, warm_ids, concurrency, calls_key="db_calls"),
        }


def db_version():
    with psycopg.connect(DSN) as conn:
        return conn.execute("SHOW server_version").fetchone()[0]


def to_markdown(args, runs, warnings, version):
    lines = [
        "# 실제 DB 캐시 효과 측정 결과",
        "",
        "같은 앱을 캐시 끈 채로(매 요청이 PostgreSQL까지), 켠 채로(Mini Redis 먼저) 진짜 HTTP로 돌려 요청 지연을 비교했다. "
        "지연을 흉내 내지 않고 쿼리가 실제로 걸린 시간을 그대로 쟀다. `python -m bench.db_cache_effect`가 이 파일을 다시 만든다.",
        "",
        f"- 컴퓨터: {platform.platform()}, {platform.processor()}, CPU {os.cpu_count()}개, Python {platform.python_version()}",
        f"- DB: PostgreSQL {version} (Docker 컨테이너, 같은 컴퓨터), users 1,000명 / orders 2,000,000건(user_id 인덱스 없음)",
        f"- 아이템(사용자) {args.items}개, 이후 요청 {args.requests}개, 동시 요청 {args.concurrency}개, 반복 {args.repeat}회 (시드 {ce.SEED})",
        f"- 캐시 TTL {config.SAMPLE_CACHE_TTL_S}초. 모드마다 프로세스 두 개(Mini Redis, 샘플 앱)를 새로 띄운다. 모든 반복의 행을 그대로 적는다.",
        "- 비-200 응답은 에러 열에 센다. 요청 열은 성공한 요청 수다. DB 호출은 샘플 앱이 DB까지 간 횟수다.",
    ]
    for warning in warnings:
        lines.append(f"- 경고: {warning}")
    header = (
        "| 모드 | 반복 | 요청 | 에러 | p50(ms) | p95(ms) | 최대(ms) | 처리량(req/s) | hit ratio | DB 호출 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    for qkey, qtitle in QUERIES:
        for key, title in ce.PHASES:
            lines += ["", f"## {qtitle} - {title}", "", *header]
            for q, label, repeat_no, result in runs:
                if q != qkey:
                    continue
                r = result[key]
                lines.append(
                    f"| {label} | {repeat_no} | {r['count']} | {r['errors']} | {r['p50']:.1f} | {r['p95']:.1f} | "
                    f"{r['max']:.1f} | {r['throughput']:.1f} | {r['hit_ratio']:.3f} | {r['origin_calls']} |"
                )
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description="실제 PostgreSQL 앞의 캐시 효과: 캐시 끔 vs 켬")
    p.add_argument("--items", type=int, default=100)
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--out", type=Path, default=OUT)
    args = p.parse_args(argv)
    try:
        version = db_version()
    except psycopg.Error as exc:
        sys.exit(f"PostgreSQL에 연결하지 못했어요({DSN}): {exc}\nbench/README.md의 준비 절차를 먼저 하세요.")

    runs, warnings = [], []
    for qkey, _ in QUERIES:
        for repeat_no in range(1, args.repeat + 1):
            for label, use_cache in ce.MODES:
                result = run_mode(use_cache, qkey, args.items, args.requests, args.concurrency)
                runs.append((qkey, label, repeat_no, result))
                print(f"{qkey} {label} {repeat_no}회차 끝: 이후 p50 {result['warm']['p50']:.1f}ms", file=sys.stderr)
                if use_cache and ce.ttl_interval(result)[2] > config.SAMPLE_CACHE_TTL_S - 5:
                    warnings.append(f"{qkey} 캐시 켬 {repeat_no}회차가 TTL에 가까울 만큼 길어서 hit가 miss로 바뀌었을 수 있어요")
    md = to_markdown(args, runs, warnings, version)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(md, encoding="utf-8")
    print(f"결과를 {args.out}에 썼어요.")


if __name__ == "__main__":
    main()
