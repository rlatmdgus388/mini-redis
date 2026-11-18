"""진짜 PostgreSQL 앞에 Mini Redis를 cache-aside로 붙인 샘플 앱이에요.

sleep으로 지연을 흉내 내지 않아요. 쿼리가 실제로 걸리는 시간이 그대로 응답 시간이 돼요.
  - 캐시 켬:  Mini Redis에 먼저 묻고(HIT), 없으면 DB에서 읽어 TTL을 걸어 채워 둬요(MISS).
  - 캐시 끔:  매 요청이 DB까지 가요(X-Cache: BYPASS).
쿼리는 둘 중 하나예요(환경 변수 PG_QUERY).
  - light: 기본키로 사용자 한 명 읽기 (가벼운 쿼리)
  - heavy: 주문 200만 건에서 사용자 한 명의 카테고리별 합계 (인덱스 없는 집계 쿼리)
"""

import json
import logging
import os
import queue
import threading
from contextlib import contextmanager
from decimal import Decimal
from typing import Callable, Optional
from urllib.parse import quote

import httpx
import psycopg
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from mini_redis import config

logger = logging.getLogger(__name__)

DEFAULT_DSN = "postgresql://postgres:bench@127.0.0.1:5433/bench"

QUERIES = {
    "light": "SELECT id, name, email FROM users WHERE id = %s",
    "heavy": (
        "SELECT category, count(*) AS orders, sum(amount) AS total "
        "FROM orders WHERE user_id = %s GROUP BY category ORDER BY total DESC"
    ),
}


def _plain(value):
    return float(value) if isinstance(value, Decimal) else value


def make_db_reader(dsn: str, query: str, pool_size: int) -> Callable[[int], list]:
    """연결 pool_size개를 미리 열어 두고, 아이템 번호로 쿼리를 실행하는 함수를 돌려줘요."""
    pool: "queue.Queue[psycopg.Connection]" = queue.Queue()
    for _ in range(pool_size):
        pool.put(psycopg.connect(dsn, autocommit=True))

    @contextmanager
    def borrow():
        conn = pool.get()
        try:
            yield conn
        finally:
            pool.put(conn)

    def read(item_id: int) -> list:
        with borrow() as conn:
            cur = conn.execute(query, (item_id,))
            names = [d.name for d in cur.description]
            return [{n: _plain(v) for n, v in zip(names, row)} for row in cur.fetchall()]

    return read


def create_pg_app(
    cache: Optional[httpx.Client],
    read_db: Callable[[int], list],
    ttl_s: int = config.SAMPLE_CACHE_TTL_S,
    use_cache: bool = True,
) -> FastAPI:
    """cache 클라이언트와 DB 읽기 함수는 밖에서 끼워 넣어요(테스트는 가짜 read_db를 써요)."""
    app = FastAPI()
    lock = threading.Lock()
    counters = {"hits": 0, "misses": 0, "db_calls": 0}

    def error(message: str, code: str, status: int = 502) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": code, "message": message})

    def from_db(item_id: int, label: str, cache_path: Optional[str] = None):
        try:
            rows = read_db(item_id)
        except psycopg.Error as exc:
            return error(f"db query failed: {exc}", "DB_ERROR")
        with lock:
            counters["db_calls"] += 1
            counters["misses"] += 1
        body = {"id": item_id, "rows": rows}
        if cache_path is not None:
            try:
                stored = cache.put(cache_path, json={"value": json.dumps(body), "ttl": ttl_s})
                if stored.status_code != 200:
                    logger.warning("cache put returned %s for %s", stored.status_code, cache_path)
            except httpx.HTTPError as exc:
                logger.warning("cache put failed for %s: %s", cache_path, exc)
        return JSONResponse(content=body, headers={"X-Cache": label})

    @app.get("/items/{item_id}")
    def get_item(item_id: int):
        if not use_cache:
            return from_db(item_id, "BYPASS")
        path = "/keys/" + quote(f"item:{item_id}", safe="")
        try:
            cached = cache.get(path)
        except httpx.HTTPError as exc:
            return error(f"cache request failed: {exc}", "CACHE_ERROR")
        if cached.status_code == 200:
            try:
                body = json.loads(cached.json()["value"])
            except (ValueError, KeyError, TypeError):
                return error("cache returned an invalid value", "CACHE_ERROR")
            with lock:
                counters["hits"] += 1
            return JSONResponse(content=body, headers={"X-Cache": "HIT"})
        if cached.status_code != 404:
            return error(f"cache returned {cached.status_code}", "CACHE_ERROR")
        return from_db(item_id, "MISS", path)

    @app.get("/stats")
    def stats() -> dict:
        with lock:
            return dict(counters)

    @app.post("/stats/reset")
    def reset() -> dict:
        with lock:
            for key in counters:
                counters[key] = 0
            return dict(counters)

    return app


def _build() -> FastAPI:
    use_cache = os.environ.get("SAMPLE_USE_CACHE", "1") != "0"
    query = QUERIES[os.environ.get("PG_QUERY", "light")]
    reader = make_db_reader(os.environ.get("PG_DSN", DEFAULT_DSN), query, int(os.environ.get("PG_POOL", "16")))
    cache = httpx.Client(base_url=os.environ.get("MINI_REDIS_URL", "http://127.0.0.1:8000")) if use_cache else None
    return create_pg_app(cache, reader, use_cache=use_cache)


# 가져오기만 해서는 DB에 연결하지 않는다. 서버로 띄울 때만 PG_APP_BUILD=1로 켠다(벤치마크가 넣어 준다).
app = _build() if os.environ.get("PG_APP_BUILD") == "1" else None
