"""실제 DB 샘플 앱(sample_app.pg_app) 테스트. DB 읽기는 가짜 함수로 끼워 넣어서 PostgreSQL 없이 돌아요."""

import json
from urllib.parse import quote

import psycopg
import pytest
from fastapi.testclient import TestClient

from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.store import Store
from sample_app.pg_app import create_pg_app


@pytest.fixture
def cache():
    return TestClient(create_app(Store(clock=FakeClock(1_000_000))))


@pytest.fixture
def db_calls():
    return []


@pytest.fixture
def read_db(db_calls):
    def read(item_id):
        db_calls.append(item_id)
        return [{"id": item_id, "name": f"user{item_id}"}]

    return read


def test_pg_app_첫_요청_MISS이고_DB_1번_호출(cache, read_db, db_calls):
    app = TestClient(create_pg_app(cache, read_db))
    response = app.get("/items/7")
    assert response.status_code == 200
    assert response.headers["X-Cache"] == "MISS"
    assert response.json() == {"id": 7, "rows": [{"id": 7, "name": "user7"}]}
    assert db_calls == [7]


def test_pg_app_두번째_요청_HIT이고_DB_호출_그대로(cache, read_db, db_calls):
    app = TestClient(create_pg_app(cache, read_db))
    first = app.get("/items/7")
    second = app.get("/items/7")
    assert second.headers["X-Cache"] == "HIT"
    assert second.json() == first.json()
    assert db_calls == [7]


def test_pg_app_캐시에_JSON_문자열과_ttl_60으로_저장(cache, read_db):
    app = TestClient(create_pg_app(cache, read_db, ttl_s=60))
    app.get("/items/7")
    key = quote("item:7", safe="")
    stored = cache.get(f"/keys/{key}").json()
    assert json.loads(stored["value"]) == {"id": 7, "rows": [{"id": 7, "name": "user7"}]}
    assert cache.get(f"/keys/{key}/ttl").json()["ttl"] == 60


def test_pg_app_캐시_끄면_BYPASS이고_캐시를_건드리지_않음(cache, read_db, db_calls):
    app = TestClient(create_pg_app(None, read_db, use_cache=False))
    for _ in range(3):
        assert app.get("/items/7").headers["X-Cache"] == "BYPASS"
    assert db_calls == [7, 7, 7]
    assert app.get("/stats").json() == {"hits": 0, "misses": 3, "db_calls": 3}


def test_pg_app_DB_오류_502_DB_ERROR_캐시에_안_채움(cache):
    def broken(item_id):
        raise psycopg.OperationalError("down")

    app = TestClient(create_pg_app(cache, broken))
    response = app.get("/items/7")
    assert response.status_code == 502
    assert response.json()["error"] == "DB_ERROR"
    assert cache.get("/keys/" + quote("item:7", safe="")).status_code == 404


def test_pg_app_stats_reset_집계를_0으로(cache, read_db):
    app = TestClient(create_pg_app(cache, read_db))
    app.get("/items/1")
    app.get("/items/1")
    assert app.get("/stats").json() == {"hits": 1, "misses": 1, "db_calls": 1}
    assert app.post("/stats/reset").json() == {"hits": 0, "misses": 0, "db_calls": 0}
