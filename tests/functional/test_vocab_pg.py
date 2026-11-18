"""단어장 시연 서버를 진짜 PostgreSQL에 붙여 보는 테스트. DB가 없으면 건너뛰어요(bench/README.md로 띄울 수 있어요)."""

import os

import pytest
from fastapi.testclient import TestClient

from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.store import Store
from sample_app.vocab_app import PgWords, create_vocab_app

DSN = os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench")


@pytest.fixture
def pg_words():
    try:
        return PgWords(DSN, pool_size=2)
    except Exception as exc:  # 연결 실패, psycopg 없음 등
        pytest.skip(f"PostgreSQL을 쓸 수 없어요: {exc}")


@pytest.fixture
def vocab(pg_words):
    redis = TestClient(create_app(Store(clock=FakeClock(1_000_000))))
    return TestClient(create_vocab_app(redis, ttl_s=60, words=pg_words))


def test_vocab_pg_첫_진입_MISS_다시_진입_HIT_복습_횟수가_함께_온다(vocab):
    first = vocab.get("/api/words").json()
    second = vocab.get("/api/words").json()
    assert (first["cache"], second["cache"]) == ("MISS", "HIT")
    assert second["words"] == first["words"]
    assert all(w["reviews"] > 0 for w in first["words"])
    state = vocab.get("/api/state").json()
    assert (state["db"], state["db_reads"]) == ("postgres", 1)


def test_vocab_pg_추가_삭제_암기변경_뒤_캐시가_지워지고_최신_목록이_보인다(vocab):
    vocab.get("/api/words")
    added = vocab.post("/api/words", json={"word": "cache", "meaning": "캐시"}).json()["word"]
    assert vocab.get("/api/state").json()["keys"] == []
    after = vocab.get("/api/words").json()
    assert after["cache"] == "MISS" and "cache" in [w["word"] for w in after["words"]]

    assert vocab.patch(f"/api/words/{added['id']}", json={"memorized": True}).status_code == 200
    assert vocab.delete(f"/api/words/{added['id']}").status_code == 200
    assert "cache" not in [w["word"] for w in vocab.get("/api/words").json()["words"]]


def test_vocab_pg_없는_단어_삭제_404(vocab):
    assert vocab.delete("/api/words/9999").status_code == 404


def test_vocab_pg_초기화_처음_5개_단어로_되돌아간다(vocab):
    vocab.post("/api/words", json={"word": "extra", "meaning": ""})
    assert vocab.post("/api/reset").status_code == 200
    assert len(vocab.get("/api/words").json()["words"]) == 5
