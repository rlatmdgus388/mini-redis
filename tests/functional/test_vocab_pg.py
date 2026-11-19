"""단어장 시연 서버를 진짜 PostgreSQL에 붙여 보는 테스트. DB가 없으면 건너뛰어요(bench/README.md로 띄울 수 있어요)."""

import os

import pytest
from fastapi.testclient import TestClient

from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.store import Store
from sample_app.vocab_app import PgWords, create_vocab_app

DSN = os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench")
STATS_KEY = "vocab:stats:demo"
_KST_DAY = "(studied_at AT TIME ZONE 'Asia/Seoul')::date"
_KST_TODAY = "(now() AT TIME ZONE 'Asia/Seoul')::date"


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


def test_vocab_pg_통계_첫_조회_MISS_다시_조회_HIT_처음_숫자가_맞다(vocab):
    first = vocab.get("/api/stats").json()
    second = vocab.get("/api/stats").json()
    assert (first["cache"], second["cache"]) == ("MISS", "HIT")
    assert second["stats"] == first["stats"] == {"words_today": 0, "seconds_today": 0, "streak_days": 4}
    state = vocab.get("/api/state").json()
    assert (state["db"], state["db_reads"]) == ("postgres", 1)


def test_vocab_pg_학습_기록_뒤_캐시가_지워지고_다음_조회에_숫자가_바뀐다(vocab):
    vocab.get("/api/stats")
    assert vocab.post("/api/study", json={"word_id": 1, "seconds": 10}).json()["invalidated"] is True
    assert vocab.get("/api/state").json()["keys"] == []
    vocab.post("/api/study", json={"word_id": 1, "seconds": 5})
    vocab.post("/api/study", json={"word_id": 2, "seconds": 3})
    after = vocab.get("/api/stats").json()
    assert after["cache"] == "MISS"
    assert after["stats"] == {"words_today": 2, "seconds_today": 18, "streak_days": 5}


def test_vocab_pg_연속_학습일_빠진_날이_있으면_거기서_끊긴다(vocab, pg_words):
    with pg_words._conn() as c:  # 3일 전을 지우면 어제, 그저께 2일만 이어진다
        c.execute(f"DELETE FROM vocab_study_events WHERE {_KST_DAY} = {_KST_TODAY} - 3")
    assert vocab.get("/api/stats").json()["stats"]["streak_days"] == 2


def test_vocab_pg_어제도_안_했으면_연속_학습일은_0이다(vocab, pg_words):
    with pg_words._conn() as c:
        c.execute(f"DELETE FROM vocab_study_events WHERE {_KST_DAY} >= {_KST_TODAY} - 1")
    assert vocab.get("/api/stats").json()["stats"]["streak_days"] == 0


def test_vocab_pg_추가_삭제_암기변경은_통계_캐시를_지우지_않고_목록에_바로_보인다(vocab):
    vocab.get("/api/stats")
    added = vocab.post("/api/words", json={"word": "cache", "meaning": "캐시"}).json()["word"]
    assert "cache" in [w["word"] for w in vocab.get("/api/words").json()["words"]]
    assert vocab.patch(f"/api/words/{added['id']}", json={"memorized": True}).status_code == 200
    assert vocab.delete(f"/api/words/{added['id']}").status_code == 200
    assert "cache" not in [w["word"] for w in vocab.get("/api/words").json()["words"]]
    assert vocab.get("/api/state").json()["keys"] == [{"key": STATS_KEY, "ttl": 60}]


def test_vocab_pg_없는_단어_삭제_404(vocab):
    assert vocab.delete("/api/words/9999").status_code == 404


def test_vocab_pg_없는_단어_학습_404_기록이_남지_않는다(vocab):
    assert vocab.post("/api/study", json={"word_id": 9999, "seconds": 3}).status_code == 404
    assert vocab.get("/api/stats").json()["stats"]["words_today"] == 0


def test_vocab_pg_초기화_처음_5개_단어와_학습_기록으로_되돌아간다(vocab):
    vocab.post("/api/words", json={"word": "extra", "meaning": ""})
    vocab.post("/api/study", json={"word_id": 1, "seconds": 9})
    assert vocab.post("/api/reset").status_code == 200
    assert len(vocab.get("/api/words").json()["words"]) == 5
    assert vocab.get("/api/stats").json()["stats"] == {"words_today": 0, "seconds_today": 0, "streak_days": 4}
