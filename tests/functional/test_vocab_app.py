"""단어장 시연 서버(sample_app.vocab_app) 테스트. 소켓도 실제 sleep도 쓰지 않아요."""

import httpx
import pytest
from fastapi.testclient import TestClient

from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.store import Store
from sample_app.vocab_app import create_vocab_app


@pytest.fixture
def clock():
    return FakeClock(1_000_000)


@pytest.fixture
def redis(clock):
    return TestClient(create_app(Store(clock=clock)))


@pytest.fixture
def slept():
    return []


@pytest.fixture
def vocab(redis, slept):
    return TestClient(create_vocab_app(redis, ttl_s=60, delay_ms=200, sleep=slept.append))


def words_of(response):
    return [w["word"] for w in response.json()["words"]]


def test_vocab_app_첫_진입_MISS_다시_진입_HIT_DB는_1번만_읽는다(vocab, slept):
    first = vocab.get("/api/words").json()
    second = vocab.get("/api/words").json()
    assert (first["cache"], second["cache"]) == ("MISS", "HIT")
    assert second["words"] == first["words"]
    assert slept == [0.2]
    assert vocab.get("/api/state").json()["db_reads"] == 1


def test_vocab_app_상태_조회_집계와_키_TTL이_맞다(vocab):
    vocab.get("/api/words")
    vocab.get("/api/words")
    state = vocab.get("/api/state").json()
    assert (state["hits"], state["misses"], state["db_reads"]) == (1, 1, 1)
    assert state["keys"] == [{"key": "vocab:demo", "ttl": 60}]


def test_vocab_app_단어_추가_캐시가_지워지고_다음_진입에_새_단어가_보인다(vocab):
    vocab.get("/api/words")
    added = vocab.post("/api/words", json={"word": "cache", "meaning": "캐시"})
    assert added.status_code == 200 and added.json()["invalidated"] is True
    assert vocab.get("/api/state").json()["keys"] == []
    after = vocab.get("/api/words")
    assert after.json()["cache"] == "MISS"
    assert "cache" in words_of(after)


def test_vocab_app_단어_삭제_캐시가_지워지고_다음_진입에_사라진다(vocab):
    vocab.get("/api/words")
    assert vocab.delete("/api/words/1").json()["deleted"] is True
    after = vocab.get("/api/words")
    assert after.json()["cache"] == "MISS"
    assert "management" not in words_of(after)


def test_vocab_app_암기_상태_변경_캐시가_지워지고_다음_진입에_반영된다(vocab):
    vocab.get("/api/words")
    changed = vocab.patch("/api/words/4", json={"memorized": True})
    assert changed.status_code == 200 and changed.json()["word"]["memorized"] is True
    assert vocab.get("/api/state").json()["keys"] == []
    after = vocab.get("/api/words").json()
    assert after["cache"] == "MISS"
    assert all(w["memorized"] for w in after["words"])


def test_vocab_app_없는_단어_암기_상태_변경_404_WORD_NOT_FOUND(vocab):
    response = vocab.patch("/api/words/999", json={"memorized": True})
    assert response.status_code == 404 and response.json()["error"] == "WORD_NOT_FOUND"


def test_vocab_app_새_단어_추가_암기_미완료로_시작한다(vocab):
    added = vocab.post("/api/words", json={"word": "cache", "meaning": "[명사] 캐시"}).json()
    assert added["word"]["memorized"] is False


def test_vocab_app_없는_단어_삭제_404_WORD_NOT_FOUND(vocab):
    response = vocab.delete("/api/words/999")
    assert response.status_code == 404
    assert response.json()["error"] == "WORD_NOT_FOUND"


def test_vocab_app_빈_단어_추가_422로_거절한다(vocab):
    assert vocab.post("/api/words", json={"word": "", "meaning": "x"}).status_code == 422


def test_vocab_app_캐시_끄면_매번_DB를_읽고_BYPASS_캐시를_건드리지_않는다(vocab, slept):
    vocab.post("/api/cache-mode", json={"enabled": False})
    results = [vocab.get("/api/words").json()["cache"] for _ in range(3)]
    assert results == ["BYPASS"] * 3
    assert slept == [0.2] * 3
    assert vocab.get("/api/state").json()["keys"] == []


def test_vocab_app_캐시_끈_채_추가해도_켜면_최신_목록이_나온다(vocab):
    vocab.get("/api/words")  # 캐시에 옛 목록이 들어간다
    vocab.post("/api/cache-mode", json={"enabled": False})
    vocab.post("/api/words", json={"word": "cache", "meaning": ""})
    vocab.post("/api/cache-mode", json={"enabled": True})
    assert "cache" in words_of(vocab.get("/api/words"))


def test_vocab_app_초기화_단어장_집계_캐시가_처음으로_돌아간다(vocab):
    vocab.get("/api/words")
    vocab.post("/api/words", json={"word": "cache", "meaning": ""})
    assert vocab.post("/api/reset").json() == {"ok": True}
    state = vocab.get("/api/state").json()
    assert (state["hits"], state["misses"], state["db_reads"], state["words"], state["keys"]) == (0, 0, 0, 5, [])


def test_vocab_app_캐시_서버가_죽으면_진입은_502_CACHE_ERROR(slept):
    def dead(request):
        raise httpx.ConnectError("down")

    client = httpx.Client(transport=httpx.MockTransport(dead), base_url="http://mock")
    app = TestClient(create_vocab_app(client, sleep=slept.append))
    response = app.get("/api/words")
    assert response.status_code == 502
    assert response.json()["error"] == "CACHE_ERROR"


def test_vocab_app_루트_요청_HTML_화면(vocab):
    response = vocab.get("/")
    assert response.status_code == 200 and "단어장" in response.text
