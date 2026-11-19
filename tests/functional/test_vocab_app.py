"""단어장 시연 서버(sample_app.vocab_app) 테스트. 소켓도 실제 sleep도 쓰지 않아요."""

import httpx
import pytest
from fastapi.testclient import TestClient

from mini_redis.api.app import create_app
from mini_redis.clock import FakeClock
from mini_redis.store import Store
from sample_app.vocab_app import MemoryWords, RemoteDb, create_vocab_app

DAY_MS = 86_400_000
STATS_KEY = "vocab:stats:demo"


@pytest.fixture
def clock():
    return FakeClock(1_800_000_000_000)


@pytest.fixture
def redis(clock):
    return TestClient(create_app(Store(clock=clock)))


@pytest.fixture
def slept():
    return []


@pytest.fixture
def vocab(redis, slept, clock):
    return TestClient(create_vocab_app(redis, ttl_s=60, delay_ms=200, sleep=slept.append, now_ms=clock))


def words_of(response):
    return [w["word"] for w in response.json()["words"]]


def stats_of(vocab):
    return vocab.get("/api/stats").json()


def test_vocab_app_통계_첫_조회_MISS_다시_조회_HIT_DB는_1번만_읽는다(vocab, slept):
    first = stats_of(vocab)
    second = stats_of(vocab)
    assert (first["cache"], second["cache"]) == ("MISS", "HIT")
    assert second["stats"] == first["stats"]
    assert slept == [0.2]
    assert vocab.get("/api/state").json()["db_reads"] == 1


def test_vocab_app_상태_조회_집계와_키_TTL이_맞다(vocab):
    stats_of(vocab)
    stats_of(vocab)
    state = vocab.get("/api/state").json()
    assert (state["hits"], state["misses"], state["db_reads"]) == (1, 1, 1)
    assert state["keys"] == [{"key": STATS_KEY, "ttl": 60}]


def test_vocab_app_처음_통계_오늘은_0이고_어제까지_4일_연속이다(vocab):
    assert stats_of(vocab)["stats"] == {"words_today": 0, "seconds_today": 0, "streak_days": 4}


def test_vocab_app_학습_기록_통계_캐시가_지워지고_다음_조회에_숫자가_바뀐다(vocab):
    stats_of(vocab)
    studied = vocab.post("/api/study", json={"word_id": 1, "seconds": 10})
    assert studied.status_code == 200 and studied.json()["invalidated"] is True
    assert vocab.get("/api/state").json()["keys"] == []
    after = stats_of(vocab)
    assert after["cache"] == "MISS"
    assert after["stats"] == {"words_today": 1, "seconds_today": 10, "streak_days": 5}


def test_vocab_app_같은_단어를_또_학습하면_단어_수는_그대로_시간은_합쳐진다(vocab):
    vocab.post("/api/study", json={"word_id": 2, "seconds": 5})
    vocab.post("/api/study", json={"word_id": 2, "seconds": 7})
    vocab.post("/api/study", json={"word_id": 3, "seconds": 3})
    stats = stats_of(vocab)["stats"]
    assert (stats["words_today"], stats["seconds_today"]) == (2, 15)


def test_vocab_app_연속_학습일_하루_쉬면_이어지고_이틀_쉬면_끊긴다(vocab, clock):
    assert stats_of(vocab)["stats"]["streak_days"] == 4  # 오늘은 아직 안 했지만 어제까지 이어져 있다
    clock.advance(DAY_MS)  # 하루 지나 어제가 비었다: 그저께까지 4일이 아니라, 어제(=기존 오늘)가 비어 끊긴다
    assert stats_of(vocab)["stats"]["streak_days"] == 0
    vocab.post("/api/study", json={"word_id": 1, "seconds": 1})
    assert stats_of(vocab)["stats"]["streak_days"] == 1


def test_vocab_app_없는_단어_학습_404_WORD_NOT_FOUND(vocab):
    response = vocab.post("/api/study", json={"word_id": 999, "seconds": 3})
    assert response.status_code == 404 and response.json()["error"] == "WORD_NOT_FOUND"


def test_vocab_app_학습_시간이_음수면_422로_거절한다(vocab):
    assert vocab.post("/api/study", json={"word_id": 1, "seconds": -1}).status_code == 422


def test_vocab_app_목록_조회는_Mini_Redis를_거치지_않고_통계_키도_만들지_않는다(vocab, slept):
    first = vocab.get("/api/words").json()
    vocab.get("/api/words")
    assert [w["word"] for w in first["words"]][:2] == ["management", "located"]
    state = vocab.get("/api/state").json()
    assert state["keys"] == [] and state["list_reads"] == 2 and state["db_reads"] == 0
    assert slept == []  # 목록은 느린 DB 흉내가 없다


def test_vocab_app_단어_추가_삭제_암기변경은_통계_캐시를_지우지_않는다(vocab):
    stats_of(vocab)
    vocab.post("/api/words", json={"word": "cache", "meaning": "캐시"})
    vocab.delete("/api/words/1")
    vocab.patch("/api/words/4", json={"memorized": True})
    assert vocab.get("/api/state").json()["keys"] == [{"key": STATS_KEY, "ttl": 60}]
    assert stats_of(vocab)["cache"] == "HIT"


def test_vocab_app_단어_추가_삭제_암기변경_다음_목록에_바로_반영된다(vocab):
    vocab.post("/api/words", json={"word": "cache", "meaning": "캐시"})
    assert "cache" in words_of(vocab.get("/api/words"))
    assert vocab.delete("/api/words/1").json()["deleted"] is True
    assert "management" not in words_of(vocab.get("/api/words"))
    changed = vocab.patch("/api/words/4", json={"memorized": True})
    assert changed.status_code == 200 and changed.json()["word"]["memorized"] is True
    assert next(w for w in vocab.get("/api/words").json()["words"] if w["id"] == 4)["memorized"] is True


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


def test_vocab_app_캐시_끄면_통계를_매번_DB에서_집계하고_BYPASS_캐시를_건드리지_않는다(vocab, slept):
    vocab.post("/api/cache-mode", json={"enabled": False})
    results = [stats_of(vocab)["cache"] for _ in range(3)]
    assert results == ["BYPASS"] * 3
    assert slept == [0.2] * 3
    assert vocab.get("/api/state").json()["keys"] == []


def test_vocab_app_캐시_끈_채_학습해도_켜면_최신_통계가_나온다(vocab):
    stats_of(vocab)  # 캐시에 옛 통계가 들어간다
    vocab.post("/api/cache-mode", json={"enabled": False})
    vocab.post("/api/study", json={"word_id": 1, "seconds": 4})
    vocab.post("/api/cache-mode", json={"enabled": True})
    assert stats_of(vocab)["stats"]["words_today"] == 1


def test_vocab_app_초기화_단어장_학습_기록_집계_캐시가_처음으로_돌아간다(vocab):
    stats_of(vocab)
    vocab.post("/api/words", json={"word": "cache", "meaning": ""})
    vocab.post("/api/study", json={"word_id": 1, "seconds": 9})
    assert vocab.post("/api/reset").json() == {"ok": True}
    state = vocab.get("/api/state").json()
    assert (state["hits"], state["misses"], state["db_reads"], state["words"], state["keys"]) == (0, 0, 0, 5, [])
    assert stats_of(vocab)["stats"] == {"words_today": 0, "seconds_today": 0, "streak_days": 4}


def _dead_redis():
    def dead(request):
        raise httpx.ConnectError("down")

    return httpx.Client(transport=httpx.MockTransport(dead), base_url="http://mock")


def test_vocab_app_캐시_서버가_죽으면_통계_조회는_502_CACHE_ERROR(slept, clock):
    app = TestClient(create_vocab_app(_dead_redis(), sleep=slept.append, now_ms=clock))
    response = app.get("/api/stats")
    assert response.status_code == 502
    assert response.json()["error"] == "CACHE_ERROR"


def test_vocab_app_캐시_서버가_죽으면_학습은_DB에_쓰고_502로_알린다(slept, clock):
    app = TestClient(create_vocab_app(_dead_redis(), sleep=slept.append, now_ms=clock))
    response = app.post("/api/study", json={"word_id": 1, "seconds": 3})
    assert response.status_code == 502 and response.json()["error"] == "CACHE_ERROR"


def test_vocab_app_루트_요청_HTML_화면(vocab):
    response = vocab.get("/")
    assert response.status_code == 200 and "단어장" in response.text


def test_RemoteDb_DB_읽기_쓰기마다_왕복_시간을_더하고_상태_조회는_더하지_않는다(redis, clock):
    waited = []
    inner = MemoryWords(0, lambda s: None, clock)
    app = TestClient(create_vocab_app(redis, ttl_s=60, words=RemoteDb(inner, 30, sleep=waited.append), now_ms=clock))

    app.get("/api/words")
    app.post("/api/study", json={"word_id": 1, "seconds": 3})
    app.get("/api/stats")
    assert waited == [0.03, 0.03, 0.03]  # 목록 읽기, 학습 쓰기, 통계 집계
    app.get("/api/state")
    app.post("/api/reset")
    assert waited == [0.03, 0.03, 0.03]  # 상태 조회와 초기화에는 더하지 않는다


def test_RemoteDb_통계_HIT는_DB를_거치지_않아_왕복_시간을_더하지_않는다(redis, clock):
    waited = []
    inner = MemoryWords(0, lambda s: None, clock)
    app = TestClient(create_vocab_app(redis, ttl_s=60, words=RemoteDb(inner, 30, sleep=waited.append), now_ms=clock))

    assert app.get("/api/stats").json()["cache"] == "MISS"
    assert app.get("/api/stats").json()["cache"] == "HIT"
    assert waited == [0.03]
    state = app.get("/api/state").json()
    assert (state["db"], state["db_rtt_ms"], state["db_reads"]) == ("memory", 30, 1)
