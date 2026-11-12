"""EXPIRE / TTL 명령과 TTL 규칙 (#7). 시간은 FakeClock으로만 움직인다."""
import pytest

from mini_redis.clock import FakeClock
from mini_redis.errors import InvalidTtl, KeyNotFound
from mini_redis.store import Store

T0 = 1_000_000


@pytest.fixture
def clock():
    return FakeClock(T0)


@pytest.fixture
def store(clock):
    return Store(clock=clock)


# --- TTL ---


def test_ttl_missing_key_returns_minus_2(store):
    assert store.ttl("nope") == -2


def test_ttl_key_without_expire_returns_minus_1(store):
    store.set("a", "1")
    assert store.ttl("a") == -1


def test_ttl_just_before_and_at_expiry_returns_1_then_minus_2(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 10_000)
    clock.advance(9_999)
    assert store.ttl("a") == 1  # 1ms 남음 -> 올림해서 1, 0은 나오지 않는다
    clock.advance(1)
    assert store.ttl("a") == -2
    assert store.size() == 0  # 접근한 순간 지웠다 (lazy)


def test_ttl_partial_second_rounds_up_like_get(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 60_000)
    clock.advance(18_500)  # 41.5초 남음
    assert store.ttl("a") == 42


def test_ttl_overwrite_without_ttl_returns_minus_1(store):
    store.set("a", "1", expire_at_ms=T0 + 60_000)
    store.set("a", "2")
    assert store.ttl("a") == -1


# --- EXPIRE ---


def test_expire_missing_key_returns_false_and_creates_nothing(store):
    assert store.expire("nope", 10) is False
    assert store.size() == 0


def test_expire_expired_key_returns_false_and_removes_it(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 1_000)
    clock.advance(1_000)
    assert store.expire("a", 10) is False
    assert store.size() == 0


def test_expire_key_without_ttl_expires_after_ttl_seconds(store, clock):
    store.set("a", "1")
    assert store.expire("a", 10) is True
    clock.advance(9_999)
    assert store.get("a").value == "1"
    clock.advance(1)
    with pytest.raises(KeyNotFound):
        store.get("a")


def test_expire_recomputes_from_now_not_added_to_remaining(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 10_000)
    clock.advance(4_000)  # 6초 남음
    store.expire("a", 10)
    assert store.get("a").expire_at_ms == T0 + 14_000  # 6+10초도, 처음+10초도 아니다
    assert store.ttl("a") == 10


def test_expire_keeps_value(store):
    store.set("a", "v")
    store.expire("a", 10)
    assert store.get("a").value == "v"


@pytest.mark.parametrize("ttl", [0, -1])
def test_expire_zero_or_negative_deletes_now_returns_true(store, ttl):
    store.set("a", "1")
    assert store.expire("a", ttl) is True
    assert store.size() == 0  # exists()의 lazy 삭제보다 먼저 확인: EXPIRE가 직접 지웠다
    assert store.exists("a") is False


def test_expire_zero_on_missing_key_returns_false(store):
    assert store.expire("nope", 0) is False


@pytest.mark.parametrize("ttl", [1.5, "10", True, None])
def test_expire_non_integer_ttl_raises_invalid_ttl_and_keeps_key(store, ttl):
    store.set("a", "1")
    with pytest.raises(InvalidTtl) as e:
        store.expire("a", ttl)
    assert (e.value.status, e.value.code) == (400, "INVALID_TTL")
    assert store.ttl("a") == -1


# --- EXPIRE와 타이밍 휠: 아무도 접근하지 않아도 새 만료 시각에 지워진다 ---


def test_expire_tick_after_expire_on_key_without_ttl_removes_key(store, clock):
    store.set("a", "1")
    store.expire("a", 1)
    clock.advance(1_000)
    assert store.expire_tick() == 1
    assert store.size() == 0


def test_expire_tick_after_expire_shortens_ttl_removes_at_new_time(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 60_000)
    store.expire("a", 1)
    clock.advance(1_000)
    assert store.expire_tick() == 1


def test_expire_tick_after_expire_extends_ttl_keeps_until_new_time(store, clock):
    store.set("a", "1", expire_at_ms=T0 + 1_000)
    store.expire("a", 10)  # 늦춤: 옛 휠 항목이 먼저 꺼내지면 새 시각으로 재등록된다
    clock.advance(1_000)
    assert store.expire_tick() == 0
    assert store.get("a").value == "1"
    clock.advance(9_000)
    assert store.expire_tick() == 1
