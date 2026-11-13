"""나눠서 지우는 prefix 삭제(#26) 테스트. 스레드도 sleep도 쓰지 않아요.

끼어들기는 delete_prefix_batched의 테스트용 이음매 `_between_chunks`(덩어리 하나를 처리할 때마다
락을 놓은 채로 불리는 함수)로 결정적으로 흉내 내요.
"""
import pytest

from mini_redis.clock import FakeClock
from mini_redis.errors import EmptyPrefix, InvalidValue, KeyNotFound, LeaseInvalid
from mini_redis.hashtable import HashTable
from mini_redis.persistence.aof import AofUnavailableError, del_record
from mini_redis.store import Store

T0 = 1_000_000


@pytest.fixture
def clock():
    return FakeClock(T0)


@pytest.fixture
def store(clock):
    s = Store(clock=clock)
    for k in ["user:1", "user:2", "user:10", "post:1", "usr", "user"]:
        s.set(k, "v")
    return s


class RecordingAof:
    def __init__(self):
        self.records = []

    def append(self, record):
        self.records.append(record)


class FlakyAof:
    """fail_at번째 append(1부터 셈)에서 AofUnavailableError를 던져요. None이면 안 던져요."""

    def __init__(self, fail_at=None):
        self.records = []
        self.fail_at = fail_at
        self.calls = 0

    def append(self, record):
        self.calls += 1
        if self.fail_at is not None and self.calls == self.fail_at:
            raise AofUnavailableError("디스크가 막혔어요")
        self.records.append(record)


def _bulk_store(clock, n_user=40, n_other=10, **kwargs):
    s = Store(clock=clock, **kwargs)
    for i in range(n_user):
        s.set(f"user:{i}", "v")
    for i in range(n_other):
        s.set(f"other:{i}", "v")
    return s


def _settle(table):
    """진행 중인 리해싱을 끝까지 보내요(조회가 한 걸음씩 옮겨요)."""
    while table._old is not None:
        table.get("nothing")


def _keys(table):
    return [e.key for e in table.items()]


# --- HashTable.bucket_chunks ---

def _filled_table(n, rehash_in_progress):
    t = HashTable(rehash_buckets=1)
    i = 0
    if rehash_in_progress:
        while t._old is None:
            t.put(f"k:{i}", i)
            i += 1
    while i < n:
        t.put(f"k:{i}", i)
        i += 1
    if rehash_in_progress:
        assert t._old is not None
    return t


@pytest.mark.parametrize("rehash_in_progress", [False, True])
def test_bucket_chunks_all_chunks_together_cover_items_exactly_once(rehash_in_progress):
    t = _filled_table(60, rehash_in_progress)
    expected = sorted(_keys(t))

    got = [e.key for chunk in t.bucket_chunks(3) for e in chunk]

    assert sorted(got) == expected
    assert len(got) == t.size


@pytest.mark.parametrize("n", [0, -1])
def test_bucket_chunks_bad_n_raises_value_error_at_call_time_before_next(n):
    with pytest.raises(ValueError):
        HashTable().bucket_chunks(n)  # next()를 부르기 전, 호출하는 순간 터져야 해요


def test_bucket_chunks_while_alive_pins_rehash():
    t = _filled_table(20, True)
    gen = t.bucket_chunks(1)
    next(gen)
    idx = t._rehash_idx

    for _ in range(40):
        t.get("nothing")

    assert t._old is not None
    assert t._rehash_idx == idx
    gen.close()


def test_bucket_chunks_after_close_rehash_resumes():
    t = _filled_table(20, True)
    gen = t.bucket_chunks(1)
    next(gen)
    gen.close()
    assert t._iterating == 0

    for _ in range(40):
        t.get("nothing")

    assert t._old is None


# --- 기본 동작 ---

def test_delete_prefix_batched_removes_only_matching_keys(store):
    store.delete_prefix_batched("user:", 4)

    assert store.keys("") == ["post:1", "user", "usr"]


def test_delete_prefix_batched_returns_count_of_matching_keys(store):
    assert store.delete_prefix_batched("user:", 4) == 3


def test_delete_prefix_batched_no_match_returns_zero_and_keeps_all(store):
    assert store.delete_prefix_batched("zzz", 4) == 0
    assert store.size() == 6


@pytest.mark.parametrize("batch", [1, 4, 1000, 10**6])
def test_delete_prefix_batched_same_count_as_delete_prefix(clock, batch):
    a = _bulk_store(clock)
    b = _bulk_store(clock)

    assert a.delete_prefix_batched("user:", batch) == b.delete_prefix("user:")
    assert a.keys("") == b.keys("")


# --- 끼어들기 ---

def _by_number(key):
    return int(key.rsplit(":", 1)[1])


def test_delete_prefix_batched_del_of_next_chain_node_right_after_chain_head_chunk_still_deletes_the_rest(clock):
    s = Store(clock=clock)
    s._table = HashTable(capacity=16, hash_fn=_by_number)  # 숫자 % 16 버킷: user:2, user:18, user:34가 2번 버킷 한 체인
    for i in [2, 18, 34]:  # 체인 머리부터 user:34 -> user:18 -> user:2
        s.set(f"user:{i}", "v")
    s.set("post:5", "v")
    fired = []

    def between():
        # 체인 머리(user:34)를 처리한 직후, 체인의 다음 노드(user:18)를 끼어든 DEL이 지워요.
        # 체인 한가운데서 멈추는 구현이면 뒤쪽 user:2를 놓쳐요.
        if not fired and s._table.get("user:34") is None and s._table.get("user:18") is not None:
            fired.append(1)
            s.delete("user:18")

    s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert s.keys("") == ["post:5"]


def test_delete_prefix_batched_resize_started_mid_scan_still_deletes_all_preexisting_keys(clock):
    s = _bulk_store(clock)
    _settle(s._table)
    started = []

    def between():
        if not started:
            i = 0
            while s._table._old is None:
                s.set(f"fresh:{i}", "v")
                i += 1
            started.append(i)

    deleted = s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert started  # 스캔 도중 리사이즈가 실제로 시작됐어요
    assert deleted == 40
    assert s.keys("user:") == []


def test_delete_prefix_batched_flush_mid_scan_stops_and_returns_count_so_far(clock):
    s = _bulk_store(clock)
    old_table = s._table
    left_at_flush = []

    def between():
        if not left_at_flush:
            left_at_flush.append(len(s.keys("user:")))
            s.flush(confirm=True)

    deleted = s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert left_at_flush[0] > 0  # 정말 중간에 flush됐어요
    assert deleted == 40 - left_at_flush[0]
    assert old_table._iterating == 0  # 갈아 끼워진 옛 테이블의 제너레이터도 닫혔어요


def test_delete_prefix_batched_flush_mid_scan_leaves_new_table_untouched(clock):
    s = _bulk_store(clock)
    done = []

    def between():
        if not done:
            done.append(1)
            s.flush(confirm=True)
            s.set("user:new", "v")

    s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert s.keys("") == ["user:new"]


def test_delete_prefix_batched_set_of_new_matching_key_mid_scan_returns_and_deletes_preexisting(clock):
    s = _bulk_store(clock)
    done = []

    def between():
        if not done:
            done.append(1)
            s.set("user:new", "v")

    deleted = s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert 40 <= deleted <= 41  # 새 키는 지워져도 안 지워져도 돼요
    assert [k for k in s.keys("user:") if k != "user:new"] == []


# --- lease ---

def test_delete_prefix_batched_lease_acquired_mid_scan_stays_valid(clock):
    s = _bulk_store(clock)
    tokens = []

    def between():
        if not tokens:
            with pytest.raises(KeyNotFound) as e:
                s.get("user:late", lease=True)
            tokens.append(e.value.extra["lease"])

    s.delete_prefix_batched("user:", 1, _between_chunks=between)
    s.set("user:late", "v", lease=tokens[0])

    assert s.get("user:late").value == "v"


def test_delete_prefix_batched_lease_acquired_before_call_is_dropped(store):
    with pytest.raises(KeyNotFound) as e:
        store.get("user:ghost", lease=True)
    token = e.value.extra["lease"]

    store.delete_prefix_batched("user:", 4)

    with pytest.raises(LeaseInvalid):
        store.set("user:ghost", "v", lease=token)


# --- 만료 ---

def test_delete_prefix_batched_expired_keys_removed_but_not_counted(store, clock):
    store.set("user:tmp", "v", expire_at_ms=T0 + 1000)
    clock.advance(1000)

    assert store.delete_prefix_batched("user:", 4) == 3
    assert store.size() == 3


def test_delete_prefix_batched_expired_key_gets_del_record_in_aof(clock):
    aof = RecordingAof()
    s = Store(clock=clock, aof=aof)
    s.set("user:tmp", "v", expire_at_ms=T0 + 1000)
    clock.advance(1000)
    aof.records.clear()

    s.delete_prefix_batched("user:", 4)

    assert aof.records == [del_record("user:tmp")]


# --- AOF ---

def test_delete_prefix_batched_logs_one_del_per_deleted_key(clock):
    aof = RecordingAof()
    s = Store(clock=clock, aof=aof)
    for k in ["user:1", "user:2", "post:1"]:
        s.set(k, "v")
    aof.records.clear()

    s.delete_prefix_batched("user:", 1)

    assert sorted(r["k"] for r in aof.records) == ["user:1", "user:2"]
    assert all(r == del_record(r["k"]) for r in aof.records)


def test_delete_prefix_batched_aof_rejects_mid_scan_error_propagates_and_deleted_keys_stay_deleted(clock):
    aof = FlakyAof()
    s = _bulk_store(clock, n_user=20, aof=aof)
    aof.calls, aof.fail_at = 0, 6  # 5개는 기록되고 6번째에서 거절돼요

    table = s._table
    with pytest.raises(AofUnavailableError):
        s.delete_prefix_batched("user:", 1)

    assert len(s.keys("user:")) == 15
    assert table._iterating == 0  # 제너레이터가 닫혔어요


def test_delete_prefix_batched_retry_after_aof_reject_deletes_the_rest(clock):
    aof = FlakyAof()
    s = _bulk_store(clock, n_user=20, aof=aof)
    aof.calls, aof.fail_at = 0, 6
    with pytest.raises(AofUnavailableError):
        s.delete_prefix_batched("user:", 1)

    aof.fail_at = None
    deleted = s.delete_prefix_batched("user:", 1)

    assert deleted == 15
    assert s.keys("user:") == []


class LockWatchingTable(HashTable):
    """_iterating이 줄어드는 순간(= 제너레이터가 닫히는 순간)에 스토어 락을 쥐고 있었는지 기록해요."""

    def __init__(self, **kwargs):
        self._iter_count = 0
        self.watched_lock = None
        self.owned_at_close = []
        super().__init__(**kwargs)

    @property
    def _iterating(self):
        return self._iter_count

    @_iterating.setter
    def _iterating(self, value):
        if value < self._iter_count and self.watched_lock is not None:
            self.owned_at_close.append(self.watched_lock._is_owned())
        self._iter_count = value


def _watched_store(clock, **kwargs):
    s = Store(clock=clock, **kwargs)
    table = LockWatchingTable()
    table.watched_lock = s._lock
    s._table = table
    for i in range(20):
        s.set(f"user:{i}", "v")
    return s, table


def test_delete_prefix_batched_generator_closes_while_store_lock_is_held_on_normal_finish(clock):
    s, table = _watched_store(clock)

    s.delete_prefix_batched("user:", 1)

    assert table.owned_at_close == [True]


def test_delete_prefix_batched_generator_closes_while_store_lock_is_held_on_aof_reject(clock):
    aof = FlakyAof()
    s, table = _watched_store(clock, aof=aof)
    aof.calls, aof.fail_at = 0, 6

    with pytest.raises(AofUnavailableError):
        s.delete_prefix_batched("user:", 1)

    assert table.owned_at_close == [True]


def test_delete_prefix_batched_generator_closes_while_store_lock_is_held_on_flush(clock):
    s, table = _watched_store(clock)
    done = []

    def between():
        if not done:
            done.append(1)
            s.flush(confirm=True)

    s.delete_prefix_batched("user:", 1, _between_chunks=between)

    assert table.owned_at_close == [True]


# --- 검증 ---

def test_delete_prefix_batched_empty_prefix_raises_empty_prefix_and_deletes_nothing(store):
    with pytest.raises(EmptyPrefix):
        store.delete_prefix_batched("", 4)
    assert store.size() == 6


def test_delete_prefix_batched_non_string_prefix_raises_invalid_value(store):
    with pytest.raises(InvalidValue):
        store.delete_prefix_batched(123, 4)
    assert store.size() == 6


@pytest.mark.parametrize("batch", [0, -1, True, "3"])
def test_delete_prefix_batched_bad_batch_raises_invalid_value_and_deletes_nothing(store, batch):
    with pytest.raises(InvalidValue):
        store.delete_prefix_batched("user:", batch)
    assert store.size() == 6


# --- 락 ---

def test_delete_prefix_batched_releases_lock_between_chunks(clock):
    s = _bulk_store(clock)
    owned = []

    s.delete_prefix_batched("user:", 1, _between_chunks=lambda: owned.append(s._lock._is_owned()))

    assert owned  # 덩어리가 여러 개라 여러 번 불렸어요
    assert not any(owned)
