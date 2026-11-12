"""Store: 전역 RLock 하나로 모든 명령을 직렬화하는 계층.

API -> Store -> HashTable 순서로만 호출한다. 락을 잡는 곳은 Store뿐이다.
공개 메서드는 모두 @locked 로 감싸 명령 하나하나가 원자적이 되게 한다
(Redis의 명령 직렬 실행과 같은 효과). 디스크 I/O는 절대 락 안에서 하지 않는다.
"""
import functools
import re
import threading
from dataclasses import dataclass

from .clock import system_clock_ms
from .config import EXPIRE_TICK_MS, MAX_KEY_BYTES, MAX_KEYS, MAX_VALUE_BYTES
from .errors import (
    CapacityExceeded,
    ConfirmRequired,
    EmptyPrefix,
    InvalidKey,
    InvalidTtl,
    InvalidValue,
    KeyNotFound,
    NotInteger,
    ValueTooLarge,
)
from .hashtable import HashTable
from .timing_wheel import TimingWheel, start_expiry_thread


def locked(method):
    """공개 메서드가 Store의 전역 RLock을 잡고 실행되게 한다.

    RLock이라 Store 내부에서 다른 공개 메서드를 불러도 교착되지 않는다.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


def validate_key(key) -> None:
    """키는 비어 있지 않은 문자열이고 UTF-8로 1~256바이트여야 한다."""
    if not isinstance(key, str):
        raise InvalidKey("키는 문자열이어야 합니다")
    size = len(key.encode("utf-8", errors="surrogatepass"))
    if size < 1:
        raise InvalidKey("키는 비어 있을 수 없습니다")
    if size > MAX_KEY_BYTES:
        raise InvalidKey(f"키는 {MAX_KEY_BYTES}바이트 이하여야 합니다")


def validate_value(value) -> None:
    """값은 문자열이고 1MB 이하여야 한다. 빈 문자열은 허용한다."""
    if not isinstance(value, str):
        raise InvalidValue("값은 문자열이어야 합니다")
    if len(value.encode("utf-8", errors="surrogatepass")) > MAX_VALUE_BYTES:
        raise ValueTooLarge(f"값은 {MAX_VALUE_BYTES}바이트 이하여야 합니다")


INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
_INT_RE = re.compile(r"-?[0-9]{1,20}")  # ASCII 10진수만. int()는 공백·밑줄·전각숫자도 받아서 쓰지 않는다


def parse_int64(text: str) -> int:
    """10진 정수 문자열을 int로. 아니거나 64비트 범위 밖이면 NotInteger(409)."""
    if not _INT_RE.fullmatch(text):
        raise NotInteger("값이 10진 정수가 아닙니다")
    n = int(text)
    if not INT64_MIN <= n <= INT64_MAX:
        raise NotInteger("값이 64비트 정수 범위를 벗어납니다")
    return n


def _validate_prefix(prefix) -> None:
    if not isinstance(prefix, str):
        raise InvalidValue("prefix는 문자열이어야 합니다")


@dataclass(frozen=True)
class Item:
    """GET 결과. 엔트리 자체(가변)를 락 밖으로 내보내지 않으려고 복사해서 돌려준다."""

    value: str
    expire_at_ms: int | None  # None이면 무기한


class Store:
    def __init__(
        self,
        clock=system_clock_ms,
        max_keys: int = MAX_KEYS,
    ):
        """clock: epoch ms를 돌려주는 함수. 테스트는 FakeClock을 넣는다."""
        self._clock = clock
        self._max_keys = max_keys
        self._lock = threading.RLock()
        self._table = HashTable()
        self._wheel = TimingWheel(self._clock())
        self._expiry_stop = None  # 만료 스레드를 멈추는 Event

    # --- 내부 도우미 (호출하는 공개 메서드가 락을 이미 잡고 있다) ---

    def _now_ms(self) -> int:
        return self._clock()

    def _check_capacity(self, key: str) -> None:
        """새 키를 넣을 때만 상한을 검사한다 (덮어쓰기는 키 수가 늘지 않는다)."""
        if self._table.get(key) is None and self._table.size >= self._max_keys:
            raise CapacityExceeded(f"키 개수 상한({self._max_keys})을 초과했습니다")

    def _schedule_expiry(self, key: str, old_expire_at_ms, new_expire_at_ms) -> None:
        """휠에 (key, 만료 시각)을 등록한다. 낡은 항목이 쌓이지 않게 필요할 때만 add한다.

        새 만료가 생기거나 기존보다 앞당겨질 때만 add한다. 기존 만료보다 늦춰지는 경우는
        기존 항목이 먼저 꺼내질 때 _delete_if_current가 새 시각으로 다시 등록한다.
        """
        if new_expire_at_ms is None:
            return
        if old_expire_at_ms is None or new_expire_at_ms < old_expire_at_ms:
            self._wheel.add(key, new_expire_at_ms)

    def _delete_if_current(self, key: str, expire_at_ms: int) -> bool:
        """휠이 꺼낸 항목의 콜백. 엔트리의 현재 만료 시각이 같을 때만 지운다."""
        entry = self._table.get(key)
        if entry is None or entry.expire_at_ms is None:
            return False
        if entry.expire_at_ms == expire_at_ms:
            self._table.delete(key)
            return True
        if entry.expire_at_ms > expire_at_ms:
            self._wheel.add(key, entry.expire_at_ms)  # 만료가 늦춰졌다: 새 시각으로 재등록
        return False

    def _get_live(self, key: str):
        """살아 있는 엔트리를 돌려준다. 만료됐으면 그 자리에서 지우고 None (lazy 만료).

        "만료됨"과 "원래 없음"은 구분하지 않는다.
        """
        entry = self._table.get(key)
        if entry is None:
            return None
        if entry.expire_at_ms is not None and self._now_ms() >= entry.expire_at_ms:
            self._table.delete(key)
            return None
        return entry

    # --- 공개 메서드 ---

    @locked
    def now_ms(self) -> int:
        return self._now_ms()

    @locked
    def size(self) -> int:
        """저장된 엔트리 수 (만료됐지만 아직 회수되지 않은 키 포함)."""
        return self._table.size

    @locked
    def set(self, key: str, value: str, expire_at_ms: int | None = None) -> None:
        """키를 저장한다. 이미 있으면 값과 만료 시각을 통째로 교체한다.

        expire_at_ms를 안 주면 기존 TTL도 사라진다 (Redis와 동일).
        TTL(초) -> 절대 시각 변환과 INVALID_TTL 검증은 API(routes._parse_ttl)가 한다.
        """
        validate_key(key)
        validate_value(value)
        old = self._get_live(key)  # 같은 키의 만료된 옛 엔트리는 새 키로 취급하려고 먼저 지운다
        self._check_capacity(key)
        old_expire_at_ms = old.expire_at_ms if old is not None else None
        self._table.put(key, value, expire_at_ms)
        self._schedule_expiry(key, old_expire_at_ms, expire_at_ms)

    @locked
    def get(self, key: str) -> Item:
        """없거나 만료됐으면 KeyNotFound(404)."""
        validate_key(key)
        entry = self._get_live(key)
        if entry is None:
            raise KeyNotFound(f"키 '{key}'가 없습니다")
        return Item(entry.value, entry.expire_at_ms)

    @locked
    def delete(self, key: str) -> bool:
        """지웠으면 True, 원래 없었으면 False (에러 아님, 키가 새로 생기지 않는다)."""
        validate_key(key)
        if self._get_live(key) is None:
            return False
        return self._table.delete(key)

    @locked
    def exists(self, key: str) -> bool:
        validate_key(key)
        return self._get_live(key) is not None

    @locked
    def incr(self, key: str, amount: int = 1) -> int:
        """값을 10진 정수로 보고 amount만큼 더한 결과를 돌려준다.

        - 없는(또는 만료된) 키는 0에서 시작하고 TTL 없는 새 키가 된다.
        - 기존 키의 TTL은 그대로 유지한다 (Redis와 동일).
        - 정수가 아니거나 결과가 64비트를 넘으면 NotInteger(409), 값은 바뀌지 않는다.
        읽기와 쓰기가 한 번의 락 안에서 일어나서 동시 호출에도 합이 맞는다.
        """
        validate_key(key)
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise InvalidValue("amount는 정수여야 합니다")
        entry = self._get_live(key)
        if entry is None:
            self._check_capacity(key)
            current, expire_at_ms = 0, None
        else:
            current, expire_at_ms = parse_int64(entry.value), entry.expire_at_ms
        result = current + amount
        if not INT64_MIN <= result <= INT64_MAX:
            raise NotInteger("결과가 64비트 정수 범위를 벗어납니다")
        self._table.put(key, str(result), expire_at_ms)
        return result

    @locked
    def expire(self, key: str, ttl: int) -> bool:
        """만료를 지금부터 ttl초 뒤로 다시 잡는다. 남은 시간에 더하지 않는다 (Redis와 동일).

        - 없는(또는 만료된) 키는 False, 키가 새로 생기지 않는다.
        - ttl이 0 이하면 바로 지우고 True.
        - ttl이 정수가 아니면 InvalidTtl(400).
        """
        validate_key(key)
        if isinstance(ttl, bool) or not isinstance(ttl, int):
            raise InvalidTtl("ttl은 정수(초)여야 합니다")
        entry = self._get_live(key)
        if entry is None:
            return False
        if ttl <= 0:
            self._table.delete(key)  # AOF(#12): DEL로 기록
            return True
        old_expire_at_ms = entry.expire_at_ms
        new_expire_at_ms = self._now_ms() + ttl * 1000
        self._table.put(key, entry.value, new_expire_at_ms)
        self._schedule_expiry(key, old_expire_at_ms, new_expire_at_ms)
        return True

    @locked
    def ttl(self, key: str) -> int:
        """남은 TTL(초). 없거나 만료됐으면 -2, 무기한이면 -1.

        남은 ms를 올림한다. GET 응답의 ttl과 같은 숫자가 나오고, 살아 있는 키는 최소 1이다.
        """
        validate_key(key)
        entry = self._get_live(key)
        if entry is None:
            return -2
        if entry.expire_at_ms is None:
            return -1
        return -(-(entry.expire_at_ms - self._now_ms()) // 1000)

    @locked
    def keys(self, prefix: str = "") -> list[str]:
        """prefix로 시작하는 살아 있는 키 목록 (정렬). 빈 prefix는 전체.

        O(capacity + n)이고 락을 끝까지 잡는다. 순회 중 만나는 만료 키는 지운다.
        """
        _validate_prefix(prefix)
        return sorted(self._live_keys_with_prefix(prefix))

    @locked
    def delete_prefix(self, prefix: str) -> int:
        """prefix로 시작하는 키를 모두 지우고 살아 있던 키의 개수를 돌려준다.

        원자성을 위해 락을 끝까지 잡는다 (O(N), 키 상한 안에서 멈춤은 감수).
        빈 prefix는 EmptyPrefix(400) — 전체 삭제는 flush를 쓴다.
        """
        _validate_prefix(prefix)
        if prefix == "":
            raise EmptyPrefix("빈 prefix로는 지울 수 없습니다 (전체 삭제는 FLUSH)")
        victims = self._live_keys_with_prefix(prefix)
        for key in victims:
            self._table.delete(key)
        return len(victims)

    @locked
    def flush(self, confirm: bool = False) -> None:
        """모든 키를 지운다. 새 해시테이블과 타이밍 휠로 참조를 바꿔 끼우므로 O(1)이다.

        옛 테이블과 휠은 가비지 컬렉터가 치운다. 실수로 호출하지 못하게 confirm=True가 필요하다.
        """
        if confirm is not True:
            raise ConfirmRequired("FLUSH는 confirm=true가 필요합니다")
        self._table = HashTable()
        self._wheel = TimingWheel(self._now_ms())

    def _live_keys_with_prefix(self, prefix: str) -> list[str]:
        """호출하는 메서드가 락을 잡고 있다. 만료된 키는 지우고 목록에서 뺀다."""
        now = self._now_ms()
        live = []
        for entry in self._table.items():
            if not entry.key.startswith(prefix):
                continue
            if entry.expire_at_ms is not None and now >= entry.expire_at_ms:
                self._table.delete(entry.key)
                continue
            live.append(entry.key)
        return live

    @locked
    def expire_tick(self) -> int:
        """만료된 키를 휠에서 꺼내 지운다 (tick당 최대 EXPIRE_MAX_PER_TICK개). 지운 개수 반환.

        만료 스레드가 EXPIRE_TICK_MS마다 부른다. 락은 이 안에서만 잡고, 남은 항목은 다음 tick으로 넘긴다.
        """
        return self._wheel.expire(self._now_ms(), self._delete_if_current)

    def start_expiry(self, interval_ms: int = EXPIRE_TICK_MS) -> None:
        """백그라운드 만료 스레드를 시작한다. 복구가 끝난 뒤에 불러야 한다. 이미 돌고 있으면 무시."""
        with self._lock:
            if self._expiry_stop is None:
                self._expiry_stop = start_expiry_thread(self.expire_tick, interval_ms)

    def stop_expiry(self) -> None:
        with self._lock:
            stop, self._expiry_stop = self._expiry_stop, None
        if stop is not None:
            stop.set()
