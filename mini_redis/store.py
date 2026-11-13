"""Store: 전역 RLock 하나로 모든 명령을 직렬화하는 계층.

API -> Store -> HashTable 순서로만 호출한다. 락을 잡는 곳은 Store뿐이다.
공개 메서드는 모두 @locked 로 감싸 명령 하나하나가 원자적이 되게 한다
(Redis의 명령 직렬 실행과 같은 효과). 디스크 I/O는 절대 락 안에서 하지 않는다.

SIEVE 축출(#25)의 접근: 살아 있는 키의 값을 읽거나 바꾼 명령(GET 히트, SET 덮어쓰기, INCR, EXPIRE)이
엔트리의 visited를 켠다. EXISTS와 TTL은 있는지·남은 시간만 묻는 명령이라 켜지 않는다(Redis도 이 둘은
LOOKUP_NOTOUCH로 LRU를 건드리지 않는다). KEYS, 스냅샷 복사도 접근이 아니다.
"""
import functools
import random
import re
import threading
from dataclasses import dataclass

from .clock import system_clock_ms
from .config import EVICTION_POLICY, EXPIRE_TICK_MS, MAX_KEY_BYTES, MAX_KEYS, MAX_VALUE_BYTES, XFETCH_BETA
from .errors import (
    CapacityExceeded,
    ConfirmRequired,
    EmptyPrefix,
    GuardRequired,
    InvalidKey,
    InvalidTtl,
    InvalidValue,
    KeyNotFound,
    LeaseInvalid,
    NotInteger,
    TtlRequired,
    ValueTooLarge,
    VersionMismatch,
)
from .hashtable import HashTable
from .lease import LeaseTable
from .namespace import VersionTable
from .persistence.aof import del_record, expire_record, flush_record, ns_version_record, set_record
from .timing_wheel import TimingWheel, start_expiry_thread
from .xfetch import should_refresh_early


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
    try:
        size = len(key.encode("utf-8"))
    except UnicodeEncodeError:
        # 외톨이 서로게이트(JSON의 "\ud800" 등)는 UTF-8로 쓸 수 없어 AOF 기록과 JSON 응답이 깨진다
        raise InvalidKey("키에 UTF-8로 표현할 수 없는 문자가 있습니다")
    if size < 1:
        raise InvalidKey("키는 비어 있을 수 없습니다")
    if size > MAX_KEY_BYTES:
        raise InvalidKey(f"키는 {MAX_KEY_BYTES}바이트 이하여야 합니다")


def validate_value(value) -> None:
    """값은 문자열이고 1MB 이하여야 한다. 빈 문자열은 허용한다."""
    if not isinstance(value, str):
        raise InvalidValue("값은 문자열이어야 합니다")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise InvalidValue("값에 UTF-8로 표현할 수 없는 문자가 있습니다")
    if size > MAX_VALUE_BYTES:
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
    lease: str | None = None  # 값은 있지만 이 요청이 조기 갱신을 맡았다는 표시 (XFetch)
    version: int | None = None  # ns를 붙여 읽었을 때 그 ns의 현재 버전


class Store:
    def __init__(
        self,
        clock=system_clock_ms,
        max_keys: int = MAX_KEYS,
        random_fn=random.random,
        xfetch_beta: float = XFETCH_BETA,
        aof=None,
        eviction: str = EVICTION_POLICY,
    ):
        """clock: epoch ms를 돌려주는 함수. 테스트는 FakeClock을 넣는다.

        random_fn: [0, 1) 난수를 돌려주는 함수. XFetch의 조기 갱신 추첨에 쓰며, 테스트는 고정값을 넣는다.
        aof: append(record) 를 가진 객체. None이면 기록하지 않는다.
        eviction: 키 상한에서 새 키를 쓸 때. "noeviction"은 CapacityExceeded(507), "sieve"는 한 키를 내보낸다.
        """
        if eviction not in ("noeviction", "sieve"):
            raise ValueError(f"알 수 없는 축출 정책: {eviction!r}")
        self._clock = clock
        self._max_keys = max_keys
        self._eviction = eviction
        self._random = random_fn
        self._xfetch_beta = xfetch_beta
        self._lock = threading.RLock()
        self._table = HashTable()
        self._wheel = TimingWheel(self._clock())
        self._leases = LeaseTable()
        self._versions = VersionTable(on_change=lambda ns, v: self._log(ns_version_record(ns, v)))  # FLUSH에도 유지한다
        self._expiry_stop = None  # 만료 스레드를 멈추는 Event
        self._aof = aof

    # --- 내부 도우미 (호출하는 공개 메서드가 락을 이미 잡고 있다) ---

    def _now_ms(self) -> int:
        return self._clock()

    def _ns_key(self, key: str, ns: str | None):
        """키를 검증하고 (내부 키, 버전)을 돌려준다. ns가 있으면 `ns:v{현재 버전}:key`, 없으면 (key, None).

        검증은 사용자가 보낸 키와 ns에 각각 한다 (내부 키는 버전 접두사만큼 길어져 256바이트를 넘을 수 있다).
        """
        validate_key(key)
        if ns is None:
            return key, None
        validate_key(ns)
        version = self._versions.current(ns, self._now_ms())
        return f"{ns}:v{version}:{key}", version

    def _log(self, record: dict) -> None:
        """상태를 바꾸기 직전에 AOF에 기록한다 (기록 먼저, 변경은 그다음).

        메모리에만 바뀌고 AOF에 없으면 재시작 때 사라지는데 클라이언트는 이미 성공을 봤기 때문이다.
        append가 예외를 던지면 그대로 올라가고 그 변경은 일어나지 않는다.
        """
        if self._aof is not None:
            self._aof.append(record)

    def _check_capacity(self, key: str) -> bool:
        """새 키를 넣을 때만 상한을 검사한다 (덮어쓰기는 키 수가 늘지 않는다).

        자리가 없으면 noeviction은 CapacityExceeded(507), sieve는 True를 돌려준다.
        sieve는 요청이 성공할 게 확실해진 뒤(lease·정수 검사 뒤), 새 키를 기록하기 직전에 _evict를 부른다.
        거절되는 요청이 키를 내보내지 않게 하려는 것이다.
        """
        if self._table.get(key) is not None or self._table.size < self._max_keys:
            return False
        if self._eviction == "noeviction":
            raise CapacityExceeded(f"키 개수 상한({self._max_keys})을 초과했습니다")
        return True

    def _evict(self) -> None:
        """SIEVE로 고른 키 하나를 내보낸다. 만료 삭제처럼 AOF에 DEL로 먼저 적는다.

        적지 않으면 재시작 때 내보낸 키가 되살아난다. lease는 지우지 않는다: DEL과 달리 원본이 바뀌었다는
        신호가 아니라서, 진행 중인 lease PUT이 가져올 값은 여전히 최신이다.
        ponytail: 내보낸 키의 휠 항목은 DEL 때처럼 그대로 두고, 만료 시각에 꺼낼 때 낡은 항목으로 무시된다.
        축출이 잦고 TTL이 길면 휠 항목 수가 키 상한보다 커질 수 있다. 문제가 되면 휠에 취소 연산을 둔다
        """
        victim = self._table.sieve_victim()
        self._log(del_record(victim.key))
        self._table.delete(victim.key)

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
            try:
                self._log(del_record(key))
            except BaseException:
                # 휠은 이미 항목을 꺼냈어요. 로그가 실패하면 등록을 되돌려 다음 tick에 다시 시도하게 해요.
                self._wheel.add(key, expire_at_ms)
                raise
            self._table.delete(key)
            return True
        if entry.expire_at_ms > expire_at_ms:
            self._wheel.add(key, entry.expire_at_ms)  # 만료가 늦춰졌다: 새 시각으로 재등록
        return False

    def _early_refresh_lease(self, key: str, entry) -> str | None:
        """XFetch 추첨에 당첨되고 이 키에 살아 있는 lease가 없으면 새 lease 토큰을 돌려준다.

        delta는 이 값을 마지막으로 lease 경로로 채울 때 잰 값이다. 없으면(처음 값이거나 lease 없이 쓴 값)
        측정 근거가 없어서 조기 갱신을 하지 않는다. 이미 누군가 갱신 중이면(lease가 살아 있으면) None이다.
        """
        if entry.expire_at_ms is None or not entry.delta_ms:
            return None
        now = self._now_ms()
        u = 1.0 - self._random()  # [0, 1) -> (0, 1]: ln(0)을 피한다
        if not should_refresh_early(now, entry.expire_at_ms, entry.delta_ms, self._xfetch_beta, u):
            return None
        token, _ = self._leases.acquire(key, now)
        return token

    def _get_live(self, key: str):
        """살아 있는 엔트리를 돌려준다. 만료됐으면 그 자리에서 지우고 None (lazy 만료).

        "만료됨"과 "원래 없음"은 구분하지 않는다.
        """
        entry = self._table.get(key)
        if entry is None:
            return None
        if entry.expire_at_ms is not None and self._now_ms() >= entry.expire_at_ms:
            self._log(del_record(key))
            self._table.delete(key)
            return None
        return entry

    # --- 공개 메서드 ---

    @locked
    def attach_aof(self, aof) -> None:
        """AOF 기록기를 바꿔 끼운다 (None이면 기록 중단). 복구로 키를 먼저 채운 뒤 붙여서 복구분이 다시 기록되지 않게 한다."""
        self._aof = aof

    @locked
    def version_items(self) -> list[tuple[str, int]]:
        """네임스페이스 버전 표를 (네임스페이스, 버전) 목록으로 복사한다. 스냅샷이 쓴다."""
        return [(e.key, e.value) for e in self._versions._table.items()]

    @locked
    def restore_entries(self, entries) -> None:
        """복구한 (키, 값, 만료 시각)을 표에 그대로 넣는다.

        `ns:v{버전}:key`처럼 외부 키 한도(256바이트)보다 길 수 있는 내부 키를 복원하려고 있다.
        그래서 키·값 검증은 하지 않는다. 변경이 아니라 복원이라 AOF에 기록하지 않는다.
        AOF가 붙은 뒤에는 쓸 수 없다. 복구는 AOF를 붙이기 전에 끝내야 한다.
        """
        if self._aof is not None:
            raise RuntimeError("AOF가 붙은 뒤에는 엔트리를 복원할 수 없어요")
        for key, value, expire_at_ms in entries:
            # ponytail: SIEVE 큐는 영속화하지 않는다. 복원한 순서(복구 표의 버킷 순서)로 다시 줄 서고 visited는 모두 꺼진다.
            # 재시작 직후 축출이 FIFO 대신 해시 순서가 된다. 문제가 되면 스냅샷을 큐 순서로 쓰고 그 순서로 복원한다
            if self._check_capacity(key):
                self._evict()  # 상한을 줄여 재시작했을 때. AOF가 없어 기록되지 않는다
            self._table.put(key, value, expire_at_ms)
            self._schedule_expiry(key, None, expire_at_ms)

    @locked
    def restore_versions(self, pairs) -> None:
        """복구한 (네임스페이스, 버전)을 표에 그대로 넣는다. 변경이 아니라 복원이라 on_change로 기록하지 않는다.

        AOF가 붙은 뒤에는 쓸 수 없다. 복구는 AOF를 붙이기 전에 끝내야 한다.
        """
        if self._aof is not None:
            raise RuntimeError("AOF가 붙은 뒤에는 버전을 복원할 수 없어요")
        for ns, version in pairs:
            self._versions._table.put(ns, version)

    @locked
    def now_ms(self) -> int:
        return self._now_ms()

    @locked
    def copy_for_snapshot(self, during_lock=None) -> list[tuple[str, str, int | None]]:
        """만료되지 않은 엔트리를 (키, 값, 만료 시각) 목록으로 복사한다.

        상태를 바꾸지 않는다: 만료된 키는 건너뛸 뿐 지우지도 AOF에 적지도 않는다.
        during_lock이 있으면 복사 직후 락을 잡은 채로 부른다. 스냅샷이 여기서 AOF 파일을 바꿔 끼우면
        복사본과 새 AOF의 경계가 정확히 맞는다.
        """
        now = self._now_ms()
        entries = [
            (entry.key, entry.value, entry.expire_at_ms)
            for entry in self._table.items()
            if entry.expire_at_ms is None or now < entry.expire_at_ms
        ]
        if during_lock is not None:
            during_lock()
        return entries

    @locked
    def size(self) -> int:
        """저장된 엔트리 수 (만료됐지만 아직 회수되지 않은 키 포함)."""
        return self._table.size

    @locked
    def set(
        self,
        key: str,
        value: str,
        expire_at_ms: int | None = None,
        lease: str | None = None,
        ns: str | None = None,
        v: int | None = None,
    ) -> None:
        """키를 저장한다. 이미 있으면 값과 만료 시각을 통째로 교체한다.

        expire_at_ms를 안 주면 기존 TTL도 사라진다 (Redis와 동일).
        TTL(초) -> 절대 시각 변환과 INVALID_TTL 검증은 API(routes._parse_ttl)가 한다.
        lease를 주면 그 키의 살아 있는 lease 토큰과 같아야 저장하고 lease를 지운다.
        없거나 만료·무효화됐거나 다르면 LeaseInvalid(409)이고 아무것도 바꾸지 않는다.
        lease 없는 쓰기는 일반 쓰기로 받고, 진행 중인 lease는 무효화한다.

        ns를 주면 `ns:v{현재 버전}:key`에 쓴다. TTL이 없으면 TtlRequired(400), lease도 v도 없으면
        GuardRequired(400), v가 현재 버전과 다르면 VersionMismatch(409)이고 아무것도 바꾸지 않는다.
        """
        key, version = self._ns_key(key, ns)
        validate_value(value)
        if lease is not None and not isinstance(lease, str):
            raise InvalidValue("lease는 문자열이어야 합니다")
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise InvalidValue("v는 정수여야 합니다")
        if ns is None:
            if v is not None:
                raise InvalidValue("v는 ns와 함께만 쓸 수 있습니다")
        else:
            if expire_at_ms is None:
                # 옛 버전 키는 다시 읽히지 않아서 TTL로만 사라진다
                raise TtlRequired("ns를 붙인 SET은 ttl이 필요합니다")
            if lease is None and v is None:
                # 서버는 도착 시점의 버전으로 키를 바꾸므로, invalidate 전에 읽은 낡은 값이 새 버전 키에 써질 수 있다
                raise GuardRequired("ns를 붙인 SET은 lease나 읽을 때 받은 버전(v)이 필요합니다")
            if v is not None and v != version:
                # 현재 버전은 알려 주지 않는다: 그 버전으로 바로 다시 쓰면 같은 낡은 값이 들어간다. 다시 GET부터 한다
                raise VersionMismatch(f"읽은 뒤 네임스페이스 '{ns}'가 무효화됐습니다 (v={v}). 다시 GET하세요")
        old = self._get_live(key)  # 같은 키의 만료된 옛 엔트리는 새 키로 취급하려고 먼저 지운다
        evict = self._check_capacity(key)  # noeviction의 507은 lease를 건드리기 전에 낸다
        delta_ms = None
        if lease is None:
            # 늦게 오는 lease PUT이 이 값보다 낡은 원본으로 덮어쓰지 못하게 한다
            self._leases.drop(key)
        else:
            now = self._now_ms()
            issued_at = self._leases.issued_at(key, now)  # consume이 lease를 지우기 전에 읽는다
            if not self._leases.consume(key, lease, now):
                raise LeaseInvalid("lease가 없거나 만료·무효화됐습니다. 다시 GET ?lease=1을 하세요")
            # lease 발급부터 PUT까지가 값을 다시 만드는 데 걸린 시간이다 (XFetch의 delta)
            delta_ms = now - issued_at
        old_expire_at_ms = old.expire_at_ms if old is not None else None
        if evict:
            self._evict()
        self._log(set_record(key, value, expire_at_ms))
        self._table.put(key, value, expire_at_ms, delta_ms)
        if old is not None:
            old.visited = True  # 덮어쓰기는 같은 엔트리를 고친다
        self._schedule_expiry(key, old_expire_at_ms, expire_at_ms)

    @locked
    def get(self, key: str, lease: bool = False, ns: str | None = None) -> Item:
        """없거나 만료됐으면 KeyNotFound(404).

        lease=True면 miss일 때 그 404에 새 lease 토큰(`lease`)을 붙인다. 같은 키에 살아 있는
        lease가 있으면 토큰 대신 `retry_after_ms`를 붙인다. 확인과 발급이 한 락 안이라 토큰은 하나만 나간다.

        lease=True이고 값이 있어도, 만료가 가까워 XFetch가 "지금 갱신하라"고 추첨한 요청 하나에는
        값과 함께 lease 토큰을 준다 (Item.lease). 그 클라이언트는 만료 전에 원본을 불러 토큰으로 PUT한다.
        다른 요청은 기다림 없이 평범한 히트를 받는다.

        ns를 주면 `ns:v{현재 버전}:key`를 읽고, 히트(Item.version)와 404(`version`) 모두에 현재 버전을 준다.
        miss를 받은 클라이언트도 그 버전을 `?v=`로 실어 PUT해야 해서다.
        """
        key, version = self._ns_key(key, ns)
        entry = self._get_live(key)
        if entry is not None:
            entry.visited = True
            token = self._early_refresh_lease(key, entry) if lease else None
            return Item(entry.value, entry.expire_at_ms, token, version)
        miss = KeyNotFound(f"키 '{key}'가 없습니다")
        if version is not None:
            miss.extra["version"] = version
        if lease:
            token, retry_after_ms = self._leases.acquire(key, self._now_ms())
            if token is not None:
                miss.extra["lease"] = token
            else:
                miss.extra["retry_after_ms"] = retry_after_ms
        raise miss

    @locked
    def delete(self, key: str, ns: str | None = None) -> bool:
        """지웠으면 True, 원래 없었으면 False (에러 아님, 키가 새로 생기지 않는다).

        키가 없어도 lease는 무효화한다. lease 보유자가 원본을 읽는 사이 원본이 바뀌었다는 신호다.
        """
        key, _ = self._ns_key(key, ns)
        self._leases.drop(key)
        if self._get_live(key) is None:
            return False
        self._log(del_record(key))
        return self._table.delete(key)

    @locked
    def exists(self, key: str, ns: str | None = None) -> bool:
        key, _ = self._ns_key(key, ns)
        return self._get_live(key) is not None

    @locked
    def incr(self, key: str, amount: int = 1, ns: str | None = None) -> int:
        """값을 10진 정수로 보고 amount만큼 더한 결과를 돌려준다.

        - 없는(또는 만료된) 키는 0에서 시작하고 TTL 없는 새 키가 된다.
        - 기존 키의 TTL은 그대로 유지한다 (Redis와 동일).
        - 정수가 아니거나 결과가 64비트를 넘으면 NotInteger(409), 값은 바뀌지 않는다.
        - ns를 붙였는데 키가 없으면 TtlRequired(400). INCR은 TTL을 줄 수 없어 무기한 키가 생기기 때문이다.
        읽기와 쓰기가 한 번의 락 안에서 일어나서 동시 호출에도 합이 맞는다.
        """
        key, _ = self._ns_key(key, ns)
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise InvalidValue("amount는 정수여야 합니다")
        entry = self._get_live(key)
        evict = False
        if entry is None:
            if ns is not None:
                raise TtlRequired("ns를 붙인 INCR은 TTL이 있는 기존 키에만 쓸 수 있습니다 (먼저 ttl과 함께 PUT)")
            evict = self._check_capacity(key)
            current, expire_at_ms = 0, None
        else:
            current, expire_at_ms = parse_int64(entry.value), entry.expire_at_ms
        result = current + amount
        if not INT64_MIN <= result <= INT64_MAX:
            raise NotInteger("결과가 64비트 정수 범위를 벗어납니다")
        if evict:
            self._evict()
        self._log(set_record(key, str(result), expire_at_ms))
        self._leases.drop(key)  # lease 없는 쓰기라 SET과 같이 진행 중인 lease를 무효화한다
        self._table.put(key, str(result), expire_at_ms)
        if entry is not None:
            entry.visited = True
        return result

    @locked
    def expire(self, key: str, ttl: int, ns: str | None = None) -> bool:
        """만료를 지금부터 ttl초 뒤로 다시 잡는다. 남은 시간에 더하지 않는다 (Redis와 동일).

        - 없는(또는 만료된) 키는 False, 키가 새로 생기지 않는다.
        - ttl이 0 이하면 바로 지우고 True.
        - ttl이 정수가 아니면 InvalidTtl(400).
        """
        key, _ = self._ns_key(key, ns)
        if isinstance(ttl, bool) or not isinstance(ttl, int):
            raise InvalidTtl("ttl은 정수(초)여야 합니다")
        entry = self._get_live(key)
        if entry is None:
            return False
        if ttl <= 0:
            self._log(del_record(key))
            self._table.delete(key)
            return True
        old_expire_at_ms = entry.expire_at_ms
        new_expire_at_ms = self._now_ms() + ttl * 1000
        self._log(expire_record(key, new_expire_at_ms))
        self._table.put(key, entry.value, new_expire_at_ms, entry.delta_ms)  # 값은 그대로라 delta도 유지
        entry.visited = True  # 세션의 "활동 시 EXPIRE"처럼 키를 쓰고 있다는 신호다 (Redis EXPIRE도 LRU를 갱신)
        self._schedule_expiry(key, old_expire_at_ms, new_expire_at_ms)
        return True

    @locked
    def ttl(self, key: str, ns: str | None = None) -> int:
        """남은 TTL(초). 없거나 만료됐으면 -2, 무기한이면 -1.

        남은 ms를 올림한다. GET 응답의 ttl과 같은 숫자가 나오고, 살아 있는 키는 최소 1이다.
        """
        key, _ = self._ns_key(key, ns)
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
        self._leases.drop_prefix(prefix)  # 키가 아직 없는 lease도 무효화한다 (DEL과 같은 이유)
        victims = self._live_keys_with_prefix(prefix)
        for key in victims:
            self._log(del_record(key))
            self._table.delete(key)
        return len(victims)

    def delete_prefix_batched(self, prefix: str, batch_buckets: int, *, _between_chunks=None) -> int:
        """delete_prefix와 같은 일을 버킷 batch_buckets개씩 나눠, 덩어리 사이에는 락을 풀어 준다.

        @locked를 붙이지 않는다. 덩어리마다 락을 잡았다 놓으니 다른 요청이 그 사이에 끼어들 수 있다.
        - 원자적이지 않다: 도는 동안 새로 SET된 키는 지워질 수도, 아닐 수도 있다 (SCAN과 같은 약속).
        - lease는 시작할 때 한 번만 지운다. 도중에 받은 lease는 그대로 유효하다.
        - 도는 동안 FLUSH가 테이블을 바꿔 끼우면 거기서 멈추고 그때까지 센 개수를 돌려준다.
        - AOF가 거절하면 예외가 그대로 올라간다. 이미 지운 키는 지워진 채 남고, 다시 부르면 이어서 지운다.
        - _between_chunks: 테스트 전용 이음매. 덩어리 하나를 처리하고 락을 푼 뒤에 인자 없이 불린다.
        """
        _validate_prefix(prefix)
        if prefix == "":
            raise EmptyPrefix("빈 prefix로는 지울 수 없습니다 (전체 삭제는 FLUSH)")
        if isinstance(batch_buckets, bool) or not isinstance(batch_buckets, int) or batch_buckets < 1:
            raise InvalidValue("batch는 1 이상의 정수여야 합니다")
        with self._lock:
            self._leases.drop_prefix(prefix)
            table = self._table
            gen = table.bucket_chunks(batch_buckets)
        deleted = 0
        try:
            while True:
                with self._lock:
                    if self._table is not table:  # FLUSH가 테이블을 바꿔 끼웠다
                        break
                    chunk = next(gen, None)
                    if chunk is None:
                        break
                    now = self._now_ms()
                    for entry in chunk:
                        if not entry.key.startswith(prefix):
                            continue
                        expired = entry.expire_at_ms is not None and now >= entry.expire_at_ms
                        self._log(del_record(entry.key))
                        if table.delete(entry.key) and not expired:
                            deleted += 1
                if _between_chunks is not None:
                    _between_chunks()
        finally:
            with self._lock:  # 닫으면서 _iterating을 내리니 락 안에서 (RLock이라 다시 잡아도 괜찮아요)
                gen.close()
        return deleted

    @locked
    def invalidate(self, ns: str) -> int:
        """네임스페이스의 버전을 올려 옛 버전 키를 한 번에 안 보이게 한다. 새 버전을 돌려준다. O(1).

        옛 키는 지우지 않고 TTL로 사라진다. 진행 중인 lease도 옛 키에 묶여 있어 그 PUT은 409가 된다.
        """
        validate_key(ns)
        return self._versions.invalidate(ns, self._now_ms())

    @locked
    def flush(self, confirm: bool = False) -> None:
        """모든 키를 지운다. 새 해시테이블·타이밍 휠·lease 테이블로 참조를 바꿔 끼우므로 O(1)이다.

        옛 테이블과 휠은 가비지 컬렉터가 치운다. 실수로 호출하지 못하게 confirm=True가 필요하다.
        버전 테이블은 그대로 둔다(명세). 새로 만들면 시계가 뒤로 갔을 때 버전이 되돌아가 FLUSH 전에 받은 v가 다시 맞을 수 있다.
        """
        if confirm is not True:
            raise ConfirmRequired("FLUSH는 confirm=true가 필요합니다")
        self._log(flush_record())
        self._table = HashTable()
        self._wheel = TimingWheel(self._now_ms())
        self._leases = LeaseTable()

    def _live_keys_with_prefix(self, prefix: str) -> list[str]:
        """호출하는 메서드가 락을 잡고 있다. 만료된 키는 지우고 목록에서 뺀다."""
        now = self._now_ms()
        live = []
        for entry in self._table.items():
            if not entry.key.startswith(prefix):
                continue
            if entry.expire_at_ms is not None and now >= entry.expire_at_ms:
                self._log(del_record(entry.key))
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
