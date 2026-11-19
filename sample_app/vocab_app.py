"""단어장 앱 시연 서버예요.

두 가지를 서로 다른 방식으로 빠르게 해요.
  - 단어장 목록: 서버는 DB에서 그냥 읽어 줘요. 빠르게 하는 일은 브라우저(localStorage)가 맡아요(vocab.html).
  - 학습 통계(홈의 "오늘의 학습 현황"): 학습 기록을 집계해서 만드는 값이라 Mini Redis에 cache-aside로 캐싱해요.
    학습을 기록하면 DB에 쓴 뒤 통계 키를 지워서, 다음에 홈을 열 때 다시 집계해요.

DB는 둘 중 하나예요.
  - memory:   메모리에 든 기록을 집계해요. 통계를 읽을 때 delay_ms를 기다려 느린 DB를 흉내 내요(PostgreSQL이 없을 때).
  - postgres: 진짜 PostgreSQL이에요. 학습 기록(vocab_study_events)을 그때그때 집계해요.

DB_RTT_MS를 주면 "Redis는 앱 서버 가까이에, DB는 그보다 멀리 있다"고 가정하고 앱이 DB에 읽고 쓸 때마다 그만큼의 왕복 시간을 더해요
(PostgreSQL일 때만, 기본 0 = 더하지 않음). Redis 호출에는 더하지 않아요. 이것만 대기로 흉내 낸 값이에요.

"오늘"은 한국 시간(UTC+9) 기준이에요. memory는 주입한 시계(epoch ms)를, postgres는 DB의 now()를 써요.
"""

import json
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from mini_redis import config
from mini_redis.clock import system_clock_ms

logger = logging.getLogger(__name__)

_PAGE = Path(__file__).with_name("vocab.html")
_PREFIX = "vocab:"
_STATS_KEY = _PREFIX + "stats:demo"
_DAY_MS = 86_400_000
_KST_OFFSET_MS = 9 * 3_600_000
_SEED = [
    ("management", "[명사] 경영, 관리", True),
    ("located", "[동사] 위치하다", True),
    ("workforce", "[명사] 노동력, 인력", True),
    ("entire", "[형용사] 전체의, 완전한", False),
    ("impact", "[명사] 영향, 충격", True),
]
# 처음 상태의 학습 기록: (며칠 전, 그날 학습 횟수). 어제까지 4일 연속이라 "연속 학습 4일"에서 시작해요.
_HISTORY = [(1, 8), (2, 6), (3, 7), (4, 5)] + [(d, 4) for d in range(6, 31, 2)]
_STUDY_SECONDS = 8  # 시드 기록 한 건의 학습 시간(초)


def _day(epoch_ms: int) -> int:
    """epoch ms를 한국 시간 기준 날짜 번호로 바꿔요."""
    return (epoch_ms + _KST_OFFSET_MS) // _DAY_MS


def _streak(days: set, today: int) -> int:
    """오늘(없으면 어제)부터 거꾸로 이어진 학습일 수예요."""
    day = today if today in days else today - 1
    count = 0
    while day in days:
        count += 1
        day -= 1
    return count


class MemoryWords:
    """메모리에 든 단어와 학습 기록. 통계를 읽을 때 delay_ms만큼 기다려서 느린 DB를 흉내 내요."""

    kind = "memory"

    def __init__(self, delay_ms: int, sleep: Callable[[float], None], now_ms: Callable[[], int] = system_clock_ms):
        self._delay_ms, self._sleep, self._now_ms = delay_ms, sleep, now_ms
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._words = [
                {"id": i + 1, "word": w, "meaning": m, "memorized": done} for i, (w, m, done) in enumerate(_SEED)
            ]
            self._next_id = len(_SEED) + 1
            today = _day(self._now_ms())
            # 기록 한 건: (날짜 번호, 단어 번호, 학습 시간 초)
            self._events = [
                (today - ago, 1 + n % len(_SEED), _STUDY_SECONDS) for ago, times in _HISTORY for n in range(times)
            ]
            self.reads = 0
            self.list_reads = 0

    def words(self) -> list:
        with self._lock:
            self.list_reads += 1
            return [dict(w) for w in self._words]

    def add(self, word: str, meaning: str) -> dict:
        with self._lock:
            item = {"id": self._next_id, "word": word, "meaning": meaning, "memorized": False}
            self._next_id += 1
            self._words.append(item)
            return dict(item)

    def delete(self, word_id: int) -> bool:
        with self._lock:
            before = len(self._words)
            self._words = [w for w in self._words if w["id"] != word_id]
            return len(self._words) != before

    def set_memorized(self, word_id: int, memorized: bool) -> Optional[dict]:
        with self._lock:
            word = next((w for w in self._words if w["id"] == word_id), None)
            if word is None:
                return None
            word["memorized"] = memorized
            return dict(word)

    def count(self) -> int:
        with self._lock:
            return len(self._words)

    def study(self, word_id: int, seconds: int) -> bool:
        with self._lock:
            if not any(w["id"] == word_id for w in self._words):
                return False
            self._events.append((_day(self._now_ms()), word_id, seconds))
            return True

    def stats(self) -> dict:
        """오늘 학습 단어 수, 오늘 학습 시간(초), 연속 학습일. 읽을 때마다 기록을 처음부터 집계해요."""
        self._sleep(self._delay_ms / 1000)
        with self._lock:
            self.reads += 1
            today = _day(self._now_ms())
            todays = [e for e in self._events if e[0] == today]
            return {
                "words_today": len({e[1] for e in todays}),
                "seconds_today": sum(e[2] for e in todays),
                "streak_days": _streak({e[0] for e in self._events}, today),
            }


_KST = "'Asia/Seoul'"
_TODAY_SQL = (
    "SELECT count(DISTINCT word_id), coalesce(sum(seconds), 0) FROM vocab_study_events "
    f"WHERE (studied_at AT TIME ZONE {_KST})::date = (now() AT TIME ZONE {_KST})::date"
)
# 학습한 날짜들을 연속 구간으로 묶어(날짜 - 순번이 같으면 같은 구간), 오늘(없으면 어제)이 속한 구간의 길이를 센다.
_STREAK_SQL = (
    f"WITH d AS (SELECT DISTINCT (studied_at AT TIME ZONE {_KST})::date AS day FROM vocab_study_events), "
    "runs AS (SELECT day, day - (row_number() OVER (ORDER BY day))::int AS grp FROM d) "
    "SELECT count(*) FROM runs WHERE grp = ("
    f"SELECT grp FROM runs WHERE day >= (now() AT TIME ZONE {_KST})::date - 1 ORDER BY day DESC LIMIT 1)"
)


class PgWords:
    """진짜 PostgreSQL. 학습 통계는 학습 기록 테이블을 읽을 때마다 집계해요."""

    kind = "postgres"

    def __init__(self, dsn: str, pool_size: int = 4):
        import psycopg  # 메모리 DB만 쓸 때는 psycopg가 없어도 되게 여기서 가져와요

        self._pool: "queue.Queue" = queue.Queue()
        for _ in range(pool_size):
            self._pool.put(psycopg.connect(dsn, autocommit=True))
        self._lock = threading.Lock()
        self.reads = 0
        self.list_reads = 0
        self._ensure_schema()
        self.reset()

    @contextmanager
    def _conn(self):
        conn = self._pool.get()
        try:
            yield conn
        finally:
            self._pool.put(conn)

    def _ensure_schema(self) -> None:
        with self._conn() as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS vocab_words (id int PRIMARY KEY, word text NOT NULL, "
                "meaning text NOT NULL DEFAULT '', memorized boolean NOT NULL DEFAULT false)"
            )
            c.execute("CREATE SEQUENCE IF NOT EXISTS vocab_words_id_seq")
            c.execute("DROP TABLE IF EXISTS vocab_study_logs")  # 예전 시연이 만든 200만 건 복습 기록 테이블
            c.execute(
                "CREATE TABLE IF NOT EXISTS vocab_study_events "
                "(id serial PRIMARY KEY, word_id int NOT NULL, seconds int NOT NULL, "
                "studied_at timestamptz NOT NULL DEFAULT now())"
            )
            c.execute("CREATE INDEX IF NOT EXISTS vocab_study_events_at ON vocab_study_events (studied_at)")

    def reset(self) -> None:
        with self._conn() as c:
            c.execute("TRUNCATE vocab_words")
            for i, (w, m, done) in enumerate(_SEED):
                c.execute("INSERT INTO vocab_words VALUES (%s, %s, %s, %s)", (i + 1, w, m, done))
            c.execute("SELECT setval('vocab_words_id_seq', %s)", (len(_SEED),))
            c.execute("TRUNCATE vocab_study_events")
            for ago, times in _HISTORY:
                c.execute(
                    "INSERT INTO vocab_study_events (word_id, seconds, studied_at) "
                    "SELECT 1 + mod(n, %s), %s, now() - make_interval(days => %s) FROM generate_series(0, %s) n",
                    (len(_SEED), _STUDY_SECONDS, ago, times - 1),
                )
        with self._lock:
            self.reads = 0
            self.list_reads = 0

    def words(self) -> list:
        with self._conn() as c:
            cur = c.execute("SELECT id, word, meaning, memorized FROM vocab_words ORDER BY id")
            names = [d.name for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        with self._lock:
            self.list_reads += 1
        return rows

    def add(self, word: str, meaning: str) -> dict:
        with self._conn() as c:
            new_id = c.execute("SELECT nextval('vocab_words_id_seq')").fetchone()[0]
            c.execute("INSERT INTO vocab_words VALUES (%s, %s, %s, false)", (new_id, word, meaning))
        return {"id": new_id, "word": word, "meaning": meaning, "memorized": False}

    def delete(self, word_id: int) -> bool:
        with self._conn() as c:
            return c.execute("DELETE FROM vocab_words WHERE id = %s", (word_id,)).rowcount > 0

    def set_memorized(self, word_id: int, memorized: bool) -> Optional[dict]:
        with self._conn() as c:
            row = c.execute(
                "UPDATE vocab_words SET memorized = %s WHERE id = %s RETURNING id, word, meaning, memorized",
                (memorized, word_id),
            ).fetchone()
        return None if row is None else dict(zip(("id", "word", "meaning", "memorized"), row))

    def count(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT count(*) FROM vocab_words").fetchone()[0]

    def study(self, word_id: int, seconds: int) -> bool:
        with self._conn() as c:
            return (
                c.execute(
                    "INSERT INTO vocab_study_events (word_id, seconds) "
                    "SELECT %s, %s WHERE EXISTS (SELECT 1 FROM vocab_words WHERE id = %s)",
                    (word_id, seconds, word_id),
                ).rowcount
                > 0
            )

    def stats(self) -> dict:
        with self._conn() as c:
            words_today, seconds_today = c.execute(_TODAY_SQL).fetchone()
            streak = c.execute(_STREAK_SQL).fetchone()[0]
        with self._lock:
            self.reads += 1
        return {"words_today": words_today, "seconds_today": int(seconds_today), "streak_days": streak}


class RemoteDb:
    """DB(words)를 감싸서, 앱이 DB에 읽고 쓰는 호출마다 왕복 시간(rtt_ms)을 더해요.

    화면 상태 조회용 count와 초기화(reset)에는 더하지 않아요. 나머지(kind, reads 등)는 안쪽 DB 것을 그대로 보여줘요.
    """

    _SLOW = ("words", "add", "delete", "set_memorized", "study", "stats")

    def __init__(self, inner, rtt_ms: float, sleep: Callable[[float], None] = time.sleep):
        self._inner, self._sleep = inner, sleep
        self.rtt_ms = rtt_ms

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name not in self._SLOW:
            return attr

        def delayed(*args, **kwargs):
            self._sleep(self.rtt_ms / 1000)
            return attr(*args, **kwargs)

        return delayed


class WordBody(BaseModel):
    word: str = Field(min_length=1, max_length=50)
    meaning: str = Field(default="", max_length=100)


class MemorizedBody(BaseModel):
    memorized: bool


class StudyBody(BaseModel):
    word_id: int
    seconds: int = Field(default=0, ge=0, le=3600)


class CacheModeBody(BaseModel):
    enabled: bool


def create_vocab_app(
    redis: httpx.Client,
    ttl_s: int = config.SAMPLE_CACHE_TTL_S,
    delay_ms: int = config.ORIGIN_DELAY_MS,
    sleep: Callable[[float], None] = time.sleep,
    words=None,
    now_ms: Callable[[], int] = system_clock_ms,
) -> FastAPI:
    """redis 클라이언트와 DB(words)는 밖에서 끼워 넣어요. words가 없으면 메모리 DB를 써요(테스트는 가짜 sleep과 시계를 써요)."""
    app = FastAPI(title="단어장 시연")
    lock = threading.Lock()
    db = words if words is not None else MemoryWords(delay_ms, sleep, now_ms)
    stats = {"hits": 0, "misses": 0}
    mode = {"use_cache": True}

    def error(message: str, code: str, status: int) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": code, "message": message})

    def invalidate_stats() -> Optional[JSONResponse]:
        """학습을 기록한 뒤 통계 캐시 키를 지워요. 실패하면 낡은 통계가 TTL까지 남을 수 있어서 알려요."""
        try:
            res = redis.delete("/keys/" + quote(_STATS_KEY, safe=""))
        except httpx.HTTPError as exc:
            return error(f"DB에는 반영됐지만 캐시를 못 지웠어요: {exc}", "CACHE_ERROR", 502)
        if res.status_code != 200:
            return error(f"DB에는 반영됐지만 캐시 삭제가 {res.status_code}예요", "CACHE_ERROR", 502)
        return None

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _PAGE.read_text(encoding="utf-8")

    @app.get("/api/words")
    def list_words() -> dict:
        """단어 목록. Mini Redis를 거치지 않고 DB에서 읽어요(브라우저가 localStorage에 저장해 둬요)."""
        return {"words": db.words()}

    @app.get("/api/stats")
    def get_stats():
        """홈 화면의 학습 통계. cache는 HIT / MISS / BYPASS예요."""
        started = time.perf_counter()
        path = "/keys/" + quote(_STATS_KEY, safe="")

        def done(value: dict, label: str) -> dict:
            ms = round((time.perf_counter() - started) * 1000, 1)
            return {"stats": value, "cache": label, "ms": ms}

        if not mode["use_cache"]:
            value = db.stats()
            with lock:
                stats["misses"] += 1
            return done(value, "BYPASS")

        try:
            cached = redis.get(path)
        except httpx.HTTPError as exc:
            return error(f"cache request failed: {exc}", "CACHE_ERROR", 502)
        if cached.status_code == 200:
            try:
                value = json.loads(cached.json()["value"])
            except (ValueError, KeyError, TypeError):
                return error("cache returned an invalid value", "CACHE_ERROR", 502)
            with lock:
                stats["hits"] += 1
            return done(value, "HIT")
        if cached.status_code != 404:
            return error(f"cache returned {cached.status_code}", "CACHE_ERROR", 502)

        value = db.stats()
        try:
            stored = redis.put(path, json={"value": json.dumps(value), "ttl": ttl_s})
            if stored.status_code != 200:
                logger.warning("cache put returned %s", stored.status_code)
        except httpx.HTTPError as exc:
            logger.warning("cache put failed: %s", exc)
        with lock:
            stats["misses"] += 1
        return done(value, "MISS")

    @app.post("/api/study")
    def study(body: StudyBody):
        """단어 하나를 학습했다고 기록해요. DB에 쓴 뒤 통계 캐시 키를 지워요."""
        if not db.study(body.word_id, body.seconds):
            return error(f"단어 {body.word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        failed = invalidate_stats()
        return failed or {"studied": True, "invalidated": True}

    @app.post("/api/words")
    def add_word(body: WordBody):
        return {"word": db.add(body.word.strip(), body.meaning.strip())}

    @app.delete("/api/words/{word_id}")
    def delete_word(word_id: int):
        if not db.delete(word_id):
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        return {"deleted": True}

    @app.patch("/api/words/{word_id}")
    def set_memorized(word_id: int, body: MemorizedBody):
        """암기 완료/미완료를 바꿔요."""
        word = db.set_memorized(word_id, body.memorized)
        if word is None:
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        return {"word": word}

    @app.get("/api/state")
    def state():
        """캐시에 든 키와 남은 TTL, 통계 조회 집계, 통계를 DB에서 읽은 횟수와 목록을 DB에서 읽은 횟수를 모아서 돌려줘요."""
        try:
            names = redis.get("/keys", params={"prefix": _PREFIX}).json()["keys"]
            keys = []
            for name in sorted(names):
                ttl = redis.get("/keys/" + quote(name, safe="") + "/ttl").json().get("ttl", -2)
                if ttl != -2:  # 조회 사이에 만료된 키는 뺀다
                    keys.append({"key": name, "ttl": ttl})
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            return error(f"state lookup failed: {exc}", "STATE_ERROR", 502)
        with lock:
            return {
                "hits": stats["hits"],
                "misses": stats["misses"],
                "db_reads": db.reads,
                "list_reads": db.list_reads,
                "db": db.kind,
                "db_rtt_ms": getattr(db, "rtt_ms", 0),
                "words": db.count(),
                "use_cache": mode["use_cache"],
                "keys": keys,
            }

    @app.post("/api/cache-mode")
    def cache_mode(body: CacheModeBody):
        """통계 캐시를 끄면 매번 DB에서 집계해요(캐시를 쓰기 전 방식과 비교하는 용도)."""
        with lock:
            mode["use_cache"] = body.enabled
        return {"use_cache": body.enabled}

    @app.post("/api/reset")
    def reset():
        """단어장, 학습 기록, 집계, 캐시 키를 모두 처음 상태로 되돌려요."""
        db.reset()
        with lock:
            stats["hits"] = stats["misses"] = 0
            mode["use_cache"] = True
        try:
            redis.delete("/keys", params={"prefix": _PREFIX})
        except httpx.HTTPError as exc:
            return error(f"cache reset failed: {exc}", "CACHE_ERROR", 502)
        return {"ok": True}

    return app


def _build() -> FastAPI:
    redis = httpx.Client(base_url=os.environ.get("MINI_REDIS_URL", "http://127.0.0.1:8000"))
    if os.environ.get("VOCAB_DB") == "pg":
        words = PgWords(os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench"))
        rtt_ms = float(os.environ.get("DB_RTT_MS", "0"))
        return create_vocab_app(redis, words=RemoteDb(words, rtt_ms) if rtt_ms > 0 else words)
    return create_vocab_app(redis)


app = _build()
