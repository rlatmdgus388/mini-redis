"""단어장 앱 시연 서버예요.

"단어장 메뉴에 들어갈 때마다 DB에서 목록을 다시 읽던" 상황을 그대로 보여 줘요.
DB는 둘 중 하나예요.
  - memory:   메모리에 든 단어 목록을 읽을 때 200ms를 기다려요(느린 DB 흉내, PostgreSQL이 없을 때).
  - postgres: 진짜 PostgreSQL이에요. 목록을 읽을 때 복습 기록 200만 건을 집계해 단어별 복습 횟수를 같이 계산해요.
그 앞에 Mini Redis를 cache-aside로 붙여요.
단어를 추가하거나 지우면 DB에 쓴 뒤 캐시 키를 지워서, 다음 진입에 최신 목록이 보이게 해요.
"""

import json
import logging
import os
import threading
import queue
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

logger = logging.getLogger(__name__)

_PAGE = Path(__file__).with_name("vocab.html")
_PREFIX = "vocab:"
_USER = "demo"
_KEY = _PREFIX + _USER
_SEED = [
    ("management", "[명사] 경영, 관리", True),
    ("located", "[동사] 위치하다", True),
    ("workforce", "[명사] 노동력, 인력", True),
    ("entire", "[형용사] 전체의, 완전한", False),
    ("impact", "[명사] 영향, 충격", True),
]


class MemoryWords:
    """메모리에 든 단어 목록. 읽을 때 delay_ms만큼 기다려서 느린 DB를 흉내 내요."""

    kind = "memory"

    def __init__(self, delay_ms: int, sleep: Callable[[float], None]):
        self._delay_ms, self._sleep = delay_ms, sleep
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._words = [
                {"id": i + 1, "word": w, "meaning": m, "memorized": done} for i, (w, m, done) in enumerate(_SEED)
            ]
            self._next_id = len(_SEED) + 1
            self.reads = 0

    def read(self) -> list:
        self._sleep(self._delay_ms / 1000)
        with self._lock:
            self.reads += 1
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


STUDY_LOGS = 2_000_000
_LIST_SQL = (
    "SELECT w.id, w.word, w.meaning, w.memorized, coalesce(c.n, 0) AS reviews "
    "FROM vocab_words w "
    "LEFT JOIN (SELECT word_id, count(*) AS n FROM vocab_study_logs GROUP BY word_id) c ON c.word_id = w.id "
    "ORDER BY w.id"
)


class PgWords:
    """진짜 PostgreSQL. 목록을 읽을 때 복습 기록 200만 건을 집계해 단어별 복습 횟수를 계산해요(인덱스 없음)."""

    kind = "postgres"

    def __init__(self, dsn: str, pool_size: int = 4):
        import psycopg  # 메모리 DB만 쓸 때는 psycopg가 없어도 되게 여기서 가져와요

        self._pool: "queue.Queue" = queue.Queue()
        for _ in range(pool_size):
            self._pool.put(psycopg.connect(dsn, autocommit=True))
        self._lock = threading.Lock()
        self.reads = 0
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
            c.execute(
                "CREATE TABLE IF NOT EXISTS vocab_study_logs "
                "(id serial PRIMARY KEY, word_id int NOT NULL, studied_at timestamptz NOT NULL DEFAULT now())"
            )
            if c.execute("SELECT NOT EXISTS (SELECT 1 FROM vocab_study_logs)").fetchone()[0]:
                c.execute(
                    "INSERT INTO vocab_study_logs (word_id) SELECT 1 + floor(random() * %s)::int "
                    "FROM generate_series(1, %s)",
                    (len(_SEED), STUDY_LOGS),
                )
                c.execute("ANALYZE vocab_study_logs")

    def reset(self) -> None:
        with self._conn() as c:
            c.execute("TRUNCATE vocab_words")
            for i, (w, m, done) in enumerate(_SEED):
                c.execute("INSERT INTO vocab_words VALUES (%s, %s, %s, %s)", (i + 1, w, m, done))
            c.execute("SELECT setval('vocab_words_id_seq', %s)", (len(_SEED),))
        with self._lock:
            self.reads = 0

    def read(self) -> list:
        with self._conn() as c:
            cur = c.execute(_LIST_SQL)
            names = [d.name for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        with self._lock:
            self.reads += 1
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


class WordBody(BaseModel):
    word: str = Field(min_length=1, max_length=50)
    meaning: str = Field(default="", max_length=100)


class MemorizedBody(BaseModel):
    memorized: bool


class CacheModeBody(BaseModel):
    enabled: bool


def create_vocab_app(
    redis: httpx.Client,
    ttl_s: int = config.SAMPLE_CACHE_TTL_S,
    delay_ms: int = config.ORIGIN_DELAY_MS,
    sleep: Callable[[float], None] = time.sleep,
    words=None,
) -> FastAPI:
    """redis 클라이언트와 DB(words)는 밖에서 끼워 넣어요. words가 없으면 sleep으로 느린 DB를 흉내 내요(테스트는 가짜 sleep을 써요)."""
    app = FastAPI(title="단어장 시연")
    lock = threading.Lock()
    db = words if words is not None else MemoryWords(delay_ms, sleep)
    stats = {"hits": 0, "misses": 0}
    mode = {"use_cache": True}

    def error(message: str, code: str, status: int) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": code, "message": message})

    def read_db() -> list:
        """DB에서 목록을 읽어요. 읽은 횟수를 세요."""
        return db.read()

    def invalidate() -> Optional[JSONResponse]:
        """쓰기 뒤에 캐시 키를 지워요. 실패하면 낡은 목록이 TTL까지 남을 수 있어서 알려요."""
        try:
            res = redis.delete("/keys/" + quote(_KEY, safe=""))
        except httpx.HTTPError as exc:
            return error(f"DB에는 반영됐지만 캐시를 못 지웠어요: {exc}", "CACHE_ERROR", 502)
        if res.status_code != 200:
            return error(f"DB에는 반영됐지만 캐시 삭제가 {res.status_code}예요", "CACHE_ERROR", 502)
        return None

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _PAGE.read_text(encoding="utf-8")

    @app.get("/api/words")
    def list_words():
        """단어장 메뉴에 들어갈 때 부르는 목록 조회. cache는 HIT / MISS / BYPASS예요."""
        started = time.perf_counter()
        path = "/keys/" + quote(_KEY, safe="")

        def done(words: list, label: str) -> dict:
            ms = round((time.perf_counter() - started) * 1000, 1)
            return {"words": words, "cache": label, "ms": ms}

        if not mode["use_cache"]:
            words = read_db()
            with lock:
                stats["misses"] += 1
            return done(words, "BYPASS")

        try:
            cached = redis.get(path)
        except httpx.HTTPError as exc:
            return error(f"cache request failed: {exc}", "CACHE_ERROR", 502)
        if cached.status_code == 200:
            try:
                words = json.loads(cached.json()["value"])
            except (ValueError, KeyError, TypeError):
                return error("cache returned an invalid value", "CACHE_ERROR", 502)
            with lock:
                stats["hits"] += 1
            return done(words, "HIT")
        if cached.status_code != 404:
            return error(f"cache returned {cached.status_code}", "CACHE_ERROR", 502)

        words = read_db()
        try:
            stored = redis.put(path, json={"value": json.dumps(words, ensure_ascii=False), "ttl": ttl_s})
            if stored.status_code != 200:
                logger.warning("cache put returned %s", stored.status_code)
        except httpx.HTTPError as exc:
            logger.warning("cache put failed: %s", exc)
        with lock:
            stats["misses"] += 1
        return done(words, "MISS")

    @app.post("/api/words")
    def add_word(body: WordBody):
        word = db.add(body.word.strip(), body.meaning.strip())
        failed = invalidate()
        return failed or {"word": word, "invalidated": True}

    @app.delete("/api/words/{word_id}")
    def delete_word(word_id: int):
        if not db.delete(word_id):
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        failed = invalidate()
        return failed or {"deleted": True, "invalidated": True}

    @app.patch("/api/words/{word_id}")
    def set_memorized(word_id: int, body: MemorizedBody):
        """암기 완료/미완료를 바꿔요. 목록 내용이 달라지니 캐시 키도 지워요."""
        word = db.set_memorized(word_id, body.memorized)
        if word is None:
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        failed = invalidate()
        return failed or {"word": word, "invalidated": True}

    @app.get("/api/state")
    def state():
        """캐시에 든 키와 남은 TTL, 집계, 원본(DB)을 읽은 횟수를 모아서 돌려줘요."""
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
                "db": db.kind,
                "words": db.count(),
                "use_cache": mode["use_cache"],
                "keys": keys,
            }

    @app.post("/api/cache-mode")
    def cache_mode(body: CacheModeBody):
        """캐시를 끄면 매번 DB를 읽어요(캐시를 쓰기 전 방식과 비교하는 용도)."""
        with lock:
            mode["use_cache"] = body.enabled
        return {"use_cache": body.enabled}

    @app.post("/api/reset")
    def reset():
        """단어장, 집계, 캐시 키를 모두 처음 상태로 되돌려요."""
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
        return create_vocab_app(redis, words=PgWords(os.environ.get("PG_DSN", "postgresql://postgres:bench@127.0.0.1:5433/bench")))
    return create_vocab_app(redis)


app = _build()
