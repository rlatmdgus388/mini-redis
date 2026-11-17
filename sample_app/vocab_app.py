"""단어장 앱 시연 서버예요.

"단어장 메뉴에 들어갈 때마다 DB에서 목록을 다시 읽던" 상황을 그대로 보여 줘요.
느린 DB(200ms)를 흉내 내고, 그 앞에 Mini Redis를 cache-aside로 붙여요.
단어를 추가하거나 지우면 DB에 쓴 뒤 캐시 키를 지워서, 다음 진입에 최신 목록이 보이게 해요.
"""

import json
import logging
import os
import threading
import time
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
) -> FastAPI:
    """redis 클라이언트와 sleep은 밖에서 끼워 넣어요(테스트는 가짜 sleep을 써요)."""
    app = FastAPI(title="단어장 시연")
    lock = threading.Lock()
    db: dict = {"words": [], "next_id": 1, "reads": 0}
    stats = {"hits": 0, "misses": 0}
    mode = {"use_cache": True}

    def seed() -> None:
        db["words"] = [
            {"id": i + 1, "word": w, "meaning": m, "memorized": done} for i, (w, m, done) in enumerate(_SEED)
        ]
        db["next_id"] = len(_SEED) + 1
        db["reads"] = 0
        stats["hits"] = stats["misses"] = 0
        mode["use_cache"] = True

    seed()

    def error(message: str, code: str, status: int) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": code, "message": message})

    def read_db() -> list:
        """느린 DB를 읽는 척해요. 읽은 횟수를 세요."""
        sleep(delay_ms / 1000)
        with lock:
            db["reads"] += 1
            return [dict(w) for w in db["words"]]

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
        with lock:
            word = {"id": db["next_id"], "word": body.word.strip(), "meaning": body.meaning.strip(), "memorized": False}
            db["next_id"] += 1
            db["words"].append(word)
        failed = invalidate()
        return failed or {"word": word, "invalidated": True}

    @app.delete("/api/words/{word_id}")
    def delete_word(word_id: int):
        with lock:
            before = len(db["words"])
            db["words"] = [w for w in db["words"] if w["id"] != word_id]
            removed = len(db["words"]) != before
        if not removed:
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        failed = invalidate()
        return failed or {"deleted": True, "invalidated": True}

    @app.patch("/api/words/{word_id}")
    def set_memorized(word_id: int, body: MemorizedBody):
        """암기 완료/미완료를 바꿔요. 목록 내용이 달라지니 캐시 키도 지워요."""
        with lock:
            word = next((w for w in db["words"] if w["id"] == word_id), None)
            if word is not None:
                word["memorized"] = body.memorized
        if word is None:
            return error(f"단어 {word_id}번이 없어요", "WORD_NOT_FOUND", 404)
        failed = invalidate()
        return failed or {"word": dict(word), "invalidated": True}

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
                "db_reads": db["reads"],
                "words": len(db["words"]),
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
        with lock:
            seed()
        try:
            redis.delete("/keys", params={"prefix": _PREFIX})
        except httpx.HTTPError as exc:
            return error(f"cache reset failed: {exc}", "CACHE_ERROR", 502)
        return {"ok": True}

    return app


app = create_vocab_app(httpx.Client(base_url=os.environ.get("MINI_REDIS_URL", "http://127.0.0.1:8000")))
