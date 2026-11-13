"""HTTP 라우트. 입력을 검증하고 Store를 부른 뒤 JSON으로 돌려주기만 한다.

핸들러는 동기(def)로 둬서 Store의 전역 락을 기다리는 동안 이벤트 루프가 막히지 않게 한다.
"""
import json
import math
import re
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Query, Request

from ..errors import InvalidBody, InvalidKey, InvalidTtl, InvalidValue
from ..store import Store

router = APIRouter()


def get_store(request: Request) -> Store:
    return request.app.state.store


async def json_body(request: Request) -> dict:
    """본문이 없으면 {}, 있으면 JSON 객체여야 한다."""
    raw = await request.body()
    if not raw.strip():
        return {}
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise InvalidBody("본문이 올바른 JSON이 아닙니다")
    if not isinstance(body, dict):
        raise InvalidBody("본문은 JSON 객체여야 합니다")
    return body


def decode_key(raw: str) -> str:
    """경로의 키를 퍼센트 디코딩한다 (한 번만). 길이 검사는 Store가 디코딩 후에 한다."""
    try:
        return unquote(raw, errors="strict")
    except UnicodeDecodeError:
        raise InvalidKey("키의 퍼센트 인코딩이 올바른 UTF-8이 아닙니다")


def _remaining_seconds(store: Store, expire_at_ms: int | None) -> int:
    """남은 TTL(초, 올림). 무기한이면 -1."""
    if expire_at_ms is None:
        return -1
    return max(1, math.ceil((expire_at_ms - store.now_ms()) / 1000))


def _parse_ttl(value) -> int:
    """TTL은 양의 정수 초. 1.5, "abc", true, 0 이하는 400 INVALID_TTL."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidTtl("ttl은 양의 정수(초)여야 합니다")
    return value


@router.get("/health")
def health():
    return {"status": "ok"}


# --- 키 하나 ---

@router.put("/keys/{key}")
def set_key(
    key: str,
    ns: str | None = Query(None),
    v: int | None = Query(None),
    body: dict = Depends(json_body),
    store: Store = Depends(get_store),
):
    """ns를 붙이면 ttl과 함께 lease나 v(GET에서 받은 version)가 필요하다 (Store.set)."""
    key = decode_key(key)
    value = body.get("value")
    expire_at_ms = None
    if body.get("ttl") is not None:
        expire_at_ms = store.now_ms() + _parse_ttl(body["ttl"]) * 1000
    store.set(key, value, expire_at_ms, lease=body.get("lease"), ns=ns, v=v)
    return {"ok": True}


@router.get("/keys/{key}")
def get_key(
    key: str,
    lease: bool = Query(False),
    ns: str | None = Query(None),
    store: Store = Depends(get_store),
):
    """?lease=1이면 miss의 404에 lease 토큰이나 retry_after_ms가 붙고, 만료 직전 히트 하나에는 조기 갱신용 lease가 붙는다.

    키 명령에 ?ns=를 붙이면 Store가 `ns:v{버전}:key`로 바꿔 처리한다 (쿼리 값은 서버가 이미 디코딩했다).
    GET은 200과 404 모두에 현재 버전 `version`이 붙는다 (Store.get).
    """
    item = store.get(decode_key(key), lease=lease, ns=ns)
    body = {"value": item.value, "ttl": _remaining_seconds(store, item.expire_at_ms)}
    if item.version is not None:
        body["version"] = item.version
    if item.lease is not None:
        body["lease"] = item.lease  # 값은 유효하지만 만료가 가까워 이 요청이 미리 갱신을 맡았다 (XFetch)
    return body


@router.delete("/keys/{key}")
def delete_key(key: str, ns: str | None = Query(None), store: Store = Depends(get_store)):
    return {"deleted": store.delete(decode_key(key), ns=ns)}


@router.get("/keys/{key}/exists")
def exists_key(key: str, ns: str | None = Query(None), store: Store = Depends(get_store)):
    return {"exists": store.exists(decode_key(key), ns=ns)}


@router.post("/keys/{key}/incr")
def incr_key(key: str, ns: str | None = Query(None), body: dict = Depends(json_body), store: Store = Depends(get_store)):
    return {"value": store.incr(decode_key(key), body.get("amount", 1), ns=ns)}


@router.post("/keys/{key}/expire")
def expire_key(key: str, ns: str | None = Query(None), body: dict = Depends(json_body), store: Store = Depends(get_store)):
    # 0 이하는 즉시 삭제라 _parse_ttl을 쓰지 않는다. 정수 검사(ttl 없음 포함)는 Store가 INVALID_TTL로 한다
    return {"updated": store.expire(decode_key(key), body.get("ttl"), ns=ns)}


@router.get("/keys/{key}/ttl")
def ttl_key(key: str, ns: str | None = Query(None), store: Store = Depends(get_store)):
    return {"ttl": store.ttl(decode_key(key), ns=ns)}


# --- 키 묶음 ---

@router.get("/keys")
def list_keys(prefix: str = Query(""), store: Store = Depends(get_store)):
    return {"keys": store.keys(prefix)}


@router.delete("/keys")
def delete_keys(prefix: str = Query(""), batch: str | None = Query(None), store: Store = Depends(get_store)):
    """batch(버킷 개수)가 있으면 나눠서 지운다 (#26, 원자적이지 않음). 없으면 예전 그대로 한 번에."""
    if batch is None:
        return {"deleted": store.delete_prefix(prefix)}
    if not re.fullmatch(r"[0-9]+", batch):  # int()는 "3_0"·" 4 "·"٣"·"+4"도 받아 줘서 직접 거른다
        raise InvalidValue("batch는 1 이상의 정수여야 합니다")
    buckets = int(batch)
    return {"deleted": store.delete_prefix_batched(prefix, buckets)}


@router.post("/namespaces/{ns}/invalidate")
def invalidate_namespace(ns: str, store: Store = Depends(get_store)):
    return {"version": store.invalidate(decode_key(ns))}


@router.post("/admin/flush")
def flush(body: dict = Depends(json_body), store: Store = Depends(get_store)):
    store.flush(confirm=body.get("confirm"))
    return {"ok": True}
