"""HTTP 라우트. 입력을 검증하고 Store를 부른 뒤 JSON으로 돌려주기만 한다.

핸들러는 동기(def)로 둬서 Store의 전역 락을 기다리는 동안 이벤트 루프가 막히지 않게 한다.
"""
import json
import math
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Query, Request

from ..errors import (
    InvalidBody,
    InvalidKey,
    InvalidTtl,
    NotImplementedYet,
)
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


def _reject_unsupported(request: Request, body: dict | None = None) -> None:
    """lease(#10)와 네임스페이스(#11)가 구현되기 전까지는 조용히 무시하지 않고 거절한다."""
    q = request.query_params
    if "ns" in q or "v" in q or "lease" in q or (body and "lease" in body):
        raise NotImplementedYet("lease와 네임스페이스(ns, v)는 아직 지원하지 않습니다")


@router.get("/health")
def health():
    return {"status": "ok"}


# --- 키 하나 ---

@router.put("/keys/{key}")
def set_key(key: str, request: Request, body: dict = Depends(json_body), store: Store = Depends(get_store)):
    _reject_unsupported(request, body)
    key = decode_key(key)
    value = body.get("value")
    expire_at_ms = None
    if body.get("ttl") is not None:
        expire_at_ms = store.now_ms() + _parse_ttl(body["ttl"]) * 1000
    store.set(key, value, expire_at_ms)
    return {"ok": True}


@router.get("/keys/{key}")
def get_key(key: str, request: Request, store: Store = Depends(get_store)):
    _reject_unsupported(request)
    item = store.get(decode_key(key))
    return {"value": item.value, "ttl": _remaining_seconds(store, item.expire_at_ms)}


@router.delete("/keys/{key}")
def delete_key(key: str, store: Store = Depends(get_store)):
    return {"deleted": store.delete(decode_key(key))}


@router.get("/keys/{key}/exists")
def exists_key(key: str, store: Store = Depends(get_store)):
    return {"exists": store.exists(decode_key(key))}


@router.post("/keys/{key}/incr")
def incr_key(key: str, body: dict = Depends(json_body), store: Store = Depends(get_store)):
    return {"value": store.incr(decode_key(key), body.get("amount", 1))}


@router.post("/keys/{key}/expire")
def expire_key(key: str, body: dict = Depends(json_body), store: Store = Depends(get_store)):
    # 0 이하는 즉시 삭제라 _parse_ttl을 쓰지 않는다. 정수 검사(ttl 없음 포함)는 Store가 INVALID_TTL로 한다
    return {"updated": store.expire(decode_key(key), body.get("ttl"))}


@router.get("/keys/{key}/ttl")
def ttl_key(key: str, store: Store = Depends(get_store)):
    return {"ttl": store.ttl(decode_key(key))}


# --- 키 묶음 ---

@router.get("/keys")
def list_keys(prefix: str = Query(""), store: Store = Depends(get_store)):
    return {"keys": store.keys(prefix)}


@router.delete("/keys")
def delete_keys(prefix: str = Query(""), store: Store = Depends(get_store)):
    return {"deleted": store.delete_prefix(prefix)}


@router.post("/admin/flush")
def flush(body: dict = Depends(json_body), store: Store = Depends(get_store)):
    store.flush(confirm=body.get("confirm"))
    return {"ok": True}
