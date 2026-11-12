"""세션 장면: 로그인 -> 활동 시 EXPIRE로 TTL 연장 -> 로그아웃 -> 조회 404."""


def test_session_login_then_logout_returns_404(client):
    client.put("/keys/session%3Aabc", json={"value": "user42", "ttl": 1800})
    assert client.get("/keys/session%3Aabc").json()["value"] == "user42"
    assert client.delete("/keys/session%3Aabc").json() == {"deleted": True}
    assert client.get("/keys/session%3Aabc").status_code == 404


def test_session_expires_after_ttl_without_activity(client, clock):
    client.put("/keys/session%3Aabc", json={"value": "user42", "ttl": 1800})
    clock.advance(1_800_000)
    assert client.get("/keys/session%3Aabc").status_code == 404


def test_session_activity_by_rewriting_with_ttl_extends_life(client, clock):
    client.put("/keys/session%3Aabc", json={"value": "user42", "ttl": 1800})
    clock.advance(1_000_000)
    client.put("/keys/session%3Aabc", json={"value": "user42", "ttl": 1800})
    clock.advance(1_000_000)
    assert client.get("/keys/session%3Aabc").status_code == 200


def test_session_activity_by_expire_extends_life_then_logout_returns_404(client, clock):
    client.put("/keys/session%3Aabc", json={"value": "user42", "ttl": 1800})
    clock.advance(1_000_000)
    assert client.post("/keys/session%3Aabc/expire", json={"ttl": 1800}).json() == {"updated": True}
    clock.advance(1_000_000)  # 처음 TTL(1800초)은 지났지만 EXPIRE로 다시 잰 덕에 산다
    assert client.get("/keys/session%3Aabc").json()["value"] == "user42"
    assert client.delete("/keys/session%3Aabc").json() == {"deleted": True}
    assert client.get("/keys/session%3Aabc").status_code == 404


def test_app_lifespan_starts_and_stops_expiry_thread(store):
    from fastapi.testclient import TestClient

    from mini_redis.api.app import create_app

    with TestClient(create_app(store)) as c:
        assert store._expiry_stop is not None
        assert c.get("/health").status_code == 200
    assert store._expiry_stop is None
