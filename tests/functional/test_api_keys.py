import pytest


def err(resp):
    body = resp.json()
    assert set(body) >= {"error", "message"}
    return body["error"]


def test_health_get_returns_200(client):
    assert client.get("/health").status_code == 200


# --- SET / GET ---

def test_put_then_get_returns_value_and_no_ttl(client):
    r = client.put("/keys/a", json={"value": "1"})
    assert (r.status_code, r.json()) == (200, {"ok": True})
    r = client.get("/keys/a")
    assert (r.status_code, r.json()) == (200, {"value": "1", "ttl": -1})


def test_put_overwrite_returns_new_value(client):
    client.put("/keys/a", json={"value": "1"})
    client.put("/keys/a", json={"value": "2"})
    assert client.get("/keys/a").json()["value"] == "2"


def test_put_empty_string_value_returns_empty_not_404(client):
    client.put("/keys/a", json={"value": ""})
    r = client.get("/keys/a")
    assert (r.status_code, r.json()["value"]) == (200, "")


def test_get_missing_key_returns_404_error_format(client):
    r = client.get("/keys/nope")
    assert r.status_code == 404
    assert err(r) == "KEY_NOT_FOUND"


def test_put_with_ttl_get_returns_remaining_seconds(client, clock):
    client.put("/keys/a", json={"value": "1", "ttl": 60})
    assert client.get("/keys/a").json()["ttl"] == 60
    clock.advance(18_500)
    assert client.get("/keys/a").json()["ttl"] == 42  # 41.5초 남음 -> 올림


def test_get_after_ttl_passed_returns_404(client, clock):
    client.put("/keys/a", json={"value": "1", "ttl": 10})
    clock.advance(10_000)
    assert client.get("/keys/a").status_code == 404


def test_put_without_ttl_overwrite_removes_old_ttl(client):
    client.put("/keys/a", json={"value": "1", "ttl": 60})
    client.put("/keys/a", json={"value": "2"})
    assert client.get("/keys/a").json()["ttl"] == -1


def test_put_null_ttl_means_no_ttl(client):
    client.put("/keys/a", json={"value": "1", "ttl": None})
    assert client.get("/keys/a").json()["ttl"] == -1


@pytest.mark.parametrize("ttl", [0, -1, 1.5, "abc", "10", True, [], {}])
def test_put_invalid_ttl_returns_400_invalid_ttl_and_stores_nothing(client, ttl):
    r = client.put("/keys/a", json={"value": "1", "ttl": ttl})
    assert r.status_code == 400
    assert err(r) == "INVALID_TTL"
    assert client.get("/keys/a").status_code == 404


# --- 입력 검증 ---

def test_put_missing_value_returns_400(client):
    r = client.put("/keys/a", json={})
    assert r.status_code == 400
    assert err(r) == "INVALID_VALUE"


def test_put_non_string_value_returns_400(client):
    assert client.put("/keys/a", json={"value": 123}).status_code == 400


def test_put_value_over_1mb_returns_413(client):
    r = client.put("/keys/a", json={"value": "x" * (1024 * 1024 + 1)})
    assert r.status_code == 413
    assert err(r) == "VALUE_TOO_LARGE"


def test_put_value_exactly_1mb_returns_200(client):
    assert client.put("/keys/a", json={"value": "x" * (1024 * 1024)}).status_code == 200


def test_put_key_over_256_bytes_returns_400_invalid_key(client):
    r = client.put("/keys/" + "x" * 257, json={"value": "1"})
    assert r.status_code == 400
    assert err(r) == "INVALID_KEY"


def test_put_key_exactly_256_bytes_returns_200(client):
    assert client.put("/keys/" + "x" * 256, json={"value": "1"}).status_code == 200


def test_put_hangul_key_over_256_bytes_checked_after_decoding(client):
    assert client.put("/keys/" + "한" * 85, json={"value": "1"}).status_code == 200
    r = client.put("/keys/" + "한" * 86, json={"value": "1"})
    assert (r.status_code, err(r)) == (400, "INVALID_KEY")


@pytest.mark.parametrize("raw", ["{bad", "[1,2]", '"str"', "123"])
def test_put_malformed_body_returns_400_invalid_body(client, raw):
    r = client.put("/keys/a", content=raw, headers={"content-type": "application/json"})
    assert r.status_code == 400
    assert err(r) == "INVALID_BODY"


def test_put_capacity_exceeded_returns_507(clock):
    from fastapi.testclient import TestClient

    from mini_redis.api.app import create_app
    from mini_redis.store import Store

    c = TestClient(create_app(Store(clock=clock, max_keys=1)))
    c.put("/keys/a", json={"value": "1"})
    r = c.put("/keys/b", json={"value": "1"})
    assert r.status_code == 507
    assert err(r) == "CAPACITY_EXCEEDED"


# --- 키 인코딩 ---

@pytest.mark.parametrize(
    "encoded, key",
    [
        ("a%2Fb", "a/b"),
        ("a%20b", "a b"),
        ("%ED%95%9C%EA%B8%80", "한글"),
        ("100%25", "100%"),
        ("a%2Fb%2Fc", "a/b/c"),
        ("user%3A1", "user:1"),
        ("exists", "exists"),
        ("ttl", "ttl"),
        ("a%3Fb%23c", "a?b#c"),
    ],
)
def test_key_percent_encoding_roundtrips_through_all_key_endpoints(client, store, encoded, key):
    assert client.put(f"/keys/{encoded}", json={"value": "v"}).status_code == 200
    assert store.get(key).value == "v"  # Store에는 디코딩된 키로 들어간다
    assert client.get(f"/keys/{encoded}").json()["value"] == "v"
    assert client.get(f"/keys/{encoded}/exists").json() == {"exists": True}
    assert client.delete(f"/keys/{encoded}").json() == {"deleted": True}


def test_key_with_encoded_slash_is_not_split_into_subcommand(client):
    client.put("/keys/a%2Fexists", json={"value": "v"})
    assert client.get("/keys/a%2Fexists").json()["value"] == "v"
    assert client.get("/keys/a").status_code == 404


def test_key_invalid_utf8_percent_sequence_returns_400(client):
    r = client.get("/keys/%FF%FE")
    assert (r.status_code, err(r)) == (400, "INVALID_KEY")


# --- DEL / EXISTS ---

def test_delete_existing_returns_true_then_get_404(client):
    client.put("/keys/a", json={"value": "1"})
    assert client.delete("/keys/a").json() == {"deleted": True}
    assert client.get("/keys/a").status_code == 404


def test_delete_missing_returns_false_not_error(client, store):
    r = client.delete("/keys/nope")
    assert (r.status_code, r.json()) == (200, {"deleted": False})
    assert store.size() == 0


def test_exists_existing_missing_and_expired(client, clock):
    client.put("/keys/a", json={"value": "1", "ttl": 5})
    assert client.get("/keys/a/exists").json() == {"exists": True}
    assert client.get("/keys/b/exists").json() == {"exists": False}
    clock.advance(5000)
    assert client.get("/keys/a/exists").json() == {"exists": False}


# --- INCR ---

def test_incr_missing_key_returns_one(client):
    r = client.post("/keys/n/incr")
    assert (r.status_code, r.json()) == (200, {"value": 1})


def test_incr_with_amount_and_negative(client):
    client.post("/keys/n/incr", json={"amount": 10})
    assert client.post("/keys/n/incr", json={"amount": -3}).json() == {"value": 7}


def test_incr_keeps_ttl(client):
    client.put("/keys/n", json={"value": "1", "ttl": 30})
    client.post("/keys/n/incr")
    assert client.get("/keys/n").json() == {"value": "2", "ttl": 30}


def test_incr_non_integer_value_returns_409_not_integer(client):
    client.put("/keys/a", json={"value": "abc"})
    r = client.post("/keys/a/incr")
    assert (r.status_code, err(r)) == (409, "NOT_INTEGER")


@pytest.mark.parametrize("amount", [1.5, "1", True])
def test_incr_invalid_amount_returns_400(client, amount):
    r = client.post("/keys/n/incr", json={"amount": amount})
    assert (r.status_code, err(r)) == (400, "INVALID_VALUE")


# --- EXPIRE / TTL ---

def test_ttl_missing_key_returns_minus_2(client):
    r = client.get("/keys/nope/ttl")
    assert (r.status_code, r.json()) == (200, {"ttl": -2})


def test_ttl_key_without_ttl_returns_minus_1(client):
    client.put("/keys/a", json={"value": "1"})
    assert client.get("/keys/a/ttl").json() == {"ttl": -1}


def test_ttl_matches_get_remaining_seconds(client, clock):
    client.put("/keys/a", json={"value": "1", "ttl": 60})
    clock.advance(18_500)  # 41.5초 남음 -> 올림
    assert client.get("/keys/a/ttl").json() == {"ttl": 42}
    assert client.get("/keys/a").json()["ttl"] == 42


def test_expire_existing_key_returns_updated_true_and_expires_later(client, clock):
    client.put("/keys/a", json={"value": "1"})
    r = client.post("/keys/a/expire", json={"ttl": 30})
    assert (r.status_code, r.json()) == (200, {"updated": True})
    assert client.get("/keys/a/ttl").json() == {"ttl": 30}
    clock.advance(30_000)
    assert client.get("/keys/a").status_code == 404


def test_expire_missing_key_returns_updated_false_and_creates_nothing(client, store):
    r = client.post("/keys/nope/expire", json={"ttl": 30})
    assert (r.status_code, r.json()) == (200, {"updated": False})
    assert store.size() == 0


@pytest.mark.parametrize("ttl", [0, -5])
def test_expire_zero_or_negative_ttl_deletes_key(client, ttl):
    client.put("/keys/a", json={"value": "1"})
    assert client.post("/keys/a/expire", json={"ttl": ttl}).json() == {"updated": True}
    assert client.get("/keys/a").status_code == 404


@pytest.mark.parametrize("body", [{"ttl": 1.5}, {"ttl": "abc"}, {"ttl": "10"}, {"ttl": True}, {"ttl": None}, {}])
def test_expire_invalid_or_missing_ttl_returns_400_invalid_ttl_and_keeps_key(client, body):
    client.put("/keys/a", json={"value": "1"})
    r = client.post("/keys/a/expire", json=body)
    assert (r.status_code, err(r)) == (400, "INVALID_TTL")
    assert client.get("/keys/a/ttl").json() == {"ttl": -1}


# --- 아직 구현 전인 명령은 조용히 무시하지 않고 거절 ---


@pytest.mark.parametrize("url", ["/keys/a?lease=1", "/keys/a?ns=user", "/keys/a?v=7"])
def test_get_with_lease_or_namespace_returns_501(client, url):
    assert client.get(url).status_code == 501


def test_put_with_lease_token_or_namespace_returns_501_and_stores_nothing(client, store):
    assert client.put("/keys/a", json={"value": "1", "lease": "tok"}).status_code == 501
    assert client.put("/keys/a?ns=user", json={"value": "1", "ttl": 5}).status_code == 501
    assert store.size() == 0


# --- 공통 오류 형식 ---

def test_unknown_route_returns_404_in_error_format(client):
    r = client.get("/nope")
    assert (r.status_code, err(r)) == (404, "NOT_FOUND")


def test_wrong_method_returns_405_in_error_format(client):
    r = client.patch("/keys/a")
    assert (r.status_code, err(r)) == (405, "METHOD_NOT_ALLOWED")


def test_unexpected_exception_returns_500_in_error_format(client, store, monkeypatch):
    monkeypatch.setattr(store, "get", lambda key: 1 / 0)
    r = client.get("/keys/a")
    assert (r.status_code, err(r)) == (500, "INTERNAL_ERROR")
