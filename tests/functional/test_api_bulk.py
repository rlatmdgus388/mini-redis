import pytest


@pytest.fixture
def seeded(client):
    for k in ["user:1", "user:2", "post:1", "user"]:
        client.put(f"/keys/{k.replace(':', '%3A')}", json={"value": "v"})
    return client


def test_list_keys_prefix_returns_sorted_matches(seeded):
    r = seeded.get("/keys", params={"prefix": "user:"})
    assert (r.status_code, r.json()) == (200, {"keys": ["user:1", "user:2"]})


def test_list_keys_without_prefix_returns_all(seeded):
    assert len(seeded.get("/keys").json()["keys"]) == 4


def test_list_keys_excludes_expired(seeded, clock):
    seeded.put("/keys/tmp", json={"value": "v", "ttl": 5})
    clock.advance(5000)
    assert "tmp" not in seeded.get("/keys").json()["keys"]


def test_list_keys_hangul_prefix_roundtrips(client):
    client.put("/keys/%ED%95%9C%EA%B8%80%3A1", json={"value": "v"})
    assert client.get("/keys", params={"prefix": "한글"}).json() == {"keys": ["한글:1"]}


def test_delete_prefix_returns_count_and_keeps_others(seeded):
    r = seeded.delete("/keys", params={"prefix": "user:"})
    assert (r.status_code, r.json()) == (200, {"deleted": 2})
    assert seeded.get("/keys").json()["keys"] == ["post:1", "user"]


def test_delete_prefix_empty_returns_400_empty_prefix(seeded):
    for url in ["/keys", "/keys?prefix="]:
        r = seeded.delete(url)
        assert (r.status_code, r.json()["error"]) == (400, "EMPTY_PREFIX")
    assert len(seeded.get("/keys").json()["keys"]) == 4


def test_flush_with_confirm_empties_store(seeded, store):
    r = seeded.post("/admin/flush", json={"confirm": True})
    assert (r.status_code, r.json()) == (200, {"ok": True})
    assert store.size() == 0


@pytest.mark.parametrize("body", [None, {}, {"confirm": False}, {"confirm": "true"}, {"confirm": 1}])
def test_flush_without_confirm_returns_400_confirm_required(seeded, store, body):
    r = seeded.post("/admin/flush", json=body) if body is not None else seeded.post("/admin/flush")
    assert (r.status_code, r.json()["error"]) == (400, "CONFIRM_REQUIRED")
    assert store.size() == 4


def test_flush_path_does_not_collide_with_key_named_all(client):
    client.put("/keys/all", json={"value": "v"})
    assert client.delete("/keys/all").json() == {"deleted": True}
    assert client.post("/admin/flush", json={"confirm": True}).status_code == 200


def test_delete_prefix_without_batch_param_keeps_todays_response(seeded):
    r = seeded.delete("/keys", params={"prefix": "user:"})
    assert (r.status_code, r.json()) == (200, {"deleted": 2})


def test_delete_prefix_with_batch_param_returns_same_deleted_shape(seeded):
    r = seeded.delete("/keys", params={"prefix": "user:", "batch": 4})
    assert (r.status_code, r.json()) == (200, {"deleted": 2})
    assert seeded.get("/keys").json()["keys"] == ["post:1", "user"]


@pytest.mark.parametrize("batch", ["0", "-3", "x", "1.5", "3_0", " 4 ", "٣", "+4"])
def test_delete_prefix_with_bad_batch_param_returns_400_invalid_value(seeded, batch):
    r = seeded.delete("/keys", params={"prefix": "user:", "batch": batch})
    assert (r.status_code, r.json()["error"]) == (400, "INVALID_VALUE")
    assert len(seeded.get("/keys").json()["keys"]) == 4
