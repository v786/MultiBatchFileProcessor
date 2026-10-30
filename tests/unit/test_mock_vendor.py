from starlette.testclient import TestClient


def test_idempotency_outage_and_stats(vendor):
    c = TestClient(vendor.app)
    r1 = c.post("/records", json={"a": 1}, headers={"Idempotency-Key": "k1"})
    r2 = c.post("/records", json={"a": 1}, headers={"Idempotency-Key": "k1"})
    assert r1.status_code == 200 and r1.json() == r2.json()
    assert c.get("/records", params={"externalId": "k1"}).json()["id"] == r1.json()["id"]
    c.put("/_control", json={"outage": True})
    assert c.post("/records", json={}, headers={"Idempotency-Key": "k2"}).status_code == 503
    stats = c.get("/_control/stats").json()
    assert stats["duplicates_seen"] == 1 and stats["accepted"] == 1 and stats["duplicate_records"] == 0
    assert stats["peak_inflight"] <= 20 and stats["rejected_429"] == 0
