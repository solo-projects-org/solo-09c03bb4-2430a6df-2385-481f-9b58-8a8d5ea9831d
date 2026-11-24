"""净重批次 API 测试：201/200 幂等重放、409 冲突、422 请求校验、404 查询。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app import api
from app.netbatch import NetBatchService
from app.publisher import publish
from conftest import utc, write_net_parquet

GROSS_DAY = "2026-11-23"
TARE_DAY = "2026-11-24"


def _client(settings):
    api.get_settings = lambda: settings  # type: ignore[method-assign]
    api.get_service = lambda: api.DatasetService(settings)  # type: ignore[method-assign]
    api.get_net_batch_service = lambda: NetBatchService(settings)  # type: ignore[method-assign]
    return TestClient(api.app)


def _setup(settings):
    write_net_parquet(
        settings.input_dir / "gross.parquet",
        [
            ("G-1", utc("2026-11-23 08:00:00"), "CNTU-1", "gross", 20000),
            ("G-2", utc("2026-11-23 09:00:00"), "CNTU-2", "gross", 18000),
        ],
    )
    write_net_parquet(
        settings.input_dir / "tare.parquet",
        [
            ("T-1", utc("2026-11-24 01:00:00"), "CNTU-1", "tare", 2000),
            ("T-2", utc("2026-11-24 02:00:00"), "CNTU-2", "tare", 3000),
        ],
    )
    return publish(["gross.parquet", "tare.parquet"], settings).manifest.version


def _payload(batch_id="NB-001", version=1, pairs=None):
    pairs = pairs or [
        {"gross": {"date": GROSS_DAY, "ticket_id": "G-1"},
         "tare": {"date": TARE_DAY, "ticket_id": "T-1"}},
    ]
    return {"batch_id": batch_id, "manifest_version": version, "pairs": pairs}


def test_submit_cross_day_batch_201_with_locations(settings):
    v = _setup(settings)
    client = _client(settings)
    r = client.post("/net-batches", json=_payload(version=v))
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["replayed"] is False
    assert body["pair_count"] == 1
    pair = body["pairs"][0]
    assert pair["net_weight_kg"] == 18000
    assert pair["container_no"] == "CNTU-1"
    assert pair["gross"] == {
        "date": GROSS_DAY,
        "ticket_id": "G-1",
        "source": "original",
        "source_file": "gross.parquet",
        "path": "gross.parquet",
        "row_index": 0,
    }
    assert pair["tare"]["ticket_id"] == "T-1"
    assert pair["tare"]["date"] == TARE_DAY


def test_replay_returns_200_and_original_result_after_republish(settings):
    v = _setup(settings)
    client = _client(settings)
    r1 = client.post("/net-batches", json=_payload(version=v))
    assert r1.status_code == 201
    # 发布换版
    publish(["gross.parquet", "tare.parquet"], settings)
    # 原请求（仍带 v1）原样重传
    r2 = client.post("/net-batches", json=_payload(version=v))
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True
    assert r2.json()["manifest_version"] == v
    assert r2.json()["pairs"] == r1.json()["pairs"]
    # GET 取回
    r3 = client.get("/net-batches/NB-001")
    assert r3.status_code == 200
    assert r3.json()["replayed"] is False


def test_same_batch_id_changed_content_is_409(settings):
    v = _setup(settings)
    client = _client(settings)
    assert client.post("/net-batches", json=_payload(version=v)).status_code == 201
    changed = _payload(
        version=v,
        pairs=[{"gross": {"date": GROSS_DAY, "ticket_id": "G-2"},
                "tare": {"date": TARE_DAY, "ticket_id": "T-2"}}],
    )
    r = client.post("/net-batches", json=changed)
    assert r.status_code == 409
    assert "不一致" in r.json()["detail"]["reasons"][0]


def test_stale_version_and_reused_ticket_are_409(settings):
    v = _setup(settings)
    client = _client(settings)
    # 换版后仍报旧版号
    publish(["gross.parquet", "tare.parquet"], settings)
    r = client.post("/net-batches", json=_payload(version=v))
    assert r.status_code == 409
    assert r.json()["detail"]["reasons"]

    # 回到 v2 正常入库一批
    ok = _payload(batch_id="NB-A", version=v + 1)
    assert client.post("/net-batches", json=ok).status_code == 201
    # 另一批复用 T-1 => 409 且整批退回
    dup = _payload(batch_id="NB-B", version=v + 1)
    r = client.post("/net-batches", json=dup)
    assert r.status_code == 409
    assert any("NB-A" in x for x in r.json()["detail"]["reasons"])
    assert client.get("/net-batches/NB-B").status_code == 404


def test_validation_errors_are_422(settings):
    _setup(settings)
    client = _client(settings)
    # 请求体问题：空 pairs、缺字段
    assert client.post("/net-batches", json={"batch_id": "x"}).status_code == 422
    assert client.post(
        "/net-batches",
        json={"batch_id": "x", "manifest_version": 1, "pairs": []},
    ).status_code == 422
    # 日期格式
    bad_date = _payload(version=1)
    bad_date["pairs"][0]["gross"]["date"] = "11/23/2026"
    assert client.post("/net-batches", json=bad_date).status_code == 422


def test_get_unknown_batch_404(settings):
    _setup(settings)
    client = _client(settings)
    assert client.get("/net-batches/NOPE").status_code == 404
