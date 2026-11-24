"""净重批次服务层测试。

覆盖：跨日配票、唯一命中/箱号/毛重皮重校验、本批与已入库批次的票号复用、
清单换版（预检与落库前两条路径）、幂等重放、同批号改内容冲突、缺列 422。
"""

from __future__ import annotations

import pytest

from app.netbatch import (
    NetBatchConflict,
    NetBatchService,
    NetBatchValidationError,
    TicketPairInput,
    TicketRef,
)
from app.publisher import publish
from conftest import utc, write_net_parquet

GROSS_DAY = "2026-11-23"
TARE_DAY = "2026-11-24"


def _pair(g_id: str, t_id: str, g_day: str = GROSS_DAY, t_day: str = TARE_DAY):
    return TicketPairInput(
        gross=TicketRef(g_day, g_id), tare=TicketRef(t_day, t_id)
    )


def _publish_tickets(settings):
    """发布一份跨两个 UTC 日的毛/皮票清单（v1）。"""
    write_net_parquet(
        settings.input_dir / "gross.parquet",
        [
            ("G-1", utc("2026-11-23 08:00:00"), "CNTU-1", "gross", 20000),
            ("G-2", utc("2026-11-23 09:00:00"), "CNTU-2", "gross", 18000),
            ("G-3", utc("2026-11-23 22:00:00"), "CNTU-3", "gross", 15000),
            ("G-BAD", utc("2026-11-23 23:00:00"), "CNTU-9", "gross", 100),
        ],
    )
    write_net_parquet(
        settings.input_dir / "tare.parquet",
        [
            ("T-1", utc("2026-11-24 01:00:00"), "CNTU-1", "tare", 2000),
            ("T-2", utc("2026-11-24 02:00:00"), "CNTU-2", "tare", 3000),
            ("T-3", utc("2026-11-24 03:00:00"), "CNTU-3", "tare", 5000),
            ("T-WRONG", utc("2026-11-24 04:00:00"), "CNTU-OTHER", "tare", 1000),
        ],
    )
    result = publish(["gross.parquet", "tare.parquet"], settings)
    return result.manifest.version


def _service(settings):
    return NetBatchService(settings)


# ------------------------------------------------------------------ 主流程
def test_cross_day_pair_settles_net_weight(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    batch, replayed = svc.submit(
        "NB-001", version, [_pair("G-1", "T-1"), _pair("G-2", "T-2")]
    )
    assert replayed is False
    assert batch.manifest_version == version
    assert len(batch.pairs) == 2
    p1 = batch.pairs[0]
    assert p1.net_weight_kg == 18000  # 20000 - 2000
    assert p1.container_no == "CNTU-1"
    # 原票定位：毛票来自 23 日原档，皮票来自 24 日原档
    assert p1.gross.date == GROSS_DAY and p1.gross.ticket_id == "G-1"
    assert p1.gross.source_file == "gross.parquet"
    assert p1.gross.row_index == 0
    assert p1.tare.date == TARE_DAY and p1.tare.ticket_id == "T-1"
    assert p1.tare.source_file == "tare.parquet"
    assert p1.tare.row_index == 0
    # 已持久化，可按批号取回
    assert svc.get_batch("NB-001").batch_id == "NB-001"


def test_persisted_file_is_json_and_survives_new_service(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    svc.submit("NB-001", version, [_pair("G-1", "T-1")])
    data_file = settings.output_dir / "net_batches" / "batches.json"
    assert data_file.is_file()
    # 全新服务实例仍能读到
    again = NetBatchService(settings).get_batch("NB-001")
    assert again is not None and again.pairs[0].net_weight_kg == 18000


# ------------------------------------------------------------------ 票定位
def test_ticket_not_found_and_ambiguous_are_rejected(settings):
    version = _publish_tickets(settings)
    # 造一张同日同票号的重复票（第二个文件）
    write_net_parquet(
        settings.input_dir / "gross_dup.parquet",
        [("G-1", utc("2026-11-23 10:00:00"), "CNTU-1", "gross", 20001)],
    )
    publish(["gross.parquet", "tare.parquet", "gross_dup.parquet"], settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version + 1, [_pair("G-1", "T-1")])
    assert any("唯一命中" in r for r in ei.value.reasons)

    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-Y", version + 1, [_pair("NOPE", "T-1")])
    assert any("未命中" in r for r in ei.value.reasons)


def test_unknown_day_returns_reason(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [_pair("G-1", "T-1", t_day="2030-01-01")])
    assert any("2030-01-01" in r and "没有任何票文件" in r for r in ei.value.reasons)


# ------------------------------------------------------------------ 票对规则
def test_container_mismatch_and_negative_net_weight(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [_pair("G-1", "T-WRONG")])
    assert any("箱号不一致" in r for r in ei.value.reasons)

    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-Y", version, [_pair("G-BAD", "T-1")])
    assert any("毛重" in r and "低于皮重" in r for r in ei.value.reasons)
    # 整批退回：没有任何批号落库
    assert svc.get_batch("NB-X") is None and svc.get_batch("NB-Y") is None


def test_wrong_ticket_type_rejected(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    # 毛票位置传了真实存在但票种为 tare 的票（T-1 在 24 日）
    bad_pair = TicketPairInput(
        gross=TicketRef(TARE_DAY, "T-1"), tare=TicketRef(TARE_DAY, "T-2")
    )
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [bad_pair])
    assert any("票种" in r for r in ei.value.reasons)


def test_reuse_within_same_request_rejected(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit(
            "NB-X",
            version,
            [_pair("G-1", "T-1"), _pair("G-2", "T-1")],  # T-1 用两次
        )
    assert any("本批只能使用一次" in r for r in ei.value.reasons)


def test_reuse_against_stored_batch_rejected(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    svc.submit("NB-001", version, [_pair("G-1", "T-1")])
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-002", version, [_pair("G-2", "T-1")])  # T-1 已被占用
    assert any("已在已入库批次 NB-001" in r for r in ei.value.reasons)
    assert svc.get_batch("NB-002") is None  # 整批退回


def test_multiple_reasons_returned_together(settings):
    version = _publish_tickets(settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit(
            "NB-X",
            version,
            [_pair("G-BAD", "T-WRONG"), _pair("NOPE", "MISSING")],
        )
    joined = " | ".join(ei.value.reasons)
    assert "箱号不一致" in joined
    assert "低于皮重" in joined
    assert "未命中" in joined


# ------------------------------------------------------------------ 换版
def test_stale_manifest_version_fast_fail(settings):
    version = _publish_tickets(settings)  # v1
    # 再发布一次产生 v2
    publish(["gross.parquet", "tare.parquet"], settings)
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [_pair("G-1", "T-1")])  # 仍报 v1
    assert any("v1" in r and "v2" in r for r in ei.value.reasons)


def test_manifest_swap_during_read_is_caught_at_commit(settings, monkeypatch):
    v1 = _publish_tickets(settings)
    svc = _service(settings)
    # 模拟读票期间发布换版：锁内重新读取 current.json 时已是 v2
    real = NetBatchService.current_manifest

    def swapped(self):
        m = real(self)
        return dataclasses_replace_version(m, v1 + 1)

    monkeypatch.setattr(NetBatchService, "current_manifest", swapped)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", v1, [_pair("G-1", "T-1")])
    assert any("清单版号不一致" in r and "v2" in r for r in ei.value.reasons)
    assert svc.get_batch("NB-X") is None  # 整批退回


def dataclasses_replace_version(manifest, new_version):
    import dataclasses

    return dataclasses.replace(manifest, version=new_version)


# ------------------------------------------------------------------ 幂等
def test_replay_original_request_returns_original_result_even_after_republish(
    settings,
):
    v1 = _publish_tickets(settings)
    svc = _service(settings)
    first, replayed = svc.submit("NB-001", v1, [_pair("G-1", "T-1")])
    assert replayed is False
    first_created = first.created_at

    # 发布换版到 v2
    publish(["gross.parquet", "tare.parquet"], settings)
    # 原请求重传（仍带 v1 版号）：返回原结果，replayed=True
    again, replayed = svc.submit("NB-001", v1, [_pair("G-1", "T-1")])
    assert replayed is True
    assert again.created_at == first_created
    assert again.manifest_version == v1
    assert again.pairs[0].net_weight_kg == 18000

    # 同批号改内容 => 冲突
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-001", v1, [_pair("G-2", "T-2")])
    assert any("不一致" in r for r in ei.value.reasons)


# ------------------------------------------------------------------ 其他
def test_no_manifest_at_all(settings):
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", 1, [_pair("G-1", "T-1")])
    assert any("尚无已发布清单" in r for r in ei.value.reasons)


def test_null_weight_and_container_are_rejected_without_crash(settings):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("weigh_time", pa.timestamp("us")),
            pa.field("container_no", pa.string()),
            pa.field("ticket_type", pa.string()),
            pa.field("weight_kg", pa.int64()),
        ]
    )
    pq.write_table(
        pa.table(
            {
                "ticket_id": ["G-N", "T-N"],
                "weigh_time": [
                    utc("2026-11-23 08:00:00"),
                    utc("2026-11-24 08:00:00"),
                ],
                "container_no": [None, "C1"],
                "ticket_type": ["gross", "tare"],
                "weight_kg": [None, 2000],
            },
            schema=schema,
        ),
        settings.input_dir / "nulls.parquet",
    )
    version = publish(["nulls.parquet"], settings).manifest.version
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [_pair("G-N", "T-N")])
    joined = " | ".join(ei.value.reasons)
    assert "箱号列为空" in joined
    assert "空重量" in joined


def test_duplicate_ticket_id_in_two_day_files_is_ambiguous(settings):
    # 同 UTC 日两个文件出现同票号 => 不唯一
    write_net_parquet(
        settings.input_dir / "f1.parquet",
        [("DUP", utc("2026-11-23 08:00:00"), "C1", "gross", 20000)],
    )
    write_net_parquet(
        settings.input_dir / "f2.parquet",
        [("DUP", utc("2026-11-23 09:00:00"), "C1", "gross", 20001)],
    )
    write_net_parquet(
        settings.input_dir / "t.parquet",
        [("T-1", utc("2026-11-24 01:00:00"), "C1", "tare", 2000)],
    )
    version = publish(["f1.parquet", "f2.parquet", "t.parquet"],
                      settings).manifest.version
    svc = _service(settings)
    with pytest.raises(NetBatchConflict) as ei:
        svc.submit("NB-X", version, [_pair("DUP", "T-1")])
    assert any("命中 2 张票" in r for r in ei.value.reasons)


def test_concurrent_submissions_serialize_and_never_overwrite(settings):
    import threading

    version = _publish_tickets(settings)
    errors: list[Exception] = []
    barrier = threading.Barrier(4)

    def worker(batch_id: str, gross_id: str, tare_id: str):
        svc = NetBatchService(settings)  # 每个线程独立实例/缓存，共享同一落盘文件
        barrier.wait()  # 尽量同时进入，压 flock 串行化
        try:
            svc.submit(batch_id, version, [_pair(gross_id, tare_id)])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=("NB-C1", "G-1", "T-1")),
        threading.Thread(target=worker, args=("NB-C2", "G-2", "T-2")),
        threading.Thread(target=worker, args=("NB-C3", "G-3", "T-3")),
        # 与 NB-C1 抢同一对票：必须因票号复用失败，而不是覆盖 NB-C1
        threading.Thread(target=worker, args=("NB-C4", "G-1", "T-1")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 1
    assert isinstance(errors[0], NetBatchConflict)
    stored = NetBatchService(settings).net_store.load_all()
    # C1 与 C4 抢同一对票：恰好一个入库、另一个冲突，且 C2/C3 都在
    assert len(stored) == 3
    assert {"NB-C2", "NB-C3"} <= set(stored)
    assert {"NB-C1", "NB-C4"} & set(stored)
    assert not ({"NB-C1", "NB-C4"} <= set(stored))
    # 落盘 JSON 仍可完整解析，无并发写损坏
    assert all(b.pairs for b in stored.values())
    # 冲突原因指明占用批号
    assert any("已在已入库批次" in r for r in errors[0].reasons)


def test_missing_required_column_is_validation_error(settings):
    # 发布一份缺 ticket_type/weight_kg 的旧结构文件
    import pyarrow as pa
    import pyarrow.parquet as pq

    old_schema = pa.schema(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("weigh_time", pa.timestamp("us")),
            pa.field("container_no", pa.string()),
            pa.field("gross_weight_kg", pa.int64()),
        ]
    )
    pq.write_table(
        pa.table(
            {
                "ticket_id": ["G-1"],
                "weigh_time": [utc("2026-11-23 08:00:00")],
                "container_no": ["CNTU-1"],
                "gross_weight_kg": [20000],
            },
            schema=old_schema,
        ),
        settings.input_dir / "old.parquet",
    )
    version = publish(["old.parquet"], settings).manifest.version
    svc = _service(settings)
    with pytest.raises(NetBatchValidationError) as ei:
        svc.submit("NB-X", version, [_pair("G-1", "G-1")])
    assert any("缺少净重结算必需列" in r for r in ei.value.reasons)
