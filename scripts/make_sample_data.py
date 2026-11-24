#!/usr/bin/env python3
"""生成演示用的码头过磅明细 Parquet（数据是虚构的）。

放到 scripts/ 下，与被处理的数据目录分离；输出目录由参数指定：
    python scripts/make_sample_data.py /workspace/data_in

生成文件（按日数据集，旧结构：仅毛重列）：
  batch_a_sameday.parquet      同日记录（页脚即可证明，走快路径复用原档）
  batch_b_mixed.parquet        两个 UTC 日混存（跨日拆写）
  batch_c_null.parquet         含空过磅时间（发布失败演示）
  batch_d_nostats.parquet      write_statistics=False（降级读明细，确认同日复用）
  batch_e_tz.parquet           带时区时间戳，含 UTC 跨日边界记录

净重批次演示文件（新结构：票种 ticket_type + 统一重量列 weight_kg）：
  nett_gross_2026-11-23.parquet  23 日毛票
  nett_tare_2026-11-24.parquet   24 日皮票（与 23 日毛票跨日配对）
  nett_conflicts.parquet         演示用问题票（票种反/箱号不同/毛重低于皮重）
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("container_no", pa.string()),
        pa.field("gross_weight_kg", pa.int64()),
    ]
)

# 净重结算结构：毛票/皮票同表，用 ticket_type 区分，重量统一放 weight_kg
NET_SCHEMA = pa.schema(
    [
        pa.field("ticket_id", pa.string()),
        pa.field("weigh_time", pa.timestamp("us")),
        pa.field("container_no", pa.string()),
        pa.field("ticket_type", pa.string()),
        pa.field("weight_kg", pa.int64()),
    ]
)


def _ts(s: str):
    import datetime as dt

    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=dt.timezone.utc
    )


def main(out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1) 同日：2026-11-23
    rows_a = [
        ("T-1001", _ts("2026-11-23 01:10:00"), "CNTU1000001", 21400),
        ("T-1002", _ts("2026-11-23 08:25:00"), "CNTU1000002", 18900),
        ("T-1003", _ts("2026-11-23 22:40:00"), "CNTU1000003", 24600),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_a] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_a_sameday.parquet",
        compression="snappy",
    )

    # 2) 跨日混存：23 日与 24 日
    rows_b = [
        ("T-2001", _ts("2026-11-23 23:20:00"), "CNTU2000001", 20100),
        ("T-2002", _ts("2026-11-24 00:05:00"), "CNTU2000002", 19800),
        ("T-2003", _ts("2026-11-24 09:15:00"), "CNTU2000003", 22300),
        ("T-2004", _ts("2026-11-24 18:30:00"), "CNTU2000004", 17750),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_b] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_b_mixed.parquet",
        compression="snappy",
    )

    # 3) 含空时间
    rows_c = [
        ("T-3001", _ts("2026-11-24 07:00:00"), "CNTU3000001", 20000),
        ("T-3002", None, "CNTU3000002", 19500),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_c] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_c_null.parquet",
        compression="snappy",
    )

    # 4) 关闭页脚统计：同日但页脚证明不了，必须读明细
    rows_d = [
        ("T-4001", _ts("2026-11-24 03:00:00"), "CNTU4000001", 18000),
        ("T-4002", _ts("2026-11-24 12:00:00"), "CNTU4000002", 19000),
    ]
    pq.write_table(
        pa.table({f.name: [r[i] for r in rows_d] for i, f in enumerate(SCHEMA)}, schema=SCHEMA),
        out / "batch_d_nostats.parquet",
        compression="snappy",
        write_statistics=False,
    )

    # 5) 带时区：UTC+8 的 11-24 07:30 对应 UTC 11-23 23:30，验证按 UTC 取日
    tz_schema = pa.schema(
        [
            pa.field("ticket_id", pa.string()),
            pa.field("weigh_time", pa.timestamp("us", tz="Asia/Shanghai")),
            pa.field("container_no", pa.string()),
            pa.field("gross_weight_kg", pa.int64()),
        ]
    )
    rows_e = [
        ("T-5001", _ts("2026-11-23 23:30:00"), "CNTU5000001", 20500),
        ("T-5002", _ts("2026-11-24 00:30:00"), "CNTU5000002", 21200),
    ]
    # 上面构造的是 UTC 瞬间；写入 tz=Asia/Shanghai 的列时 Arrow 保留同一瞬间
    table_e = pa.table(
        {
            "ticket_id": [r[0] for r in rows_e],
            "weigh_time": pa.array([r[1] for r in rows_e], type=pa.timestamp("us", "UTC")),
            "container_no": [r[2] for r in rows_e],
            "gross_weight_kg": [r[3] for r in rows_e],
        }
    ).cast(tz_schema)
    pq.write_table(table_e, out / "batch_e_tz.parquet", compression="snappy")

    # 6) 净重批次演示：23 日毛票
    gross_rows = [
        ("NG-1001", _ts("2026-11-23 06:30:00"), "NETU1000001", "gross", 22500),
        ("NG-1002", _ts("2026-11-23 14:10:00"), "NETU1000002", "gross", 19800),
        ("NG-1003", _ts("2026-11-23 21:50:00"), "NETU1000003", "gross", 24100),
    ]
    pq.write_table(
        pa.table(
            {f.name: [r[i] for r in gross_rows] for i, f in enumerate(NET_SCHEMA)},
            schema=NET_SCHEMA,
        ),
        out / "nett_gross_2026-11-23.parquet",
        compression="snappy",
    )

    # 7) 24 日皮票：与前一日毛票跨日配对（两次过磅跨 UTC 日）
    tare_rows = [
        ("NT-2001", _ts("2026-11-24 00:40:00"), "NETU1000001", "tare", 2300),
        ("NT-2002", _ts("2026-11-24 07:25:00"), "NETU1000002", "tare", 2100),
        ("NT-2003", _ts("2026-11-24 15:05:00"), "NETU1000003", "tare", 2600),
    ]
    pq.write_table(
        pa.table(
            {f.name: [r[i] for r in tare_rows] for i, f in enumerate(NET_SCHEMA)},
            schema=NET_SCHEMA,
        ),
        out / "nett_tare_2026-11-24.parquet",
        compression="snappy",
    )

    # 8) 问题票（整批退回演示，按需发布）：
    #    NG-9001/NT-9001 箱号不同；NG-9002 毛重低于皮重；NT-9003 票种写成 gross
    conflict_rows = [
        ("NG-9001", _ts("2026-11-23 10:00:00"), "NETU9000001", "gross", 20000),
        ("NT-9001", _ts("2026-11-24 10:00:00"), "NETU9000002", "tare", 2000),
        ("NG-9002", _ts("2026-11-23 11:00:00"), "NETU9000003", "gross", 500),
        ("NT-9002", _ts("2026-11-24 11:00:00"), "NETU9000003", "tare", 2000),
        ("NT-9003", _ts("2026-11-24 12:00:00"), "NETU9000004", "gross", 1800),
    ]
    pq.write_table(
        pa.table(
            {f.name: [r[i] for r in conflict_rows]
             for i, f in enumerate(NET_SCHEMA)},
            schema=NET_SCHEMA,
        ),
        out / "nett_conflicts.parquet",
        compression="snappy",
    )

    for p in sorted(out.glob("*.parquet")):
        print(f"wrote {p} ({pq.ParquetFile(p).metadata.num_rows} rows)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        raise SystemExit(1)
    main(sys.argv[1])
