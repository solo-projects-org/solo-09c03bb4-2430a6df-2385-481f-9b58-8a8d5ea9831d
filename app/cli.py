"""命令行入口：发布 / 查看当前清单 / 按日读取 / 净重批次。

用法：
  python -m app.cli publish rel1.parquet rel2.parquet
  python -m app.cli manifest
  python -m app.cli read 2026-11-23
  python -m app.cli net-batch NB-001 1 \
      '2026-11-23:NG-1001=2026-11-24:NT-2001' \
      '2026-11-23:NG-1002=2026-11-24:NT-2002'
"""

from __future__ import annotations

import argparse
import json
import sys

from .config import Settings
from .netbatch import (
    NetBatchError,
    NetBatchService,
    TicketPairInput,
    TicketRef,
)
from .publisher import PublishError, publish
from .service import DatasetService


def _parse_pair(text: str) -> TicketPairInput:
    # 形如 2026-11-23:NG-1001=2026-11-24:NT-2001（毛票=皮票）
    try:
        gross_s, tare_s = text.split("=", 1)
        g_day, g_id = gross_s.split(":", 1)
        t_day, t_id = tare_s.split(":", 1)
    except ValueError:
        raise SystemExit(
            f"票对格式错误: {text!r}，应为 毛票UTC日:毛票号=皮票UTC日:皮票号"
        )
    return TicketPairInput(
        gross=TicketRef(g_day, g_id), tare=TicketRef(t_day, t_id)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="weigh-dataset")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pub = sub.add_parser("publish", help="按输入清单发布新版本")
    p_pub.add_argument("files", nargs="+")

    sub.add_parser("manifest", help="打印当前生效清单")

    p_read = sub.add_parser("read", help="按 UTC 日期读取并打印 JSON 行")
    p_read.add_argument("day")
    p_read.add_argument("--limit", type=int, default=1000)

    p_net = sub.add_parser("net-batch", help="提交净重批次（毛票日:号=皮票日:号 …）")
    p_net.add_argument("batch_id")
    p_net.add_argument("manifest_version", type=int)
    p_net.add_argument("pairs", nargs="+")
    p_get = sub.add_parser("net-batch-get", help="按批号查询已入库净重批次")
    p_get.add_argument("batch_id")

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "publish":
        try:
            result = publish(args.files, settings)
        except PublishError as exc:
            print(f"发布失败，已保留上一版清单: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(result.manifest.to_dict(), ensure_ascii=False, indent=2))
        print("处理方式:", file=sys.stderr)
        for o in result.outcomes:
            print(f"  - {o.input_file}: {o.mode} -> {o.days}", file=sys.stderr)
        return 0

    service = DatasetService(settings)
    manifest = service.current_manifest()
    if args.cmd == "manifest":
        if manifest is None:
            print("尚无已发布版本")
        else:
            print(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "read":
        if manifest is None:
            print("尚无已发布版本", file=sys.stderr)
            return 1
        table = service.read_date(args.day)
        if table is None or table.num_rows == 0:
            print("[]")
            return 0
        for row in table.slice(0, args.limit).to_pylist():
            print(json.dumps(row, ensure_ascii=False, default=str))
        return 0

    net_service = NetBatchService(settings)
    if args.cmd == "net-batch":
        pairs = [_parse_pair(p) for p in args.pairs]
        try:
            batch, replayed = net_service.submit(
                args.batch_id, args.manifest_version, pairs
            )
        except NetBatchError as exc:
            print("净重批次被整批退回:", file=sys.stderr)
            for reason in exc.reasons:
                print(f"  - {reason}", file=sys.stderr)
            return 2
        payload = batch.to_dict()
        payload["replayed"] = replayed
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "net-batch-get":
        batch = net_service.get_batch(args.batch_id)
        if batch is None:
            print(f"批号 {args.batch_id} 不存在", file=sys.stderr)
            return 1
        print(json.dumps(batch.to_dict(), ensure_ascii=False, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
