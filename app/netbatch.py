"""净重批次结算。

业务规则（见 README「净重批次」）：

* 提交批号、核对过的清单版号和一组票对；每张票用「UTC 日 + 票号」定位。
* 每张票在当日必须唯一命中；票对两张票箱号相同、毛重不低于皮重。
* 同日同票号在本批与所有已入库批次中只能使用一次。
* 读票期间允许发布换版（本模块用独立锁，不阻塞 publish）；落库前重新读取
  current.json，清单已换版或复检出任一冲突 => 整批退回，不落任何数据。
* 同批号重传原请求：幂等返回原结果（即使清单已换版）；同批号改内容 => 冲突。

存储（output_dir 下）::

    net_batches/
      .net-batches.lock   # 净重批次落库互斥（与发布锁相互独立）
      batches.json        # 全部已入库批次，临时文件 + os.replace 原子替换
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import json
import os
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from .config import Settings
from .locking import FileLock, LockTimeout
from .manifest import DayFile, ManifestStore

NET_BATCHES_DIR = "net_batches"
NET_BATCHES_FILE = "batches.json"
NET_BATCHES_LOCK = ".net-batches.lock"


class NetBatchError(RuntimeError):
    """净重批次失败基类；HTTP 状态码由子类决定。"""

    status_code = 409

    def __init__(self, reasons: list[str]):
        self.reasons = reasons
        super().__init__("；".join(reasons))


class NetBatchValidationError(NetBatchError):
    """请求本身或票数据不满足校验（422）。"""

    status_code = 422


class NetBatchConflict(NetBatchError):
    """业务冲突 / 换版 / 锁超时（409），整批退回。"""

    status_code = 409


@dataclasses.dataclass(frozen=True)
class TicketRef:
    """票的外部定位：UTC 日 + 票号。"""

    date: str  # YYYY-MM-DD（UTC）
    ticket_id: str


@dataclasses.dataclass(frozen=True)
class TicketPairInput:
    gross: TicketRef
    tare: TicketRef


@dataclasses.dataclass(frozen=True)
class TicketLocation:
    """原票定位：命中行所在文件与行号。"""

    date: str
    ticket_id: str
    source: str  # original | split
    source_file: str
    path: str
    row_index: int


@dataclasses.dataclass(frozen=True)
class NetPairResult:
    gross: TicketLocation
    tare: TicketLocation
    manifest_version: int
    container_no: str
    net_weight_kg: float


@dataclasses.dataclass(frozen=True)
class NetBatch:
    batch_id: str
    manifest_version: int
    created_at: str
    request: dict  # 规范化后的原请求，用于幂等重放比对
    pairs: list[NetPairResult]

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "manifest_version": self.manifest_version,
            "created_at": self.created_at,
            "request": self.request,
            "pairs": [
                {
                    "gross": dataclasses.asdict(p.gross),
                    "tare": dataclasses.asdict(p.tare),
                    "manifest_version": p.manifest_version,
                    "container_no": p.container_no,
                    "net_weight_kg": p.net_weight_kg,
                }
                for p in self.pairs
            ],
        }

    @staticmethod
    def from_dict(data: dict) -> "NetBatch":
        pairs = [
            NetPairResult(
                gross=TicketLocation(**p["gross"]),
                tare=TicketLocation(**p["tare"]),
                manifest_version=int(p["manifest_version"]),
                container_no=p["container_no"],
                net_weight_kg=p["net_weight_kg"],
            )
            for p in data["pairs"]
        ]
        return NetBatch(
            batch_id=data["batch_id"],
            manifest_version=int(data["manifest_version"]),
            created_at=data["created_at"],
            request=data["request"],
            pairs=pairs,
        )


def canonical_request(
    batch_id: str, manifest_version: int, pairs: list[TicketPairInput]
) -> dict:
    """把请求规范化成可 JSON 序列化、字段顺序稳定的结构（幂等比对用）。"""
    return {
        "batch_id": batch_id,
        "manifest_version": manifest_version,
        "pairs": [
            {
                "gross": {"date": pr.gross.date, "ticket_id": pr.gross.ticket_id},
                "tare": {"date": pr.tare.date, "ticket_id": pr.tare.ticket_id},
            }
            for pr in pairs
        ],
    }


def _canonical_key(request: dict) -> str:
    return json.dumps(request, ensure_ascii=False, sort_keys=True)


class NetBatchStore:
    """batches.json 的原子读写与独立互斥锁。"""

    def __init__(self, output_dir: Path, lock_timeout: float):
        self.dir = output_dir.resolve() / NET_BATCHES_DIR
        self.data_path = self.dir / NET_BATCHES_FILE
        self.lock_path = self.dir / NET_BATCHES_LOCK
        self.lock_timeout = lock_timeout

    def ensure(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)

    def lock(self) -> FileLock:
        return FileLock(
            self.lock_path,
            self.lock_timeout,
            "另一个净重批次正在落库，获取锁超时",
        )

    def load_all(self) -> dict[str, NetBatch]:
        if not self.data_path.exists():
            return {}
        with open(self.data_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return {bid: NetBatch.from_dict(item) for bid, item in payload.items()}

    def save_all(self, batches: dict[str, NetBatch]) -> None:
        self.ensure()
        payload = {bid: batch.to_dict() for bid, batch in batches.items()}
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=self.dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.data_path)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp)
            raise


@dataclasses.dataclass
class _TicketRow:
    location: TicketLocation
    ticket_type: str
    container_no: str
    weight: float


class NetBatchService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.manifest_store = ManifestStore(settings.output_dir)
        self.net_store = NetBatchStore(
            settings.output_dir, settings.lock_timeout_seconds
        )
        # 单次提交内的读票缓存：(date, source_file) -> 行列表
        self._day_cache: dict[tuple[str, str], list[_TicketRow]] = {}

    # ------------------------------------------------------------------
    def get_batch(self, batch_id: str) -> NetBatch | None:
        return self.net_store.load_all().get(batch_id)

    def current_manifest(self):
        # 单独成方法，便于测试模拟「读票期间换版」
        return self.manifest_store.load_current()

    # ------------------------------------------------------------------
    def submit(
        self,
        batch_id: str,
        manifest_version: int,
        pairs: list[TicketPairInput],
    ) -> tuple[NetBatch, bool]:
        """提交净重批次；返回 (批次记录, 是否幂等重放)。

        失败抛 NetBatchValidationError(422) / NetBatchConflict(409)，
        任何失败路径都不写 batches.json。
        """
        # 锁外只做请求形态校验；清单版号以锁内重新读取的 current.json 为准，
        # 且幂等重放优先于版号校验（换版后原请求重传仍返回原结果）
        self._check_request_internal_duplicates(pairs)

        try:
            with self.net_store.lock():
                stored = self.net_store.load_all()
                if batch_id in stored:
                    return self._handle_replay(batch_id, manifest_version, pairs,
                                               stored[batch_id])

                # 落库前以当前 current.json 为准重新读取（允许发布在此期间换版）
                fresh_manifest = self.current_manifest()
                if fresh_manifest is None:
                    raise NetBatchConflict(["尚无已发布清单，无法核对净重批次"])
                if fresh_manifest.version != manifest_version:
                    raise NetBatchConflict(
                        [
                            f"清单版号不一致：请求核对版号 v{manifest_version}，"
                            f"当前生效 v{fresh_manifest.version}，整批退回"
                        ]
                    )

                results = self._validate_pairs(fresh_manifest, pairs, stored)
                batch = NetBatch(
                    batch_id=batch_id,
                    manifest_version=manifest_version,
                    created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
                    request=canonical_request(batch_id, manifest_version, pairs),
                    pairs=results,
                )
                stored[batch_id] = batch
                self.net_store.save_all(stored)  # 原子替换，失败则整批不存在
                return batch, False
        except LockTimeout as exc:
            raise NetBatchConflict([str(exc)]) from exc

    # ------------------------------------------------------------------
    def _handle_replay(
        self,
        batch_id: str,
        manifest_version: int,
        pairs: list[TicketPairInput],
        existing: NetBatch,
    ) -> tuple[NetBatch, bool]:
        incoming = canonical_request(batch_id, manifest_version, pairs)
        if _canonical_key(incoming) == _canonical_key(existing.request):
            # 原请求重传：无论清单是否已换版，都原样返回首次结果
            return existing, True
        raise NetBatchConflict(
            [
                f"批号 {batch_id} 已入库且请求内容与首次提交不一致"
                "（同批号改内容报冲突；如需重放请原样重传原请求）"
            ]
        )

    def _check_request_internal_duplicates(
        self, pairs: list[TicketPairInput]
    ) -> None:
        seen: dict[tuple[str, str], str] = {}
        reasons: list[str] = []
        for idx, pr in enumerate(pairs, start=1):
            for role, ref in (("毛票", pr.gross), ("皮票", pr.tare)):
                key = (ref.date, ref.ticket_id)
                if key in seen:
                    reasons.append(
                        f"第 {idx} 对{role} {ref.date}/{ref.ticket_id} "
                        f"与{seen[key]}重复：同日同票号在本批只能使用一次"
                    )
                else:
                    seen[key] = f"第 {idx} 对{role}"
        if reasons:
            raise NetBatchConflict(reasons)

    # ------------------------------------------------------------------
    def _validate_pairs(
        self,
        manifest,
        pairs: list[TicketPairInput],
        stored: dict[str, NetBatch],
    ) -> list[NetPairResult]:
        reasons: list[str] = []
        results: list[NetPairResult] = []

        used_refs = self._collect_used_refs(stored)
        required = [
            self.settings.ticket_id_column,
            self.settings.container_column,
            self.settings.ticket_type_column,
            self.settings.weight_column,
        ]

        for idx, pr in enumerate(pairs, start=1):
            gross_row = self._locate_ticket(
                manifest, pr.gross, "毛票", idx, required, reasons
            )
            tare_row = self._locate_ticket(
                manifest, pr.tare, "皮票", idx, required, reasons
            )
            for role, ref, owner in (
                ("毛票", pr.gross, gross_row),
                ("皮票", pr.tare, tare_row),
            ):
                owner_batch = used_refs.get((ref.date, ref.ticket_id))
                if owner_batch is not None:
                    reasons.append(
                        f"第 {idx} 对{role} {ref.date}/{ref.ticket_id} "
                        f"已在已入库批次 {owner_batch} 中使用，不可重复使用"
                    )
            if gross_row is None or tare_row is None:
                continue  # 定位失败的票对无法继续核对，原因已登记
            if gross_row.ticket_type != self.settings.gross_type_value:
                reasons.append(
                    f"第 {idx} 对毛票 {pr.gross.date}/{pr.gross.ticket_id} "
                    f"票种为 {gross_row.ticket_type!r}，"
                    f"应为 {self.settings.gross_type_value!r}"
                )
            if tare_row.ticket_type != self.settings.tare_type_value:
                reasons.append(
                    f"第 {idx} 对皮票 {pr.tare.date}/{pr.tare.ticket_id} "
                    f"票种为 {tare_row.ticket_type!r}，"
                    f"应为 {self.settings.tare_type_value!r}"
                )
            if gross_row.container_no != tare_row.container_no:
                reasons.append(
                    f"第 {idx} 对箱号不一致：毛票箱号 {gross_row.container_no!r}，"
                    f"皮票箱号 {tare_row.container_no!r}"
                )
            if gross_row.weight is None or tare_row.weight is None:
                reasons.append(f"第 {idx} 对存在空重量，无法核算净重")
            elif gross_row.weight < tare_row.weight:
                reasons.append(
                    f"第 {idx} 对毛重 {gross_row.weight} 低于皮重 "
                    f"{tare_row.weight}，净重为负，拒绝结算"
                )
            pair_problem = any(f"第 {idx} 对" in r for r in reasons)
            if not pair_problem:
                # 仅当该票对自身没有任何问题时才产出结果
                results.append(
                    NetPairResult(
                        gross=gross_row.location,
                        tare=tare_row.location,
                        manifest_version=manifest.version,
                        container_no=gross_row.container_no,
                        net_weight_kg=gross_row.weight - tare_row.weight,
                    )
                )
        if reasons:
            raise NetBatchConflict(reasons)
        return results

    def _collect_used_refs(
        self, stored: dict[str, NetBatch]
    ) -> dict[tuple[str, str], str]:
        used: dict[tuple[str, str], str] = {}
        for batch in stored.values():
            for p in batch.pairs:
                used[(p.gross.date, p.gross.ticket_id)] = batch.batch_id
                used[(p.tare.date, p.tare.ticket_id)] = batch.batch_id
        return used

    # ------------------------------------------------------------------
    def _locate_ticket(
        self,
        manifest,
        ref: TicketRef,
        role: str,
        pair_index: int,
        required_columns: list[str],
        reasons: list[str],
    ) -> _TicketRow | None:
        entries = manifest.files_for_date(ref.date)
        if not entries:
            reasons.append(
                f"第 {pair_index} 对{role} {ref.date}/{ref.ticket_id} 未命中："
                f"清单 v{manifest.version} 在 UTC 日 {ref.date} 没有任何票文件"
            )
            return None
        hits: list[_TicketRow] = []
        for entry in entries:
            for row in self._read_day(manifest, entry, required_columns, reasons):
                if row.location.ticket_id == ref.ticket_id:
                    hits.append(row)
        prefix = f"第 {pair_index} 对{role} {ref.date}/{ref.ticket_id}"
        if not hits:
            reasons.append(f"{prefix} 未命中：当日清单中不存在该票号")
            return None
        if len(hits) > 1:
            reasons.append(
                f"{prefix} 命中 {len(hits)} 张票（要求唯一命中），定位不唯一"
            )
            return None
        row = hits[0]
        if row.container_no is None:
            reasons.append(f"{prefix} 箱号列为空，无法核对箱号")
        if row.weight is None:
            reasons.append(f"{prefix} 重量列为空，无法核算净重")
        return row

    def _read_day(
        self,
        manifest,
        entry: DayFile,
        required_columns: list[str],
        reasons: list[str],
    ) -> list[_TicketRow]:
        cache_key = (entry.date, entry.source_file)
        if cache_key in self._day_cache:
            return self._day_cache[cache_key]
        fs_path = self.manifest_store.resolve(entry, input_dir=self.settings.input_dir)
        if not fs_path.is_file():
            raise NetBatchConflict(
                [f"清单登记的票文件已丢失: {entry.path}（source_file={entry.source_file}）"]
            )
        table = pq.read_table(fs_path)
        missing = [c for c in required_columns if c not in table.column_names]
        if missing:
            raise NetBatchValidationError(
                [
                    f"票文件 {entry.source_file} 缺少净重结算必需列 "
                    f"{missing}（当前配置: {required_columns}）"
                ]
            )
        ids = table.column(self.settings.ticket_id_column).to_pylist()
        types = table.column(self.settings.ticket_type_column).to_pylist()
        containers = table.column(self.settings.container_column).to_pylist()
        weights = table.column(self.settings.weight_column).to_pylist()
        rows: list[_TicketRow] = []
        # enumerate 不跳过空票号行：row_index 必须是 Parquet 文件内的真实行号
        for row_index, (ticket_id, ticket_type, container, weight) in enumerate(
            zip(ids, types, containers, weights)
        ):
            if ticket_id is None:
                continue  # 空票号行不可能被 (UTC 日, 票号) 定位
            rows.append(
                _TicketRow(
                    location=TicketLocation(
                        date=entry.date,
                        ticket_id=str(ticket_id),
                        source=entry.source,
                        source_file=entry.source_file,
                        path=entry.path,
                        row_index=row_index,
                    ),
                    ticket_type="" if ticket_type is None else str(ticket_type),
                    container_no=container,
                    weight=weight,
                )
            )
        self._day_cache[cache_key] = rows
        return rows
