"""核心领域引擎：质量执行、版本存储、消费确认与处置编排。

所有写操作经过同一把进程锁串行化；SQLite 单文件持久化。
关键不变量：
  1. raw_records 只 INSERT，代码中不存在任何 UPDATE/DELETE 路径。
  2. versions 只追加；坏版本永久保留，隔离/修复都以更高版本表达。
  3. 质检结果迟到（任务版本 < 当前最新版本）一律 fenced，不撤销任何已发布版本。
  4. 补偿记录携带幂等键，崩溃重试不会重复冲减；非幂等消费者的重复处理
     风险被如实暴露（对账会出现差异），而不是被平台悄悄掩盖。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .schema import SCHEMA_SQL

EPS = 1e-9


class DeliveryCrash(Exception):
    """模拟下游在投递过程中崩溃（部分提交或提交前失败）。"""


class DomainError(Exception):
    """业务规则错误。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _totals_to_jsonable(totals: dict[tuple[str, str], float]) -> dict[str, float]:
    """tuple 键 → "分区\\u001f指标" 字符串键，供 JSON 持久化。"""
    return {f"{pk}\x1f{metric}": total for (pk, metric), total in totals.items()}


def _totals_from_jsonable(data: dict[str, float]) -> dict[tuple[str, str], float]:
    return {tuple(key.split("\x1f", 1)): total for key, total in data.items()}


class QualityEngine:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    # ------------------------------------------------------------------ 基础
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def _event(self, conn: sqlite3.Connection, kind: str, detail: dict) -> None:
        conn.execute(
            "INSERT INTO events(ts, kind, detail_json) VALUES(?,?,?)",
            (_now(), kind, _dumps(detail)),
        )

    def _max_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COALESCE(MAX(version),0) AS m FROM versions").fetchone()
        return row["m"]

    # ------------------------------------------------------- 原始接入（只追加）
    def ingest_records(self, records: list[dict], note: str = "") -> list[str]:
        """接入原始记录。返回 record_id 列表。原始记录之后不可变。"""
        if not records:
            raise DomainError("没有可接入的记录")
        with self._lock, self._connect() as conn:
            ids = []
            for rec in records:
                rid = rec.get("record_id") or _new_id("rec")
                if conn.execute(
                    "SELECT 1 FROM raw_records WHERE record_id=?", (rid,)
                ).fetchone():
                    raise DomainError(f"record_id 已存在: {rid}")
                amount = float(rec["amount"])
                row = (
                    rid,
                    rec["stream"],
                    rec["partition_key"],
                    rec["metric"],
                    amount,
                    rec["event_time"],
                    _now(),
                    hashlib.sha256(
                        _dumps(
                            [rid, rec["stream"], rec["partition_key"],
                             rec["metric"], amount, rec["event_time"]]
                        ).encode()
                    ).hexdigest(),
                )
                conn.execute(
                    "INSERT INTO raw_records(record_id, stream, partition_key, metric,"
                    " amount, event_time, received_at, source_hash) VALUES(?,?,?,?,?,?,?,?)",
                    row,
                )
                ids.append(rid)
            self._event(conn, "ingest", {"record_ids": ids, "note": note})
            return ids

    def publish_incremental(self, record_ids: list[str], note: str = "") -> dict:
        """把指定（且尚未被任何增量版本包含的）原始记录作为新增量版本发布。"""
        if not record_ids:
            raise DomainError("增量版本至少包含一条记录")
        with self._lock, self._connect() as conn:
            rows = []
            for rid in record_ids:
                r = conn.execute(
                    "SELECT * FROM raw_records WHERE record_id=?", (rid,)
                ).fetchone()
                if not r:
                    raise DomainError(f"原始记录不存在: {rid}")
                dup = conn.execute(
                    "SELECT 1 FROM version_records vr JOIN versions v ON v.version=vr.version"
                    " WHERE vr.record_id=? AND v.version_type IN ('incremental','snapshot')",
                    (rid,),
                ).fetchone()
                if dup:
                    raise DomainError(f"记录已被历史版本包含，不能重复发布: {rid}")
                rows.append(r)
            version = self._max_version(conn) + 1
            payload = {
                "type": "incremental",
                "records": [dict(r) for r in rows],
            }
            conn.execute(
                "INSERT INTO versions(version, version_type, note, payload_json, created_at)"
                " VALUES(?,?,?,?,?)",
                (version, "incremental", note, _dumps(payload), _now()),
            )
            conn.executemany(
                "INSERT INTO version_records(version, record_id) VALUES(?,?)",
                [(version, rid) for rid in record_ids],
            )
            self._event(conn, "publish_incremental",
                        {"version": version, "record_ids": record_ids, "note": note})
            return {"version": version, "record_count": len(rows)}

    def create_snapshot(self, note: str = "") -> dict:
        """基于当前原始台账（含已确认修正的真值）生成全量快照版本。"""
        with self._lock, self._connect() as conn:
            totals = self._truth_totals(conn)
            record_ids = [
                r["record_id"]
                for r in conn.execute("SELECT record_id FROM raw_records ORDER BY record_id")
            ]
            version = self._max_version(conn) + 1
            payload = {"type": "snapshot",
                       "totals": _totals_to_jsonable(totals),
                       "record_ids": record_ids}
            conn.execute(
                "INSERT INTO versions(version, version_type, note, payload_json, created_at)"
                " VALUES(?,?,?,?,?)",
                (version, "snapshot", note, _dumps(payload), _now()),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO version_records(version, record_id) VALUES(?,?)",
                [(version, rid) for rid in record_ids],
            )
            self._event(conn, "snapshot_created",
                        {"version": version, "record_count": len(record_ids), "note": note})
            return {"version": version, "record_count": len(record_ids)}

    # ------------------------------------------------------------- 坏数据注入
    def inject_defect(self, record_id: str, correct_amount: float, note: str = "") -> dict:
        """登记一条坏数据嫌疑。不修改 raw_records，只在独立表登记正确值。

        确认前不影响真值；质检确认（或人工确认）后才纳入真值与对账。
        """
        with self._lock, self._connect() as conn:
            r = conn.execute(
                "SELECT * FROM raw_records WHERE record_id=?", (record_id,)
            ).fetchone()
            if not r:
                raise DomainError(f"原始记录不存在: {record_id}")
            if conn.execute(
                "SELECT 1 FROM injected_defects WHERE record_id=?", (record_id,)
            ).fetchone():
                raise DomainError("该记录已登记坏数据")
            did = _new_id("def")
            conn.execute(
                "INSERT INTO injected_defects(defect_id, record_id, bad_amount,"
                " correct_amount, note, confirmed, injected_at) VALUES(?,?,?,?,?,0,?)",
                (did, record_id, r["amount"], float(correct_amount), note, _now()),
            )
            self._event(conn, "defect_injected",
                        {"defect_id": did, "record_id": record_id,
                         "bad_amount": r["amount"], "correct_amount": float(correct_amount),
                         "note": note, "confirmed": False})
            return {"defect_id": did, "record_id": record_id,
                    "bad_amount": r["amount"], "correct_amount": float(correct_amount)}

    def confirm_defect_manually(self, defect_id: str) -> dict:
        """人工确认坏数据（用于迟到质检之外的处置路径），同样不会回滚旧版本。"""
        with self._lock, self._connect() as conn:
            issues = self._confirm_defects(conn, [defect_id], manual=True)
            return {"issues": issues}

    def _confirm_defects(self, conn, defect_ids: list[str], manual: bool) -> list[dict]:
        created = []
        for did in defect_ids:
            d = conn.execute(
                "SELECT * FROM injected_defects WHERE defect_id=?", (did,)
            ).fetchone()
            if not d:
                raise DomainError(f"坏数据登记不存在: {did}")
            if d["confirmed"]:
                continue
            conn.execute(
                "UPDATE injected_defects SET confirmed=1, confirmed_at=? WHERE defect_id=?",
                (_now(), did),
            )
            # 按 分区×指标×坏版本 归并成隔离单
            r = conn.execute(
                "SELECT * FROM raw_records WHERE record_id=?", (d["record_id"],)
            ).fetchone()
            bad_version_row = conn.execute(
                "SELECT MIN(version) AS v FROM version_records WHERE record_id=?",
                (d["record_id"],),
            ).fetchone()
            bad_version = bad_version_row["v"]
            if bad_version is None:
                # 记录尚未发布：修正未来版本即可，无需隔离单
                self._event(conn, "issue_skipped_unpublished",
                            {"record_id": d["record_id"]})
                continue
            issue = conn.execute(
                "SELECT * FROM issues WHERE partition_key=? AND metric=? AND bad_version=?"
                " AND state IN ('isolated','fenced','repairing')",
                (r["partition_key"], r["metric"], bad_version),
            ).fetchone()
            if issue:
                recs = json.loads(issue["record_ids_json"])
                recs.append(r["record_id"])
                bad_total = issue["bad_total"] + d["bad_amount"]
                expected_total = issue["expected_total"] + d["correct_amount"]
                conn.execute(
                    "UPDATE issues SET record_ids_json=?, bad_total=?, expected_total=?,"
                    " updated_at=? WHERE issue_id=?",
                    (_dumps(sorted(set(recs))), bad_total, expected_total,
                     _now(), issue["issue_id"]),
                )
                iid = issue["issue_id"]
            else:
                iid = _new_id("iss")
                conn.execute(
                    "INSERT INTO issues(issue_id, state, partition_key, metric, bad_version,"
                    " record_ids_json, bad_total, expected_total, created_at, updated_at)"
                    " VALUES(?, 'isolated', ?, ?, ?, ?, ?, ?, ?, ?)",
                    (iid, r["partition_key"], r["metric"], bad_version,
                     _dumps([r["record_id"]]), d["bad_amount"], d["correct_amount"],
                     _now(), _now()),
                )
            created.append({"issue_id": iid, "record_id": r["record_id"],
                            "bad_version": bad_version})
            self._event(conn, "issue_created",
                        {"issue_id": iid, "record_id": r["record_id"],
                         "partition_key": r["partition_key"], "metric": r["metric"],
                         "bad_version": bad_version, "manual": manual})
        return created

    # ---------------------------------------------------------------- 质检
    def create_qc_task(self, version: int) -> dict:
        conn = self._connect()
        with self._lock, conn:
            if not conn.execute("SELECT 1 FROM versions WHERE version=?", (version,)).fetchone():
                raise DomainError(f"版本不存在: {version}")
            tid = _new_id("qc")
            conn.execute(
                "INSERT INTO qc_tasks(task_id, version, status, created_at)"
                " VALUES(?,?,'pending',?)",
                (tid, version, _now()),
            )
            self._event(conn, "qc_created", {"task_id": tid, "version": version})
            return {"task_id": tid, "version": version}

    def complete_qc_task(self, task_id: str, passed: bool,
                         defect_ids: list[str] | None = None) -> dict:
        """质检结果返回。

        迟到（任务版本 < 当前最新版本）：结果仅记录为 fenced，不产生任何处置、
        不修改新版本。人工仍可另行确认缺陷，但同样只能产生更高版本的补偿。
        """
        with self._lock, self._connect() as conn:
            t = conn.execute("SELECT * FROM qc_tasks WHERE task_id=?", (task_id,)).fetchone()
            if not t:
                raise DomainError(f"质检任务不存在: {task_id}")
            if t["status"] != "pending":
                raise DomainError(f"质检任务已完结: {task_id}（{t['status']}）")
            latest = self._max_version(conn)
            result = {"passed": passed, "defect_ids": defect_ids or []}
            if t["version"] < latest:
                # 围栏：迟到结果不能撤销新发布
                conn.execute(
                    "UPDATE qc_tasks SET status='fenced', result_json=?, completed_at=?"
                    " WHERE task_id=?",
                    (_dumps(result), _now(), task_id),
                )
                self._event(conn, "qc_fenced",
                            {"task_id": task_id, "task_version": t["version"],
                             "current_version": latest, "passed": passed})
                return {"task_id": task_id, "status": "fenced",
                        "task_version": t["version"], "current_version": latest}

            if passed:
                conn.execute(
                    "UPDATE qc_tasks SET status='passed', result_json=?, completed_at=?"
                    " WHERE task_id=?",
                    (_dumps(result), _now(), task_id),
                )
                self._event(conn, "qc_passed", {"task_id": task_id,
                                                "version": t["version"]})
                return {"task_id": task_id, "status": "passed"}

            # 准时失败：确认关联缺陷并生成隔离单
            valid_defects = []
            for did in defect_ids or []:
                d = conn.execute(
                    "SELECT * FROM injected_defects WHERE defect_id=?", (did,)
                ).fetchone()
                if d:
                    valid_defects.append(did)
            issues = self._confirm_defects(conn, valid_defects, manual=False)
            conn.execute(
                "UPDATE qc_tasks SET status='failed', result_json=?, completed_at=?"
                " WHERE task_id=?",
                (_dumps(result), _now(), task_id),
            )
            self._event(conn, "qc_failed",
                        {"task_id": task_id, "version": t["version"],
                         "issue_count": len(issues)})
            return {"task_id": task_id, "status": "failed", "issues": issues}

    # ----------------------------------------------------------- 影响面追踪
    def issue_impact(self, issue_id: str) -> dict:
        with self._lock, self._connect() as conn:
            issue = conn.execute("SELECT * FROM issues WHERE issue_id=?", (issue_id,)).fetchone()
            if not issue:
                raise DomainError(f"隔离单不存在: {issue_id}")
            record_ids = json.loads(issue["record_ids_json"])
            bad_versions = [
                r["version"]
                for r in conn.execute(
                    "SELECT DISTINCT version FROM version_records WHERE record_id IN (%s)"
                    % ",".join("?" * len(record_ids)),
                    record_ids,
                )
            ]
            read_consumers, unread_consumers = [], []
            for c in conn.execute("SELECT * FROM consumers ORDER BY consumer_id"):
                cv = conn.execute(
                    "SELECT status FROM consumer_versions WHERE consumer_id=? AND version=?",
                    (c["consumer_id"], issue["bad_version"]),
                ).fetchone()
                info = {"consumer_id": c["consumer_id"], "name": c["name"],
                        "mode": c["mode"], "last_offset": c["last_offset"]}
                if cv and cv["status"] == "delivered":
                    read_consumers.append(info)
                else:
                    unread_consumers.append(info)
            affected_records = [
                dict(conn.execute("SELECT * FROM raw_records WHERE record_id=?", (rid,)).fetchone())
                for rid in record_ids
            ]
            return {
                "issue": dict(issue),
                "affected_versions": sorted(bad_versions),
                "read_consumers": read_consumers,
                "unread_consumers": unread_consumers,
                "affected_records": affected_records,
                "delta": round(issue["expected_total"] - issue["bad_total"], 10),
            }

    # --------------------------------------------------------------- 处置决策
    def plan_disposition(self, issue_ids: list[str]) -> dict:
        """对同一批隔离单给出两种策略的真实对比数据（不产生版本）。"""
        with self._lock, self._connect() as conn:
            issues = self._load_issues(conn, issue_ids)
            affected_pm = {(i["partition_key"], i["metric"]) for i in issues}
            all_pm = set(self._truth_totals(conn).keys())
            consumer_rows = conn.execute(
                "SELECT * FROM consumers ORDER BY consumer_id"
            ).fetchall()
            record_count = conn.execute(
                "SELECT COUNT(*) AS c FROM raw_records"
            ).fetchone()["c"]

            comp = {
                "strategy": "compensation",
                "label": "发布补偿记录（增量冲减/补提）",
                "scope_partition_metrics": sorted(affected_pm),
                "scope_size": len(affected_pm),
                "new_versions": 1,
                "rebuild_record_count": sum(
                    len(json.loads(i["record_ids_json"])) for i in issues
                ),
                "wait": "短：只重算受影响分区，无需全量重算，可立即发布",
                "consumer_action": "正常拉取新版本，按幂等键应用差额",
                "duplicate_risk": (
                    "低：幂等消费者以 版本:记录 键去重，崩溃重试安全；"
                    "非幂等消费者重试会重复冲减，需其自行改造或人工核对"
                ),
                "laggard_behavior": "未读坏版本的消费者跳到补偿版本时，"
                                    "仅看到修正后的累计结果，不会先坏后好地抖动",
                "history": "坏版本与补偿版本都保留，历史可审计",
            }
            snap = {
                "strategy": "snapshot",
                "label": "切完整新快照（全量替换基线）",
                "scope_partition_metrics": sorted(all_pm),
                "scope_size": len(all_pm),
                "new_versions": 1,
                "rebuild_record_count": record_count,
                "wait": f"长：需全量重算 {record_count} 条记录并等待所有消费者切换基线",
                "consumer_action": "全部消费者以新快照替换本地状态（replace 语义）",
                "duplicate_risk": (
                    "替换语义本身不产生重复计数；但若有消费者漏接快照、继续在旧基线上"
                    "累加增量，会长期偏离；错误地把快照当增量累加则直接翻倍"
                ),
                "laggard_behavior": "落后消费者必须先对齐到快照版本，否则后续增量无基线可挂",
                "history": "坏版本同样保留，快照为独立新版本",
            }
            per_consumer = []
            for c in consumer_rows:
                read_bad = any(
                    conn.execute(
                        "SELECT 1 FROM consumer_versions WHERE consumer_id=? AND version=?"
                        " AND status='delivered'",
                        (c["consumer_id"], i["bad_version"]),
                    ).fetchone()
                    for i in issues
                )
                per_consumer.append({
                    "consumer_id": c["consumer_id"], "name": c["name"],
                    "mode": c["mode"], "read_bad_version": bool(read_bad),
                    "compensation_risk": "重复处理会被幂等键拦截"
                    if c["mode"] == "idempotent" else "重试将重复冲减（高危）",
                    "snapshot_requirement": "必须完成一次基线替换",
                })
            return {"issues": [dict(i) for i in issues],
                    "options": {"compensation": comp, "snapshot": snap},
                    "consumers": per_consumer}

    def _load_issues(self, conn, issue_ids):
        issues = []
        for iid in issue_ids:
            i = conn.execute("SELECT * FROM issues WHERE issue_id=?", (iid,)).fetchone()
            if not i:
                raise DomainError(f"隔离单不存在: {iid}")
            if i["state"] == "repaired":
                raise DomainError(f"隔离单已修复: {iid}")
            issues.append(i)
        return issues

    def execute_disposition(self, strategy: str, issue_ids: list[str],
                            note: str = "") -> dict:
        with self._lock, self._connect() as conn:
            issues = self._load_issues(conn, issue_ids)
            disp_id = _new_id("dsp")
            now = _now()
            if strategy == "compensation":
                corrections = []
                affected_records = []
                for i in issues:
                    conn.execute(
                        "UPDATE issues SET state='repairing', disposition_id=?, updated_at=?"
                        " WHERE issue_id=?",
                        (disp_id, now, i["issue_id"]),
                    )
                    for rid in json.loads(i["record_ids_json"]):
                        d = conn.execute(
                            "SELECT * FROM injected_defects WHERE record_id=?", (rid,)
                        ).fetchone()
                        r = conn.execute(
                            "SELECT * FROM raw_records WHERE record_id=?", (rid,)
                        ).fetchone()
                        corrections.append({
                            "record_id": rid,
                            "partition_key": i["partition_key"],
                            "metric": i["metric"],
                            "bad_amount": d["bad_amount"],
                            "correct_amount": d["correct_amount"],
                            "delta": round(d["correct_amount"] - d["bad_amount"], 10),
                            "source_version": i["bad_version"],
                            # 幂等键：同一坏记录的补偿无论重试多少次只生效一次
                            "idempotency_key": f"v{i['bad_version']}:{rid}",
                        })
                        affected_records.append(rid)
                version = self._max_version(conn) + 1
                payload = {"type": "compensation",
                           "disposition_id": disp_id,
                           "corrections": corrections}
                conn.execute(
                    "INSERT INTO versions(version, version_type, note, payload_json, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (version, "compensation", note, _dumps(payload), now),
                )
                conn.executemany(
                    "INSERT OR IGNORE INTO version_records(version, record_id) VALUES(?,?)",
                    [(version, rid) for rid in affected_records],
                )
            elif strategy == "snapshot":
                for i in issues:
                    conn.execute(
                        "UPDATE issues SET state='repairing', disposition_id=?, updated_at=?"
                        " WHERE issue_id=?",
                        (disp_id, now, i["issue_id"]),
                    )
                totals = self._truth_totals(conn)
                record_ids = [
                    r["record_id"]
                    for r in conn.execute("SELECT record_id FROM raw_records ORDER BY record_id")
                ]
                version = self._max_version(conn) + 1
                payload = {"type": "repair_snapshot", "disposition_id": disp_id,
                           "totals": _totals_to_jsonable(totals),
                           "record_ids": record_ids}
                conn.execute(
                    "INSERT INTO versions(version, version_type, note, payload_json, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (version, "repair_snapshot", note, _dumps(payload), now),
                )
                conn.executemany(
                    "INSERT OR IGNORE INTO version_records(version, record_id) VALUES(?,?)",
                    [(version, rid) for rid in record_ids],
                )
            else:
                raise DomainError(f"未知处置策略: {strategy}")

            rationale = self.plan_disposition(issue_ids)
            conn.execute(
                "INSERT INTO dispositions(disposition_id, strategy, issue_ids_json,"
                " repair_version, rationale_json, state, created_at, released_at)"
                " VALUES(?,?,?,?,?, 'released', ?, ?)",
                (disp_id, strategy, _dumps(issue_ids), version,
                 _dumps(rationale), now, now),
            )
            for i in issues:
                conn.execute(
                    "UPDATE issues SET state='repaired', updated_at=? WHERE issue_id=?",
                    (now, i["issue_id"]),
                )
            self._event(conn, "disposition_released",
                        {"disposition_id": disp_id, "strategy": strategy,
                         "repair_version": version, "issue_ids": issue_ids})
            return {"disposition_id": disp_id, "strategy": strategy,
                    "repair_version": version, "issue_count": len(issues)}

    # ------------------------------------------------------------- 消费者管理
    def register_consumer(self, name: str, mode: str) -> dict:
        if mode not in ("idempotent", "non_idempotent"):
            raise DomainError("mode 只能是 idempotent / non_idempotent")
        with self._lock, self._connect() as conn:
            cid = _new_id("c")
            conn.execute(
                "INSERT INTO consumers(consumer_id, name, mode, created_at)"
                " VALUES(?,?,?,?)",
                (cid, name, mode, _now()),
            )
            self._event(conn, "consumer_registered",
                        {"consumer_id": cid, "name": name, "mode": mode})
            return {"consumer_id": cid, "name": name, "mode": mode}

    # ------------------------------------------------------------- 下游投递模拟
    def deliver_versions(self, consumer_id: str, target_version: int | None = None,
                         crash_phase: str | None = None) -> dict:
        """把未投递版本投递给消费者。

        crash_phase:
          before_commit —— 整版处理完但提交前崩溃（回滚，无部分效果）
          mid           —— 处理到一半崩溃（已处理单元带幂等键落库）
        幂等消费者：每个应用单元先写幂等键，重试自动跳过；
        非幂等消费者：不写键，重试会重复处理（用于暴露真实风险）。
        """
        if crash_phase not in (None, "before_commit", "mid"):
            raise DomainError("crash_phase 非法")
        with self._lock:
            conn = self._connect()
            try:
                c = conn.execute(
                    "SELECT * FROM consumers WHERE consumer_id=?", (consumer_id,)
                ).fetchone()
                if not c:
                    raise DomainError(f"消费者不存在: {consumer_id}")
                latest = self._max_version(conn)
                target = latest if target_version is None else target_version
                if target > latest:
                    raise DomainError(f"目标版本 {target} 尚不存在（当前最新 {latest}）")
                idem = c["mode"] == "idempotent"
                applied, crashed_at, skipped_units = [], None, 0

                for version in range(c["last_offset"] + 1, target + 1):
                    v = conn.execute(
                        "SELECT * FROM versions WHERE version=?", (version,)
                    ).fetchone()
                    payload = json.loads(v["payload_json"])
                    cv = conn.execute(
                        "SELECT * FROM consumer_versions WHERE consumer_id=? AND version=?",
                        (consumer_id, version),
                    ).fetchone()
                    attempts = (cv["attempts"] if cv else 0) + 1
                    conn.execute(
                        "INSERT INTO consumer_versions(consumer_id, version, status, attempts)"
                        " VALUES(?,?,?,?) ON CONFLICT(consumer_id, version) DO UPDATE SET"
                        " attempts=excluded.attempts",
                        (consumer_id, version, "crashed", attempts),
                    )
                    conn.execute("BEGIN")
                    try:
                        if payload["type"] in ("incremental", "compensation"):
                            units = (payload["records"] if payload["type"] == "incremental"
                                     else payload["corrections"])
                            midpoint = max(len(units) // 2 - (1 if len(units) == 1 else 0), 0)
                            for idx, unit in enumerate(units):
                                pk = unit["partition_key"]
                                metric = unit["metric"]
                                if payload["type"] == "incremental":
                                    key = f"{version}:{unit['record_id']}"
                                    delta = unit["amount"]
                                else:
                                    key = unit["idempotency_key"]
                                    delta = unit["delta"]
                                if idem and conn.execute(
                                    "SELECT 1 FROM idempotency_keys WHERE consumer_id=? AND idem_key=?",
                                    (consumer_id, key),
                                ).fetchone():
                                    skipped_units += 1
                                    continue
                                self._apply_delta(conn, consumer_id, pk, metric, delta)
                                if idem:
                                    conn.execute(
                                        "INSERT INTO idempotency_keys(consumer_id, idem_key,"
                                        " version, applied_at) VALUES(?,?,?,?)",
                                        (consumer_id, key, version, _now()),
                                    )
                                if crash_phase == "mid" and idx == midpoint and version == target:
                                    conn.commit()  # 半版已落库，随后“崩溃”
                                    raise DeliveryCrash(
                                        f"版本 v{version} 处理到第 {idx + 1}/{len(units)} 条时崩溃")
                        else:
                            # snapshot / repair_snapshot：整版替换基线，单键原子完成
                            key = f"snapshot:{version}"
                            already = bool(conn.execute(
                                "SELECT 1 FROM idempotency_keys WHERE consumer_id=? AND idem_key=?",
                                (consumer_id, key),
                            ).fetchone()) if idem else False
                            if not already:
                                conn.execute(
                                    "DELETE FROM consumer_state WHERE consumer_id=?",
                                    (consumer_id,),
                                )
                                for (pk, metric), total in _totals_from_jsonable(
                                    payload["totals"]
                                ).items():
                                    conn.execute(
                                        "INSERT INTO consumer_state(consumer_id, partition_key,"
                                        " metric, total, base_version) VALUES(?,?,?,?,?)",
                                        (consumer_id, pk, metric, total, version),
                                    )
                                if idem:
                                    conn.execute(
                                        "INSERT INTO idempotency_keys(consumer_id, idem_key,"
                                        " version, applied_at) VALUES(?,?,?,?)",
                                        (consumer_id, key, version, _now()),
                                    )
                            if crash_phase == "before_commit" and version == target:
                                conn.rollback()
                                raise DeliveryCrash(
                                    f"版本 v{version} 快照替换在提交前崩溃（已回滚）")
                        if crash_phase == "before_commit" and version == target:
                            conn.rollback()
                            raise DeliveryCrash(
                                f"版本 v{version} 在提交前崩溃（整版回滚）")
                        conn.execute(
                            "UPDATE consumers SET last_offset=? WHERE consumer_id=?",
                            (version, consumer_id),
                        )
                        conn.execute(
                            "UPDATE consumer_versions SET status='delivered', delivered_at=?"
                            " WHERE consumer_id=? AND version=?",
                            (_now(), consumer_id, version),
                        )
                        conn.commit()
                        applied.append(version)
                    except DeliveryCrash:
                        conn.rollback()
                        # mid 崩溃的半版内容在异常前已 commit；保留 crashed 轨迹
                        crashed_at = version
                        self._append_event(
                            "delivery_crash",
                            {"consumer_id": consumer_id, "version": version,
                             "crash_phase": crash_phase})
                        raise
                    except Exception:
                        conn.rollback()
                        raise
                self._append_event("delivery",
                                   {"consumer_id": consumer_id, "applied_versions": applied,
                                    "skipped_units": skipped_units})
                return {"consumer_id": consumer_id, "applied_versions": applied,
                        "skipped_units": skipped_units, "crashed_at": crashed_at,
                        "last_offset": target}
            finally:
                conn.close()

    def _append_event(self, kind: str, detail: dict) -> None:
        with self._connect() as conn:
            self._event(conn, kind, detail)

    @staticmethod
    def _apply_delta(conn, consumer_id: str, partition_key: str, metric: str,
                     delta: float) -> None:
        row = conn.execute(
            "SELECT total FROM consumer_state WHERE consumer_id=? AND partition_key=? AND metric=?",
            (consumer_id, partition_key, metric),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE consumer_state SET total=? WHERE consumer_id=? AND partition_key=?"
                " AND metric=?",
                (row["total"] + delta, consumer_id, partition_key, metric),
            )
        else:
            conn.execute(
                "INSERT INTO consumer_state(consumer_id, partition_key, metric, total,"
                " base_version) VALUES(?,?,?,?,0)",
                (consumer_id, partition_key, metric, delta),
            )

    def apply_snapshot_as_incremental(self, consumer_id: str, version: int) -> dict:
        """错误用法演示：把快照当增量累加（应在 UI 明确标注为错误接线）。"""
        with self._lock, self._connect() as conn:
            v = conn.execute("SELECT * FROM versions WHERE version=?", (version,)).fetchone()
            if not v or json.loads(v["payload_json"])["type"] not in (
                "snapshot", "repair_snapshot"
            ):
                raise DomainError(f"v{version} 不是快照版本")
            payload = json.loads(v["payload_json"])
            payload_totals = (
                _totals_from_jsonable(payload["totals"])
                if payload["type"] in ("snapshot", "repair_snapshot") else None
            )
            for (pk, metric), total in payload_totals.items():
                self._apply_delta(conn, consumer_id, pk, metric, total)
            conn.execute(
                "INSERT INTO consumer_versions(consumer_id, version, status, attempts,"
                " delivered_at) VALUES(?,?, 'delivered', 1, ?) ON CONFLICT(consumer_id,"
                " version) DO UPDATE SET status='delivered', delivered_at=excluded.delivered_at",
                (consumer_id, version, _now()),
            )
            conn.execute("UPDATE consumers SET last_offset=? WHERE consumer_id=?",
                         (max(version, conn.execute(
                             "SELECT last_offset FROM consumers WHERE consumer_id=?",
                             (consumer_id,)).fetchone()["last_offset"]), consumer_id))
            self._event(conn, "snapshot_misused_as_incremental",
                        {"consumer_id": consumer_id, "version": version})
            return {"consumer_id": consumer_id, "version": version,
                    "warning": "快照被错误地按增量累加，对账必然出现差异"}

    def reset_consumer_state(self, consumer_id: str) -> dict:
        """清空消费者物化状态与投递轨迹（用于换策略/重新演示）。"""
        with self._lock, self._connect() as conn:
            if not conn.execute("SELECT 1 FROM consumers WHERE consumer_id=?",
                                (consumer_id,)).fetchone():
                raise DomainError(f"消费者不存在: {consumer_id}")
            conn.execute("DELETE FROM consumer_state WHERE consumer_id=?", (consumer_id,))
            conn.execute("DELETE FROM consumer_versions WHERE consumer_id=?", (consumer_id,))
            conn.execute("DELETE FROM idempotency_keys WHERE consumer_id=?", (consumer_id,))
            conn.execute("UPDATE consumers SET last_offset=0 WHERE consumer_id=?",
                         (consumer_id,))
            self._event(conn, "state_reset", {"consumer_id": consumer_id})
            return {"consumer_id": consumer_id, "reset": True}

    # ------------------------------------------------------------------ 对账
    def _truth_totals(self, conn) -> dict[tuple[str, str], float]:
        """系统真值：原始台账 + 已确认缺陷的正确值。未确认缺陷不改变真值。"""
        totals: dict[tuple[str, str], float] = {}
        for r in conn.execute("SELECT * FROM raw_records ORDER BY record_id"):
            d = conn.execute(
                "SELECT * FROM injected_defects WHERE record_id=? AND confirmed=1",
                (r["record_id"],),
            ).fetchone()
            amount = d["correct_amount"] if d else r["amount"]
            key = (r["partition_key"], r["metric"])
            totals[key] = round(totals.get(key, 0.0) + amount, 10)
        return totals

    def reconcile(self) -> dict:
        with self._lock, self._connect() as conn:
            expected = self._truth_totals(conn)
            result = {"expected_totals": [
                {"partition_key": pk, "metric": m, "total": total}
                for (pk, m), total in sorted(expected.items())
            ], "consumers": [], "all_matched": True}
            for c in conn.execute("SELECT * FROM consumers ORDER BY consumer_id"):
                diffs = []
                state = {
                    (r["partition_key"], r["metric"]): r["total"]
                    for r in conn.execute(
                        "SELECT * FROM consumer_state WHERE consumer_id=?",
                        (c["consumer_id"],))
                }
                keys = sorted(set(expected) | set(state))
                for key in keys:
                    exp = expected.get(key, 0.0)
                    got = state.get(key, 0.0)
                    if abs(exp - got) > EPS:
                        diffs.append({"partition_key": key[0], "metric": key[1],
                                      "expected": exp, "actual": round(got, 10),
                                      "difference": round(got - exp, 10)})
                matched = not diffs
                result["all_matched"] &= matched
                result["consumers"].append({
                    "consumer_id": c["consumer_id"], "name": c["name"], "mode": c["mode"],
                    "last_offset": c["last_offset"], "matched": matched, "diffs": diffs,
                })
            return result

    # ------------------------------------------------------------------ 总览
    def overview(self) -> dict:
        with self._lock, self._connect() as conn:
            versions = [
                {"version": r["version"], "version_type": r["version_type"],
                 "note": r["note"], "created_at": r["created_at"],
                 "payload": json.loads(r["payload_json"])}
                for r in conn.execute("SELECT * FROM versions ORDER BY version")
            ]
            issues = [dict(r) for r in conn.execute(
                "SELECT * FROM issues ORDER BY created_at")]
            for i in issues:
                i["record_ids"] = json.loads(i.pop("record_ids_json"))
            defects = [dict(r) for r in conn.execute(
                "SELECT * FROM injected_defects ORDER BY injected_at")]
            consumers = []
            for c in conn.execute("SELECT * FROM consumers ORDER BY consumer_id"):
                state = [dict(r) for r in conn.execute(
                    "SELECT partition_key, metric, total, base_version FROM consumer_state"
                    " WHERE consumer_id=? ORDER BY partition_key, metric",
                    (c["consumer_id"],))]
                tracks = [dict(r) for r in conn.execute(
                    "SELECT version, status, attempts, delivered_at FROM consumer_versions"
                    " WHERE consumer_id=? ORDER BY version", (c["consumer_id"],))]
                consumers.append({**dict(c), "state": state, "tracks": tracks})
            dispositions = [dict(r) for r in conn.execute(
                "SELECT * FROM dispositions ORDER BY created_at")]
            for d in dispositions:
                d["issue_ids"] = json.loads(d.pop("issue_ids_json"))
                d["rationale"] = json.loads(d.pop("rationale_json"))
            qc_tasks = [dict(r) for r in conn.execute(
                "SELECT * FROM qc_tasks ORDER BY created_at")]
            for t in qc_tasks:
                t["result"] = json.loads(t.pop("result_json")) if t["result_json"] else None
            events = [
                {"id": r["id"], "ts": r["ts"], "kind": r["kind"],
                 "detail": json.loads(r["detail_json"])}
                for r in conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 60")
            ]
            raw = [dict(r) for r in conn.execute(
                "SELECT * FROM raw_records ORDER BY received_at, record_id")]
            partition_metrics = sorted({(r["partition_key"], r["metric"]) for r in raw})
            return {
                "latest_version": self._max_version(conn),
                "versions": versions,
                "raw_records": raw,
                "defects": defects,
                "issues": issues,
                "dispositions": dispositions,
                "qc_tasks": qc_tasks,
                "consumers": consumers,
                "events": events,
                "partition_metrics": [{"partition_key": a, "metric": b}
                                      for a, b in partition_metrics],
                "reconcile": self.reconcile(),
            }

    # ------------------------------------------------------------------ 重置
    def reset_all(self) -> None:
        with self._lock:
            with self._connect() as conn:
                for table in ("events", "idempotency_keys", "consumer_versions",
                              "consumer_state", "consumers", "dispositions", "issues",
                              "qc_tasks", "version_records", "versions",
                              "injected_defects", "raw_records"):
                    conn.execute(f"DELETE FROM {table}")
                conn.execute(
                    "DELETE FROM sqlite_sequence WHERE name IN ('versions','events')")
            self._append_event("reset_all", {})

    # ------------------------------------------------------------- 演示数据
    def seed_demo(self) -> dict:
        """一键灌入完整故事线（坏版本 + 三类消费者 + 待返回质检）。"""
        self.reset_all()
        with self._lock, self._connect() as conn:
            self._event(conn, "demo_seeded", {})
        base = [
            ("r1001", "orders", "华东", "销售额", 1000.0, "2026-11-14T09:00:00Z"),
            ("r1002", "orders", "华东", "订单量", 120.0,   "2026-11-14T09:05:00Z"),
            ("r1003", "orders", "华南", "销售额", 800.0,  "2026-11-14T10:00:00Z"),
            ("r1004", "orders", "华南", "销售额", 500.0,  "2026-11-14T11:00:00Z"),
            ("r1005", "orders", "华北", "订单量", 60.0,   "2026-11-14T11:30:00Z"),
            ("r1006", "orders", "华北", "销售额", 400.0,  "2026-11-14T12:00:00Z"),
        ]
        self.ingest_records(
            [dict(record_id=rid, stream=s, partition_key=pk, metric=m, amount=a,
                  event_time=ts) for rid, s, pk, m, a, ts in base],
            note="演示：首日接入 6 条记录",
        )
        self.publish_incremental(["r1001", "r1002", "r1003"], note="v1 首批增量")
        self.publish_incremental(["r1004", "r1005", "r1006"], note="v2 第二批增量（含坏数据 r1004）")
        # r1004 真实值应为 750（坏值 500，少计 250）
        defect = self.inject_defect("r1004", 750.0, note="上游重复去重错误，销售额少计 250")
        self.create_qc_task(1)
        qc_v2 = self.create_qc_task(2)
        qc_late = self.create_qc_task(1)  # v1 的复检任务，留到新版本发布后再返回
        # 消费者：已读坏版本的幂等/非幂等各一个，外加只读到 v1 的落后者
        eager = self.register_consumer("实时看板（幂等）", "idempotent")
        naive = self.register_consumer("旧版报表（非幂等）", "non_idempotent")
        laggard = self.register_consumer("离线数仓（幂等，落后）", "idempotent")
        self.deliver_versions(eager["consumer_id"])
        self.deliver_versions(naive["consumer_id"])
        self.deliver_versions(laggard["consumer_id"], target_version=1)
        return {
            "defect_id": defect["defect_id"],
            "qc_v2_task": qc_v2["task_id"],
            "qc_late_task": qc_late["task_id"],
            "consumers": {"eager": eager["consumer_id"], "naive": naive["consumer_id"],
                          "laggard": laggard["consumer_id"]},
        }
