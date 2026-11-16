"""端到端正确性测试：质量执行、版本存储、消费确认、处置与恢复。

运行：python -m unittest discover -s tests -v
每个用例使用独立临时数据库，不依赖外部服务。
"""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

# 兼容 python -I（隔离模式不读 PYTHONPATH/当前目录）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.engine import DeliveryCrash, DomainError, QualityEngine


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "test.db")
        self.eng = QualityEngine(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def _seed_simple(self):
        """两条分区×指标，其中 r2 是坏数据（500，应为 750）。"""
        ids = self.eng.ingest_records([
            dict(record_id="r1", stream="orders", partition_key="华东",
                 metric="销售额", amount=1000.0, event_time="t1"),
            dict(record_id="r2", stream="orders", partition_key="华南",
                 metric="销售额", amount=500.0, event_time="t2"),
            dict(record_id="r3", stream="orders", partition_key="华北",
                 metric="订单量", amount=60.0, event_time="t3"),
        ])
        self.eng.publish_incremental(ids, note="v1")
        return ids

    def _truth(self, conn):
        return {
            (r["partition_key"], r["metric"]): r["total"]
            for r in conn.execute(
                "SELECT partition_key, metric, SUM(amount) AS total FROM raw_records"
                " GROUP BY partition_key, metric")
        }


class TestImmutableRawRecords(EngineTestBase):
    def test_no_update_or_delete_path_and_hash_preserved(self):
        ids = self._seed_simple()
        before = self.eng.overview()["raw_records"]
        self.eng.inject_defect("r2", 750.0, note="坏数据")
        defect = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(defect["defect_id"])
        after = self.eng.overview()["raw_records"]
        # 原始台账逐字段不变（含 source_hash）
        self.assertEqual(
            [(r["record_id"], r["amount"], r["source_hash"]) for r in before],
            [(r["record_id"], r["amount"], r["source_hash"]) for r in after],
        )
        # 直接查库：raw_records 不允许出现 750 这个修正值
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT amount FROM raw_records WHERE record_id='r2'").fetchone()
            self.assertEqual(row[0], 500.0)
            # 修正值只存在于独立缺陷表
            row = conn.execute(
                "SELECT correct_amount, confirmed FROM injected_defects"
                " WHERE record_id='r2'").fetchone()
            self.assertEqual(row, (750.0, 1))


class TestVersionLedger(EngineTestBase):
    def test_bad_version_is_never_deleted(self):
        ids = self._seed_simple()
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        versions = [v["version"] for v in self.eng.overview()["versions"]]
        # v1（坏版本）和 v2（补偿）同时存在
        self.assertEqual(versions, [1, 2])
        v1 = self.eng.overview()["versions"][0]
        self.assertEqual(
            [r["record_id"] for r in v1["payload"]["records"] if r["record_id"] == "r2"][0],
            "r2")

    def test_record_cannot_be_published_twice(self):
        ids = self._seed_simple()
        with self.assertRaises(DomainError):
            self.eng.publish_incremental(["r1"])

    def test_versions_are_monotonic_append_only(self):
        ids = self._seed_simple()
        self.eng.publish_incremental  # sanity
        extra = self.eng.ingest_records([
            dict(record_id="r4", stream="orders", partition_key="华东",
                 metric="订单量", amount=10.0, event_time="t4")])
        self.eng.publish_incremental(extra)
        self.eng.create_snapshot()
        nums = [v["version"] for v in self.eng.overview()["versions"]]
        self.assertEqual(nums, sorted(nums))
        self.assertEqual(nums, [1, 2, 3])


class TestLateQcFencing(EngineTestBase):
    def test_late_qc_result_is_fenced_and_cannot_undo_new_publish(self):
        ids = self._seed_simple()
        late_task = self.eng.create_qc_task(1)
        # 新版本先发布，旧任务才返回
        extra = self.eng.ingest_records([
            dict(record_id="r4", stream="orders", partition_key="华东",
                 metric="订单量", amount=10.0, event_time="t4")])
        self.eng.publish_incremental(extra)
        self.eng.inject_defect("r2", 750.0)
        defect_id = self.eng.overview()["defects"][0]["defect_id"]
        result = self.eng.complete_qc_task(
            late_task["task_id"], False, [defect_id])
        self.assertEqual(result["status"], "fenced")
        ov = self.eng.overview()
        # 没有自动隔离单、没有新版本、缺陷未确认
        self.assertEqual(ov["issues"], [])
        self.assertEqual(ov["latest_version"], 2)
        self.assertEqual(ov["defects"][0]["confirmed"], 0)
        # 任务状态留痕为 fenced
        self.assertEqual(ov["qc_tasks"][0]["status"], "fenced")

    def test_on_time_qc_failure_creates_issue(self):
        ids = self._seed_simple()
        task = self.eng.create_qc_task(1)
        self.eng.inject_defect("r2", 750.0)
        defect_id = self.eng.overview()["defects"][0]["defect_id"]
        result = self.eng.complete_qc_task(task["task_id"], False, [defect_id])
        self.assertEqual(result["status"], "failed")
        issues = self.eng.overview()["issues"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["state"], "isolated")

    def test_completed_task_cannot_be_completed_again(self):
        self._seed_simple()
        task = self.eng.create_qc_task(1)
        self.eng.complete_qc_task(task["task_id"], True)
        with self.assertRaises(DomainError):
            self.eng.complete_qc_task(task["task_id"], True)


class TestImpactTracking(EngineTestBase):
    def test_impact_partitions_versions_and_consumers(self):
        self._seed_simple()
        c1 = self.eng.register_consumer("已读者", "idempotent")
        c2 = self.eng.register_consumer("落后者", "idempotent")
        self.eng.deliver_versions(c1["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        impact = self.eng.issue_impact(issue["issue_id"])
        self.assertEqual(impact["issue"]["partition_key"], "华南")
        self.assertEqual(impact["affected_versions"], [1])
        self.assertEqual([c["consumer_id"] for c in impact["read_consumers"]],
                         [c1["consumer_id"]])
        self.assertEqual([c["consumer_id"] for c in impact["unread_consumers"]],
                         [c2["consumer_id"]])
        self.assertAlmostEqual(impact["delta"], 250.0)


class TestCompensationFlow(EngineTestBase):
    def test_idempotent_consumer_crash_retry_no_double_reversal(self):
        self._seed_simple()
        c = self.eng.register_consumer("幂等消费者", "idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])  # v2
        # 投递补偿时在处理中途崩溃（单条补偿：崩溃点即首条，半版已落库）
        with self.assertRaises(DeliveryCrash):
            self.eng.deliver_versions(c["consumer_id"], crash_phase="mid")
        ov = self.eng.overview()
        consumer = next(x for x in ov["consumers"] if x["consumer_id"] == c["consumer_id"])
        # 崩溃后 offset 未推进，轨迹保留 crashed
        self.assertEqual(consumer["last_offset"], 1)
        self.assertEqual(
            [t["status"] for t in consumer["tracks"] if t["version"] == 2], ["crashed"])
        # 重试：幂等键跳过已应用单元，不重复冲减
        retry = self.eng.deliver_versions(c["consumer_id"])
        self.assertGreaterEqual(retry["skipped_units"], 1)
        recon = self.eng.reconcile()
        cr = next(x for x in recon["consumers"] if x["consumer_id"] == c["consumer_id"])
        self.assertTrue(cr["matched"], cr["diffs"])
        # 幂等键只有一条，证明没有重复写入
        with sqlite3.connect(self.db) as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM idempotency_keys WHERE consumer_id=?",
                (c["consumer_id"],)).fetchone()[0]
            self.assertEqual(n, 4)  # 3 条增量 + 1 条补偿

    def test_before_commit_crash_leaves_no_partial_effect(self):
        self._seed_simple()
        c = self.eng.register_consumer("幂等消费者", "idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        with self.assertRaises(DeliveryCrash):
            self.eng.deliver_versions(c["consumer_id"], crash_phase="before_commit")
        # 补偿完全没落库，重试后一次到位
        self.eng.deliver_versions(c["consumer_id"])
        recon = self.eng.reconcile()
        self.assertTrue(recon["all_matched"])

    def test_non_idempotent_consumer_duplicate_risk_is_visible(self):
        self._seed_simple()
        c = self.eng.register_consumer("非幂等消费者", "non_idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        with self.assertRaises(DeliveryCrash):
            self.eng.deliver_versions(c["consumer_id"], crash_phase="mid")
        self.eng.deliver_versions(c["consumer_id"])  # 重试 → 差额被应用两次
        recon = self.eng.reconcile()
        cr = next(x for x in recon["consumers"] if x["consumer_id"] == c["consumer_id"])
        self.assertFalse(cr["matched"])
        diff = next(x for x in cr["diffs"] if x["partition_key"] == "华南")
        self.assertAlmostEqual(diff["difference"], 250.0)  # 多冲减/补提一次

    def test_laggard_skips_bad_value_jitter(self):
        """落后消费者没读过坏版本，补偿版本直接给出正确累计。"""
        self._seed_simple()
        laggard = self.eng.register_consumer("落后者", "idempotent")
        self.eng.deliver_versions(laggard["consumer_id"], target_version=0)  # 什么都没读
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        self.eng.deliver_versions(laggard["consumer_id"])  # 一次性到 v2
        recon = self.eng.reconcile()
        self.assertTrue(recon["all_matched"])


class TestSnapshotFlow(EngineTestBase):
    def test_repair_snapshot_replaces_baseline_and_reconciles(self):
        self._seed_simple()
        c = self.eng.register_consumer("快照消费者", "idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("snapshot", [issue["issue_id"]])  # v2 修复快照
        # 快照投递在提交前崩溃：重试必须安全（快照键去重）
        with self.assertRaises(DeliveryCrash):
            self.eng.deliver_versions(c["consumer_id"], crash_phase="before_commit")
        self.eng.deliver_versions(c["consumer_id"])
        recon = self.eng.reconcile()
        self.assertTrue(recon["all_matched"])
        # 再次投递（无新版本）是 no-op
        again = self.eng.deliver_versions(c["consumer_id"])
        self.assertEqual(again["applied_versions"], [])

    def test_misusing_snapshot_as_incremental_doubles_values(self):
        self._seed_simple()
        c = self.eng.register_consumer("错误接线消费者", "idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.create_snapshot()  # v2 普通快照
        self.eng.apply_snapshot_as_incremental(c["consumer_id"], 2)
        recon = self.eng.reconcile()
        cr = next(x for x in recon["consumers"] if x["consumer_id"] == c["consumer_id"])
        self.assertFalse(cr["matched"])
        # 每个分区×指标都被多加了一份全量
        for diff in cr["diffs"]:
            exp = next(e for e in recon["expected_totals"]
                       if e["partition_key"] == diff["partition_key"]
                       and e["metric"] == diff["metric"])
            self.assertAlmostEqual(diff["difference"], exp["total"])


class TestIncrementDuringDisposition(EngineTestBase):
    def test_normal_increments_continue_alongside_repair(self):
        ids = self._seed_simple()  # v1
        c = self.eng.register_consumer("主消费者", "idempotent")
        self.eng.deliver_versions(c["consumer_id"])
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        # 处置期间正常增量继续
        extra = self.eng.ingest_records([
            dict(record_id="r5", stream="orders", partition_key="华南",
                 metric="销售额", amount=100.0, event_time="t5")])
        self.eng.publish_incremental(extra, note="处置期间正常增量")  # v2
        self.eng.execute_disposition("compensation", [issue["issue_id"]])  # v3 补偿
        self.eng.deliver_versions(c["consumer_id"])
        recon = self.eng.reconcile()
        self.assertTrue(recon["all_matched"])
        cr = next(x for x in recon["consumers"] if x["consumer_id"] == c["consumer_id"])
        south = next(s for s in self.eng.overview()["consumers"][0]["state"]
                     if s["partition_key"] == "华南")
        # 500(坏) + 100(新增量) + 250(补偿) = 850 = 真值 750+100
        self.assertAlmostEqual(south["total"], 850.0)


class TestPlanComparison(EngineTestBase):
    def test_plan_contains_real_scope_and_consumer_risk(self):
        self._seed_simple()
        self.eng.register_consumer("幂等", "idempotent")
        self.eng.register_consumer("非幂等", "non_idempotent")
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        plan = self.eng.plan_disposition([issue["issue_id"]])
        comp, snap = plan["options"]["compensation"], plan["options"]["snapshot"]
        self.assertEqual(comp["scope_size"], 1)       # 仅华南/销售额
        self.assertEqual(snap["scope_size"], 3)       # 全表 3 个分区×指标
        risks = {x["name"]: x["compensation_risk"] for x in plan["consumers"]}
        self.assertIn("拦截", risks["幂等"])
        self.assertIn("重复冲减", risks["非幂等"])

    def test_repaired_issue_cannot_be_disposed_again(self):
        self._seed_simple()
        self.eng.inject_defect("r2", 750.0)
        d = self.eng.overview()["defects"][0]
        self.eng.confirm_defect_manually(d["defect_id"])
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        with self.assertRaises(DomainError):
            self.eng.execute_disposition("compensation", [issue["issue_id"]])


class TestDemoSeed(EngineTestBase):
    def test_seed_storyline_and_full_repair_path(self):
        info = self.eng.seed_demo()
        ov = self.eng.overview()
        self.assertEqual(ov["latest_version"], 2)
        self.assertEqual(len(ov["consumers"]), 3)
        # 完成 v2 准时质检 → 隔离单
        r = self.eng.complete_qc_task(info["qc_v2_task"], False, [info["defect_id"]])
        self.assertEqual(r["status"], "failed")
        # v1 的复检任务迟到 → 围栏
        fenced = self.eng.complete_qc_task(info["qc_late_task"], True)
        self.assertEqual(fenced["status"], "fenced")
        issue = self.eng.overview()["issues"][0]
        self.eng.execute_disposition("compensation", [issue["issue_id"]])
        # 三个消费者各自追到最新
        for cid in info["consumers"].values():
            self.eng.deliver_versions(cid)
        self.assertTrue(self.eng.reconcile()["all_matched"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
