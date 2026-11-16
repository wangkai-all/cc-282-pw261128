"""HTTP 端到端测试：通过真实 HTTP 接口走完整条质量处置故事线。

启动一个临时端口的服务，用标准库 urllib 调用，无需第三方依赖。
运行：python -I -m unittest tests.test_api -v
"""

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.server import make_handler
from app.engine import QualityEngine


class ApiTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls.tmp.name) / "api.db")
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(QualityEngine(cls.db)))
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.httpd.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.tmp.cleanup()

    def call(self, path, body=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def state(self):
        _, d = self.call("/api/state")
        return d


class TestFullStoryOverHttp(ApiTestBase):
    def test_compensation_storyline_with_crash_and_reconcile(self):
        # 演示场景
        _, seed = self.call("/api/demo/seed", {})
        st = self.state()
        self.assertEqual(st["latest_version"], 2)
        self.assertEqual(len(st["consumers"]), 3)

        # 准时质检失败 → 隔离单
        _, qc = self.call("/api/qc/complete",
                          {"task_id": seed["qc_v2_task"], "passed": False,
                           "defect_ids": [seed["defect_id"]]})
        self.assertEqual(qc["status"], "failed")

        # 迟到质检 → 围栏
        _, fenced = self.call("/api/qc/complete",
                              {"task_id": seed["qc_late_task"], "passed": True})
        self.assertEqual(fenced["status"], "fenced")
        self.assertEqual(self.state()["latest_version"], 2)  # 没有新版本被撤销/产生

        issue_id = self.state()["issues"][0]["issue_id"]

        # 影响面
        _, impact = self.call("/api/issue/impact", {"issue_id": issue_id})
        self.assertEqual(impact["issue"]["partition_key"], "华南")
        self.assertTrue(impact["read_consumers"])
        self.assertTrue(impact["unread_consumers"])

        # 方案对比
        _, plan = self.call("/api/disposition/plan", {"issue_ids": [issue_id]})
        self.assertEqual(plan["options"]["compensation"]["scope_size"], 1)
        self.assertEqual(plan["options"]["snapshot"]["scope_size"], 5)

        # 处置期间正常增量继续
        _, ing = self.call("/api/ingest", {"records": [
            dict(record_id="r2001", stream="orders", partition_key="华南",
                 metric="销售额", amount=77.0, event_time="2026-11-16T08:00:00Z")]})
        _, pub = self.call("/api/publish-incremental",
                           {"record_ids": ing["record_ids"], "note": "处置期增量"})
        self.assertEqual(pub["version"], 3)

        # 执行补偿 → v4
        _, disp = self.call("/api/disposition/execute",
                            {"strategy": "compensation", "issue_ids": [issue_id]})
        self.assertEqual(disp["repair_version"], 4)

        # 非幂等消费者：mid 崩溃 + 重试，风险应在对账中暴露
        naive = seed["consumers"]["naive"]
        code, crash = self.call("/api/consumer/deliver",
                                {"consumer_id": naive, "crash_phase": "mid"})
        self.assertEqual(code, 200)
        self.assertTrue(crash["crashed"])
        self.call("/api/consumer/deliver", {"consumer_id": naive})
        _, recon = self.call("/api/reconcile")
        naive_r = next(c for c in recon["consumers"] if c["consumer_id"] == naive)
        self.assertFalse(naive_r["matched"])  # 非幂等重复处理如实暴露

        # 幂等消费者：同样 mid 崩溃 + 重试，对账必须一致
        eager = seed["consumers"]["eager"]
        self.call("/api/consumer/deliver",
                  {"consumer_id": eager, "crash_phase": "mid"})
        self.call("/api/consumer/deliver", {"consumer_id": eager})
        _, recon = self.call("/api/reconcile")
        eager_r = next(c for c in recon["consumers"] if c["consumer_id"] == eager)
        self.assertTrue(eager_r["matched"], eager_r["diffs"])

        # 落后者一次性追上，同样一致
        laggard = seed["consumers"]["laggard"]
        self.call("/api/consumer/deliver", {"consumer_id": laggard})
        _, recon = self.call("/api/reconcile")
        laggard_r = next(c for c in recon["consumers"] if c["consumer_id"] == laggard)
        self.assertTrue(laggard_r["matched"], laggard_r["diffs"])

        # 原始记录从未被修改
        bad = next(r for r in self.state()["raw_records"] if r["record_id"] == "r1004")
        self.assertEqual(bad["amount"], 500.0)
        # 坏版本 v2 仍在账本中
        versions = [v["version"] for v in self.state()["versions"]]
        self.assertIn(2, versions)
        self.assertIn(4, versions)

    def test_snapshot_strategy_and_misuse(self):
        self.call("/api/reset", {})
        _, seed = self.call("/api/demo/seed", {})
        _, qc = self.call("/api/qc/complete",
                          {"task_id": seed["qc_v2_task"], "passed": False,
                           "defect_ids": [seed["defect_id"]]})
        issue_id = self.state()["issues"][0]["issue_id"]
        _, disp = self.call("/api/disposition/execute",
                            {"strategy": "snapshot", "issue_ids": [issue_id]})
        snap_version = disp["repair_version"]

        eager = seed["consumers"]["eager"]
        # 快照投递提交前崩溃，再重试
        code, crash = self.call("/api/consumer/deliver",
                                {"consumer_id": eager, "crash_phase": "before_commit"})
        self.assertTrue(crash["crashed"])
        self.call("/api/consumer/deliver", {"consumer_id": eager})
        _, recon = self.call("/api/reconcile")
        self.assertTrue(next(c for c in recon["consumers"]
                             if c["consumer_id"] == eager)["matched"])

        # 错误接线：快照当增量 → 必然差异
        _, misuse = self.call("/api/consumer/misuse-snapshot",
                              {"consumer_id": eager, "version": snap_version})
        self.assertIn("warning", misuse)
        _, recon = self.call("/api/reconcile")
        self.assertFalse(next(c for c in recon["consumers"]
                              if c["consumer_id"] == eager)["matched"])

    def test_error_routing_and_validation(self):
        code, err = self.call("/api/publish-incremental", {"record_ids": ["nope"]})
        self.assertEqual(code, 400)
        self.assertIn("error", err)
        code, _ = self.call("/api/unknown", {})
        self.assertEqual(code, 400)
        # 静态页
        req = urllib.request.Request(self.base + "/nonexistent.html")
        try:
            urllib.request.urlopen(req)
            self.fail("应当 404")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
