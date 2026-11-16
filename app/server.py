"""HTTP 层：标准库 ThreadingHTTPServer，提供 JSON API 与静态网页。

所有状态写操作都进入 QualityEngine 的进程锁，线程安全。
"""

from __future__ import annotations

import json
import os
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .engine import DeliveryCrash, DomainError, QualityEngine

WEB_ROOT = Path(__file__).resolve().parent.parent / "web"

MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


def make_handler(engine: QualityEngine):
    class Handler(BaseHTTPRequestHandler):
        server_version = "QualityDisposition/1.0"

        def log_message(self, fmt, *args):  # 精简日志
            print("[http]", fmt % args)

        # ------------------------------------------------------------ 工具
        def _json(self, status: int, obj) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode())
            except json.JSONDecodeError as exc:
                raise DomainError(f"请求体不是合法 JSON: {exc}")

        def _static(self, path: str) -> None:
            rel = "index.html" if path in ("", "/") else path.lstrip("/")
            target = (WEB_ROOT / rel).resolve()
            if WEB_ROOT not in target.parents or not target.is_file():
                self.send_error(404, "Not Found")
                return
            data = target.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", MIME.get(target.suffix, "application/octet-stream"))
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        # ------------------------------------------------------------ 路由
        def do_GET(self):
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/api/state":
                    return self._json(200, engine.overview())
                if parsed.path == "/api/reconcile":
                    return self._json(200, engine.reconcile())
                if parsed.path.startswith("/api/"):
                    raise DomainError("该路径不支持 GET")
                return self._static(parsed.path)
            except DomainError as e:
                return self._json(400, {"error": str(e)})
            except Exception:
                traceback.print_exc()
                return self._json(500, {"error": "服务器内部错误"})

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                body = self._body()
                result = self._route(path, body)
                return self._json(200, result)
            except DomainError as e:
                return self._json(400, {"error": str(e)})
            except DeliveryCrash as e:
                # 崩溃是“预期内”的模拟结果：200 + crashed 标记，页面按正常事件渲染
                return self._json(200, {"crashed": True, "error": str(e)})
            except Exception:
                traceback.print_exc()
                return self._json(500, {"error": "服务器内部错误"})

        def _route(self, path: str, b: dict) -> dict:
            e = engine

            if path == "/api/demo/seed":
                return e.seed_demo()
            if path == "/api/reset":
                e.reset_all()
                return {"ok": True}

            if path == "/api/ingest":
                return {"record_ids": e.ingest_records(b["records"], b.get("note", ""))}
            if path == "/api/publish-incremental":
                return e.publish_incremental(b["record_ids"], b.get("note", ""))
            if path == "/api/snapshot":
                return e.create_snapshot(b.get("note", ""))

            if path == "/api/defect/inject":
                return e.inject_defect(b["record_id"], float(b["correct_amount"]),
                                       b.get("note", ""))
            if path == "/api/defect/confirm":
                return e.confirm_defect_manually(b["defect_id"])

            if path == "/api/qc/create":
                return e.create_qc_task(int(b["version"]))
            if path == "/api/qc/complete":
                return e.complete_qc_task(
                    b["task_id"], bool(b["passed"]), b.get("defect_ids") or [])

            if path == "/api/issue/impact":
                return e.issue_impact(b["issue_id"])
            if path == "/api/disposition/plan":
                return e.plan_disposition(b["issue_ids"])
            if path == "/api/disposition/execute":
                return e.execute_disposition(b["strategy"], b["issue_ids"],
                                             b.get("note", ""))

            if path == "/api/consumer/register":
                return e.register_consumer(b["name"], b["mode"])
            if path == "/api/consumer/deliver":
                tv = b.get("target_version")
                return e.deliver_versions(b["consumer_id"],
                                          int(tv) if tv is not None else None,
                                          b.get("crash_phase"))
            if path == "/api/consumer/misuse-snapshot":
                return e.apply_snapshot_as_incremental(b["consumer_id"], int(b["version"]))
            if path == "/api/consumer/reset":
                return e.reset_consumer_state(b["consumer_id"])

            raise DomainError(f"未知接口: {path}")

    return Handler


def serve(db_path: str, host: str = "0.0.0.0", port: int = 8000) -> None:
    engine = QualityEngine(db_path)
    httpd = ThreadingHTTPServer((host, port), make_handler(engine))
    print(f"质量处置平台已启动: http://{host}:{port}/  (db={db_path})")
    httpd.serve_forever()


if __name__ == "__main__":
    serve(
        os.environ.get("QUALITY_DB", "data/quality.db"),
        port=int(os.environ.get("PORT", "8000")),
    )
