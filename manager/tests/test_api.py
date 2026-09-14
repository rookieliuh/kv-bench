"""FastAPI route tests for the manager.

Skipped automatically when fastapi/httpx are not installed (e.g. offline
environments that still run the domain-level test_manager.py).
"""

import os
import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient

    from manager.api import create_app
    from manager.app import (
        DeploymentManager,
        DeployStatusStore,
        HttpWorkerClient,
        Node,
        NodeStore,
        RunArtifacts,
        TaskStore,
    )

    HAVE_FASTAPI = True
    # 测试用：server 就绪探测默认通过（生产实现为 TCP 连接探测）
    DeploymentManager.default_server_probe = lambda node, port: True
except Exception:  # pragma: no cover - exercised only without third-party deps
    HAVE_FASTAPI = False


class FakeExecutor:
    def __init__(self, versions):
        self.versions = versions
        self.calls = []

    def run(self, node, command):
        self.calls.append((node.name, tuple(command)))
        if command[:2] == ["rpm", "-qa"]:
            return self.versions[node.name]
        return "ok"

    def copy(self, node, source, destination):
        self.calls.append((node.name, "copy", source, destination))


class FailingExecutor(FakeExecutor):
    def copy(self, node, source, destination):
        raise RuntimeError(f"scp to {node.name} failed: remote dir missing")


class FakeWorkerClient:
    def __init__(self):
        self.started = []
        self.stopped = []

    def health(self, node, port=None):
        return {"state": "ready"}

    def start(self, node, task_id, command, port=None):
        self.started.append((node.name, task_id, command, port))

    def stop(self, node, task_id, port=None):
        self.stopped.append((node.name, task_id, port))

    def result(self, node, task_id, port=None):
        return {"state": "ready", "ops": 10 if node.name == "a" else 20, "bytes": 100, "errors": 1}


class DownWorkerClient(FakeWorkerClient):
    def health(self, node, port=None):
        raise RuntimeError("worker down")


@unittest.skipUnless(HAVE_FASTAPI, "fastapi/httpx not installed")
class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        store = NodeStore(Path(self.tmpdir.name) / "nodes.json")
        self.task_store = TaskStore(Path(self.tmpdir.name) / "tasks.json")
        self.deploy_status = DeployStatusStore(Path(self.tmpdir.name) / "deploy_status.json")
        self.artifacts = RunArtifacts(Path(self.tmpdir.name) / "runs")
        self.manager = DeploymentManager(
            FakeExecutor({"a": "liburma-1.0", "b": "liburma-1.0"}),
            FakeWorkerClient(), store,
            self.task_store, self.deploy_status, self.artifacts,
        )
        self.client = TestClient(create_app(self.manager, dist_dir=Path(self.tmpdir.name) / "dist"))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_nodes_crud_roundtrip_without_password_leak(self):
        response = self.client.post("/v1/nodes", json={
            "name": "a", "ip": "10.0.0.1", "password": "secret",
        })
        self.assertEqual(response.status_code, 201)
        nodes = self.client.get("/v1/nodes").json()
        self.assertEqual([node["name"] for node in nodes], ["a"])
        self.assertNotIn("password", nodes[0])
        response = self.client.delete("/v1/nodes/a")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get("/v1/nodes").json(), [])

    def test_node_tags_exposed_in_api(self):
        response = self.client.post("/v1/nodes", json={
            "name": "g1", "ip": "10.0.0.9", "password": "secret", "tags": ["gpu", "fast"],
        })
        self.assertEqual(response.status_code, 201)
        nodes = self.client.get("/v1/nodes").json()
        self.assertEqual(nodes[0]["tags"], ["gpu", "fast"])
        self.assertNotIn("password", nodes[0])

    def test_patch_node_tags_preserves_other_fields(self):
        self.client.post("/v1/nodes", json={
            "name": "a", "ip": "10.0.0.1", "password": "keep", "tags": ["gpu"],
        })
        response = self.client.patch("/v1/nodes/a", json={"tags": ["gpu", "idle", "idle", " fast "]})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["tags"], ["gpu", "idle", "fast"])  # 归一化去重
        self.assertEqual(body["ip"], "10.0.0.1")
        self.assertNotIn("password", body)
        # 密码与其余字段保持不变
        self.assertEqual(self.manager.node_store.get("a").password, "keep")
        self.assertEqual(self.manager.node_store.get("a").ip, "10.0.0.1")
        # 清空标签
        cleared = self.client.patch("/v1/nodes/a", json={"tags": []})
        self.assertEqual(cleared.status_code, 200)
        self.assertEqual(cleared.json()["tags"], [])
        # 节点不存在 -> 404
        missing = self.client.patch("/v1/nodes/nope", json={"tags": ["x"]})
        self.assertEqual(missing.status_code, 404)

    def test_task_workers_include_tags_without_password(self):
        create = self.client.post("/v1/tasks", json={
            "task_id": "t-tags",
            "workers": [
                {"name": "a", "ip": "10.0.0.1", "password": "x", "tags": ["gpu"]},
                {"name": "b", "ip": "10.0.0.2", "tags": ["fast", "gpu"]},
            ],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        self.assertEqual(create.status_code, 201)
        tasks = self.client.get("/v1/tasks").json()
        self.assertEqual(tasks[0]["workers"]["a"]["tags"], ["gpu"])
        self.assertEqual(tasks[0]["workers"]["b"]["tags"], ["fast", "gpu"])
        self.assertNotIn("password", str(tasks))

    def test_deploy_reports_version_consistency(self):
        response = self.client.post("/v1/deploy", json={
            "nodes": [
                {"name": "a", "ip": "10.0.0.1"},
                {"name": "b", "ip": "10.0.0.2"},
            ],
            "artifact": "build/kv-bench",
            "destination": "/opt/kv-bench/build/kv-bench",
            "source_dir": "/opt/kv-bench",
            "umdk_root": "/opt/umdk",
        })
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["consistent"])
        self.assertEqual(body["versions"]["a"], ["liburma-1.0"])

    def test_deploy_without_umdk_root_is_optional(self):
        response = self.client.post("/v1/deploy", json={
            "nodes": [{"name": "a", "ip": "10.0.0.1"}],
            "artifact": "build/kv-bench",
            "destination": "/opt/kv-bench/build/kv-bench",
            "source_dir": "/opt/kv-bench",
            # 省略 umdk_root -> 编译时（若有）不加 -DUMDK_ROOT
        })
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["consistent"])

    def test_deploy_failure_returns_clear_error(self):
        manager = DeploymentManager(
            FailingExecutor({"a": "liburma-1.0"}), FakeWorkerClient(),
            NodeStore(Path(self.tmpdir.name) / "nodes.json"),
        )
        client = TestClient(create_app(manager, dist_dir=Path(self.tmpdir.name) / "dist"))
        response = client.post("/v1/deploy", json={
            "nodes": [{"name": "a", "ip": "10.0.0.1"}],
            "artifact": "build/kv-bench",
            "destination": "/opt/kv-bench/build/kv-bench",
            "source_dir": "/opt/kv-bench",
        })
        self.assertEqual(response.status_code, 500)
        self.assertIn("scp to a failed", response.json()["error"])

    def test_task_lifecycle_and_result(self):
        create = self.client.post("/v1/tasks", json={
            "task_id": "t1",
            "workers": [
                {"name": "a", "ip": "10.0.0.1", "password": "hunter2"},
                {"name": "b", "ip": "10.0.0.2", "password": "hunter2"},
            ],
            "bench_items": [{"src": "a", "dst": "b", "type": "bidirectional"}],
            "options": {"op": "write", "threads": 4, "duration": 30},
        })
        self.assertEqual(create.status_code, 201)
        self.assertEqual(create.json()["state"], "queued")

        tasks = self.client.get("/v1/tasks").json()
        self.assertEqual(len(tasks), 1)
        self.assertNotIn("password", tasks[0]["workers"]["a"])
        self.assertNotIn("password", str(tasks))

        start = self.client.post("/v1/tasks/t1/start")
        self.assertEqual(start.status_code, 200)
        self.assertEqual(start.json()["state"], "running")
        self.assertEqual(len(start.json()["commands"]), 2)
        self.assertEqual(start.json()["worker_ports"], {"a": [18082], "b": [18083]})

        result = self.client.get("/v1/tasks/t1/result")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["aggregate"]["ops"], 30)

        stop = self.client.post("/v1/tasks/t1/stop")
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(stop.json()["state"], "stopped")
        # 停止后 worker 已回收，再次收集返回上次缓存结果而非全零
        after_stop = self.client.get("/v1/tasks/t1/result").json()
        self.assertEqual(after_stop["aggregate"]["ops"], 30)

    def test_task_restart_after_stop(self):
        self.client.post("/v1/tasks", json={
            "task_id": "t1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        self.assertEqual(self.client.post("/v1/tasks/t1/start").status_code, 200)
        self.assertEqual(self.client.post("/v1/tasks/t1/stop").status_code, 200)
        restart = self.client.post("/v1/tasks/t1/start")
        self.assertEqual(restart.status_code, 200)
        self.assertEqual(restart.json()["state"], "running")
        self.assertEqual(restart.json()["worker_ports"], {"a": [18082], "b": [18083]})

    def test_task_update_and_delete_endpoints(self):
        self.client.post("/v1/tasks", json={
            "task_id": "t1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        update = self.client.put("/v1/tasks/t1", json={
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "c", "ip": "10.0.0.3"}],
            "bench_items": [{"src": "a", "dst": "c", "type": "bidirectional"}],
            "options": {"threads": 8},
        })
        self.assertEqual(update.status_code, 200)
        self.assertEqual(update.json()["state"], "queued")
        tasks = self.client.get("/v1/tasks").json()
        self.assertEqual(tasks[0]["bench_items"][0]["dst"], "c")
        self.assertEqual(tasks[0]["bench_items"][0]["type"], "bidirectional")
        # 删除
        deleted = self.client.delete("/v1/tasks/t1")
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(deleted.json()["state"], "deleted")
        self.assertEqual(self.client.get("/v1/tasks").json(), [])
        self.assertEqual(self.client.delete("/v1/tasks/t1").status_code, 404)
        self.assertEqual(self.client.put("/v1/tasks/nope", json={
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b"}],
        }).status_code, 404)

    def test_errors_map_to_http_status(self):
        missing = self.client.post("/v1/tasks/nope/start")
        self.assertEqual(missing.status_code, 404)
        duplicate = self.client.post("/v1/tasks", json={
            "task_id": "dup",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b"}],
        })
        self.assertEqual(duplicate.status_code, 201)
        duplicate_again = self.client.post("/v1/tasks", json={
            "task_id": "dup",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b"}],
        })
        self.assertEqual(duplicate_again.status_code, 400)
        bad_item = self.client.post("/v1/tasks", json={
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "a"}],
        })
        self.assertEqual(bad_item.status_code, 400)

    def test_index_serves_hint_without_built_frontend(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("kv-bench Manager", response.text)
        self.assertIn("npm run build", response.text)

    def test_tasks_persist_across_manager_restart(self):
        create = self.client.post("/v1/tasks", json={
            "task_id": "persist-1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        self.assertEqual(create.status_code, 201)
        manager2 = DeploymentManager(
            FakeExecutor({"a": "u", "b": "u"}), FakeWorkerClient(),
            self.manager.node_store, self.task_store, self.deploy_status, self.artifacts,
        )
        client2 = TestClient(create_app(manager2, dist_dir=Path(self.tmpdir.name) / "dist"))
        tasks = client2.get("/v1/tasks").json()
        self.assertEqual([task["task_id"] for task in tasks], ["persist-1"])
        self.assertEqual(tasks[0]["state"], "queued")
        self.assertNotIn("password", str(tasks))

    def test_task_logs_endpoint_saves_artifacts(self):
        self.client.post("/v1/tasks", json={
            "task_id": "lt1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        response = self.client.get("/v1/tasks/lt1/logs")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["task_id"], "lt1")
        self.assertIn("a", body["workers"])
        self.assertEqual(body["workers"]["a"]["content"], "ok")  # FakeExecutor tail 输出
        self.assertIn("logs collected", body["manager_log"])
        result_dir = Path(self.tmpdir.name) / "runs" / "lt1"
        self.assertTrue((result_dir / "logs" / "a.log").exists())
        self.assertTrue((result_dir / "task.json").exists())
        self.assertEqual(self.client.get("/v1/tasks/nope/logs").status_code, 404)

    def test_nodes_include_deploy_status(self):
        self.client.post("/v1/nodes", json={"name": "a", "ip": "10.0.0.1"})
        deploy = self.client.post("/v1/deploy", json={
            "nodes": [{"name": "a", "ip": "10.0.0.1"}],
            "artifact": "build/kv-bench",
            "destination": "/opt/kv-bench/build/kv-bench",
            "source_dir": "/opt/kv-bench",
            "umdk_root": "/opt/umdk",
        })
        self.assertEqual(deploy.status_code, 200)
        nodes = self.client.get("/v1/nodes").json()
        self.assertIsNotNone(nodes[0]["deploy"])
        self.assertEqual(nodes[0]["deploy"]["worker_state"], "deployed")  # 仅编译+拷贝
        self.assertEqual(nodes[0]["deploy"]["versions"], ["liburma-1.0"])
        self.assertTrue(nodes[0]["deploy"]["consistent"])
        # 部署状态持久化到磁盘
        restored = DeployStatusStore(Path(self.tmpdir.name) / "deploy_status.json")
        self.assertEqual(restored.get("a")["worker_state"], "deployed")

    def test_task_start_failure_returns_error_and_keeps_queued(self):
        manager = DeploymentManager(
            FakeExecutor({"a": "liburma-1.0"}), DownWorkerClient(),
            NodeStore(Path(self.tmpdir.name) / "nodes.json"),
        )
        manager.worker_start_attempts = 1
        manager.worker_start_interval = 0
        manager.port_attempts = 1
        client = TestClient(create_app(manager, dist_dir=Path(self.tmpdir.name) / "dist"))
        client.post("/v1/tasks", json={
            "task_id": "t-fail",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        response = client.post("/v1/tasks/t-fail/start")
        self.assertEqual(response.status_code, 500)
        self.assertIn("did not become healthy", response.json()["error"])
        tasks = client.get("/v1/tasks").json()
        self.assertEqual(tasks[0]["state"], "queued")
        self.assertEqual(manager.ports.used_ports(), [])


@unittest.skipUnless(HAVE_FASTAPI, "fastapi/httpx not installed")
class WorkerClientTests(unittest.TestCase):
    """worker 客户端必须绕过环境代理（内网控制面直连），否则 504。"""

    def test_worker_client_bypasses_env_proxy(self):
        import http.server
        import socketserver
        import threading

        seen: dict[str, str] = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                seen["path"] = self.path
                body = b'{"state":"running"}'
                self.send_response(202)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            # 塞入必定失败的代理环境变量；若客户端跟随环境代理将无法直连
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
                os.environ[key] = "http://127.0.0.1:1"
            try:
                node = Node("w", "127.0.0.1", api_port=server.server_address[1])
                HttpWorkerClient().start(node, "t1", ["kv-bench", "--op=write"])
                self.assertEqual(seen.get("path"), "/v1/tasks/start")
            finally:
                for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
                    os.environ.pop(key, None)
                server.shutdown()


if __name__ == "__main__":
    unittest.main()
