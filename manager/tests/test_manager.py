import json
import tempfile
import unittest
from pathlib import Path

from manager import BenchItem, DeploymentManager, Node, NodeStore, TaskSpec
from manager.app import DeployStatusStore, RunArtifacts, SshExecutor, TaskStore

# 测试用：server 就绪探测默认通过（生产实现为 TCP 连接探测）
DeploymentManager.default_server_probe = lambda node, port: True


class FakeExecutor:
    def __init__(self, versions):
        self.versions = versions
        self.calls = []

    def run(self, node, command):
        self.calls.append((node.name, tuple(command)))
        if command[:3] == ["rpm", "-qa", "|"]:
            return ""
        if command[:2] == ["rpm", "-qa"]:
            return self.versions[node.name]
        return "ok"

    def copy(self, node, source, destination):
        self.calls.append((node.name, "copy", source, destination))


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


class ManagerTests(unittest.TestCase):
    def test_topology_generates_forward_and_reverse_commands(self):
        manager = DeploymentManager(FakeExecutor({"a": "liburma-1.0", "b": "liburma-1.0"}))
        task = TaskSpec(
            task_id="t1",
            workers={"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
            bench_items=[BenchItem("10.0.0.1", "10.0.0.2", "bidirectional")],
        )
        commands = manager.build_commands(task)
        self.assertEqual(len(commands), 2)
        self.assertTrue(all("--direction=bidirectional" in c for c in commands))
        self.assertIn("--peer-ip=10.0.0.2", commands[0])
        self.assertIn("--peer-ip=10.0.0.1", commands[1])

    def test_build_assignments_handles_flag_options(self):
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}))
        task = TaskSpec(
            task_id="t2",
            workers={"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
            bench_items=[BenchItem("a", "b", "forward")],
            options={
                "event_mode": True,   # 开关 True -> --event-mode
                "cacheable": False,   # 默认关开关 False -> 不传
                "mbind": False,       # 默认开开关 False -> --no-mbind
                "drv_ext": True,      # 默认开开关 True -> --drv-ext
                "seed": 42,           # 标量 -> --seed=42
            },
        )
        commands = manager.build_commands(task)
        first = commands[0]
        self.assertIn("--event-mode", first)
        self.assertNotIn("--cacheable", first)
        self.assertIn("--no-mbind", first)
        self.assertIn("--drv-ext", first)
        self.assertIn("--seed=42", first)
        self.assertIn("--op=write", first)
        self.assertIn("--threads=1", first)

    def test_version_mismatch_compiles_only_mismatching_node(self):
        executor = FakeExecutor({"a": "liburma-1.0", "b": "liburma-2.0"})
        manager = DeploymentManager(executor)
        nodes = [Node("a", "10.0.0.1"), Node("b", "10.0.0.2")]
        result = manager.ensure_urma_consistency(nodes, "/opt/kv-bench", "/opt/umdk")
        self.assertFalse(result.consistent)
        compile_calls = [call for call in executor.calls if "cmake" in call[-1]]
        self.assertEqual([call[0] for call in compile_calls], ["b", "b"])

    def test_compile_node_without_umdk_root_omits_flag(self):
        executor = FakeExecutor({"a": "u"})
        manager = DeploymentManager(executor)
        manager.compile_node(Node("a", "10.0.0.1"), "/opt/kv-bench", "")
        configure = executor.calls[0][1]
        self.assertEqual(configure[0], "cmake")
        self.assertNotIn("-DUMDK_ROOT", configure)
        manager.compile_node(Node("a", "10.0.0.1"), "/opt/kv-bench", "/opt/umdk")
        configure_with = executor.calls[2][1]
        self.assertIn("-DUMDK_ROOT=/opt/umdk", configure_with)

    def test_ssh_executor_copy_missing_source_raises_clear_error(self):
        executor = SshExecutor()
        with self.assertRaisesRegex(RuntimeError, "artifact not found"):
            executor.copy(Node("a", "10.0.0.1"), "/nonexistent/kv-bench-artifact",
                          "/opt/kv-bench/build/kv-bench")

    def test_manager_controls_workers_through_worker_api(self):
        client = FakeWorkerClient()
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), client)
        task = manager.create_task({
            "task_id": "t2",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task(task.task_id)
        # 被动端（server）先下发，主动端（client）等 server 就绪后下发
        self.assertEqual([call[0] for call in client.started], ["b", "a"])
        self.assertNotIn("--peer-ip=10.0.0.1", client.started[0][2])  # b 被动端
        self.assertIn("--peer-ip=10.0.0.2", client.started[1][2])     # a 主动端
        manager.stop_task(task.task_id)
        self.assertEqual([call[0] for call in client.stopped], ["a", "b"])
        result = manager.collect_result(task.task_id)
        self.assertEqual(result["aggregate"]["ops"], 30)
        self.assertEqual(result["aggregate"]["errors"], 2)

    def test_task_worker_ports_staggered_and_released(self):
        executor = FakeExecutor({"a": "u", "b": "u"})
        client = FakeWorkerClient()
        manager = DeploymentManager(executor, client)
        task1 = manager.create_task({
            "task_id": "p1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task("p1")
        self.assertEqual(task1.worker_ports, {"a": [18082], "b": [18083]})
        # 第二个任务端口继续错开
        task2 = manager.create_task({
            "task_id": "p2",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task("p2")
        self.assertEqual(task2.worker_ports, {"a": [18084], "b": [18085]})
        # 停止 p1 后端口回收，p3 复用 18082/18083
        manager.stop_task("p1")
        task3 = manager.create_task({
            "task_id": "p3",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task("p3")
        self.assertEqual(task3.worker_ports, {"a": [18082], "b": [18083]})
        # 停止时对每个节点执行 sh -c "pkill ..." 杀 worker
        pkills = [call for call in executor.calls if call[1] and call[1][0] == "sh"
                  and "pkill" in " ".join(call[1])]
        self.assertEqual(len(pkills), 2)  # p1 的两个节点

    def test_task_restart_after_stop(self):
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), FakeWorkerClient())
        manager.create_task({
            "task_id": "r1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task("r1")
        manager.stop_task("r1")
        self.assertEqual(manager.tasks["r1"].state, "stopped")
        self.assertEqual(manager.tasks["r1"].worker_ports, {})
        # 停止后可再次执行，端口重新分配
        manager.start_task("r1")
        self.assertEqual(manager.tasks["r1"].state, "running")
        self.assertEqual(manager.tasks["r1"].worker_ports, {"a": [18082], "b": [18083]})

    def test_task_update_and_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            artifacts = RunArtifacts(Path(directory) / "runs")
            manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), FakeWorkerClient(),
                                        task_store=store, artifacts=artifacts)
            manager.create_task({
                "task_id": "u1",
                "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
                "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
                "options": {"threads": 2},
            })
            manager.start_task("u1")
            manager.collect_result("u1")
            manager.stop_task("u1")
            # 修改 stopped 任务 -> 回到 queued、结果清空、字段替换
            updated = manager.update_task("u1", {
                "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "c", "ip": "10.0.0.3"}],
                "bench_items": [{"src": "a", "dst": "c", "type": "reverse"}],
                "options": {"threads": 8},
            })
            self.assertEqual(updated.state, "queued")
            self.assertEqual(updated.result, {})
            self.assertEqual(updated.bench_items[0].dst, "c")
            self.assertEqual(updated.bench_items[0].type, "reverse")
            self.assertEqual(updated.options["threads"], 8)
            # 运行中不可修改
            manager.start_task("u1")
            with self.assertRaises(ValueError):
                manager.update_task("u1", {"workers": [], "bench_items": [], "options": {}})
            manager.stop_task("u1")
            # 删除：内存/持久化/运行产物全部移除
            manager.delete_task("u1")
            self.assertNotIn("u1", manager.tasks)
            self.assertEqual(store.values(), [])
            self.assertFalse((Path(directory) / "runs" / "u1").exists())
            with self.assertRaises(KeyError):
                manager.delete_task("u1")

    def test_start_waits_for_server_listening_before_client(self):
        calls: list[tuple[str, str]] = []
        probe_calls: list[tuple[str, int]] = []

        class TrackingClient(FakeWorkerClient):
            def start(self, node, task_id, command, port=None):
                calls.append((node.name, " ".join(command)))
                super().start(node, task_id, command, port=port)

        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), TrackingClient())
        manager.server_probe = lambda node, port: probe_calls.append((node.name, port)) or True
        manager.create_task({
            "task_id": "g1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task("g1")
        # 顺序：被动端 b 先下发 -> 探测 b 的 server 端口就绪 -> 主动端 a 下发
        self.assertEqual([name for name, _ in calls], ["b", "a"])
        self.assertEqual(probe_calls, [("b", 13857)])
        self.assertIn("--no-interactive", calls[0][1])
        self.assertIn("--peer-ip=10.0.0.2", calls[1][1])

    def test_start_fails_if_server_never_listens(self):
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), FakeWorkerClient())
        manager.server_probe = lambda node, port: False
        manager.server_ready_timeout = 0.2
        manager.server_ready_interval = 0.05
        manager.create_task({
            "task_id": "g2",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        with self.assertRaisesRegex(RuntimeError, "did not listen"):
            manager.start_task("g2")
        self.assertEqual(manager.tasks["g2"].state, "queued")
        self.assertEqual(manager.ports.used_ports(), [])

    def test_start_does_not_block_other_operations(self):
        import threading

        entered = threading.Event()
        release = threading.Event()

        class BlockingExecutor(FakeExecutor):
            def run(self, node, command):
                if command and command[0].startswith("nohup"):
                    entered.set()
                    release.wait(5)
                return super().run(node, command)

        manager = DeploymentManager(BlockingExecutor({"a": "u", "b": "u"}), FakeWorkerClient())
        manager.create_task({
            "task_id": "s1",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        outcome: dict[str, object] = {}
        thread = threading.Thread(
            target=lambda: outcome.update(started=manager.start_task("s1")), daemon=True)
        thread.start()
        entered.wait(5)  # spawn 的 ssh（nohup）被卡住中
        # 慢操作不再持有全局锁：此时仍能创建任务
        created = manager.create_task({
            "task_id": "s2",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        self.assertEqual(created.state, "queued")
        release.set()
        thread.join(15)
        self.assertFalse(thread.is_alive())
        self.assertEqual(manager.tasks["s1"].state, "running")

    def test_start_failure_rolls_back_workers(self):
        class DownWorker(FakeWorkerClient):
            def health(self, node, port=None):
                raise RuntimeError("worker down")

        class FailDispatchOnB(FakeWorkerClient):
            def start(self, node, task_id, command, port=None):
                if node.name == "b":
                    raise RuntimeError("dispatch failed")
                super().start(node, task_id, command, port=port)

        def make_task(manager):
            return manager.create_task({
                "task_id": "t",
                "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
                "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
            })

        # 1) 健康探测失败 -> 报错、端口释放、任务保持 queued
        executor = FakeExecutor({"a": "u", "b": "u"})
        manager = DeploymentManager(executor, DownWorker())
        manager.worker_start_attempts = 1
        manager.worker_start_interval = 0
        manager.port_attempts = 1
        make_task(manager)
        with self.assertRaises(RuntimeError):
            manager.start_task("t")
        self.assertEqual(manager.tasks["t"].state, "queued")
        self.assertEqual(manager.tasks["t"].worker_ports, {})
        self.assertEqual(manager.ports.used_ports(), [])

        # 2) 已拉起的节点在下发 bench 时失败 -> 回滚杀掉已拉起的 worker、释放端口
        executor2 = FakeExecutor({"a": "u", "b": "u"})
        manager2 = DeploymentManager(executor2, FailDispatchOnB())
        make_task(manager2)
        with self.assertRaises(RuntimeError):
            manager2.start_task("t")
        self.assertEqual(manager2.tasks["t"].state, "queued")
        self.assertEqual(manager2.tasks["t"].worker_ports, {})
        self.assertEqual(manager2.ports.used_ports(), [])
        pkills = [call for call in executor2.calls if call[1] and call[1][0] == "sh"
                  and "pkill" in " ".join(call[1])]
        self.assertEqual(len(pkills), 2)  # a、b 都已拉起，全部回滚杀掉

    def test_node_store_persists_registered_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nodes.json"
            store = NodeStore(path)
            node = Node("a", "10.0.0.1", password="secret")
            store.upsert(node)
            self.assertEqual(store.get("a"), node)
            restored = NodeStore(path)
            self.assertEqual(restored.list(), [node])
            restored.remove("a")
            self.assertEqual(restored.list(), [])

    def test_node_tags_persist_and_normalize(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nodes.json"
            store = NodeStore(path)
            node = Node("a", "10.0.0.1", tags=["gpu", "gpu", "  fast ", "fast"])
            store.upsert(node)
            # 去空白、去重、保序，元组归一化
            self.assertEqual(store.get("a").tags, ("gpu", "fast"))
            restored = NodeStore(path)
            self.assertEqual(restored.list()[0].tags, ("gpu", "fast"))
            # 无 tags 字段的旧文件兼容
            old = Node("b", "10.0.0.2")
            self.assertEqual(old.tags, ())

    def test_task_store_persists_state_across_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tasks.json"
            manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), task_store=TaskStore(path))
            manager.create_task({
                "task_id": "t1",
                "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
                "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
            })
            manager.start_task("t1")
            manager.stop_task("t1")
            # 用持久化的 store 重建 manager -> 状态不丢
            manager2 = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), task_store=TaskStore(path))
            task = manager2.tasks["t1"]
            self.assertEqual(task.state, "stopped")
            self.assertEqual(task.workers["a"].ip, "10.0.0.1")
            self.assertEqual(task.bench_items[0].type, "forward")

    def test_deploy_status_store_persists(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "deploy_status.json"
            store = DeployStatusStore(path)
            store.update("a", deployed_at="2026-01-01T00:00:00", consistent=True, versions=["liburma-1.0"])
            restored = DeployStatusStore(path)
            self.assertEqual(restored.get("a")["consistent"], True)
            self.assertEqual(restored.get("a")["versions"], ["liburma-1.0"])
            self.assertIsNone(restored.get("nope"))

    def test_run_artifacts_save_result_and_fetch_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            artifacts = RunArtifacts(directory)
            executor = FakeExecutor({"a": "u", "b": "u"})
            task = TaskSpec(
                "t1",
                {"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
                [BenchItem("a", "b", "forward")],
            )
            artifacts.save_result(task, {"aggregate": {"ops": 42}})
            artifacts.append_event("t1", "created")
            logs = artifacts.fetch_logs(executor, task)
            self.assertEqual(set(logs), {"a", "b"})
            base = Path(directory) / "t1"
            self.assertEqual(json.loads((base / "result.json").read_text())["aggregate"]["ops"], 42)
            for name in ("a", "b"):
                self.assertEqual((base / "logs" / f"{name}.log").read_text(), "ok")
            data = artifacts.read_logs("t1")
            self.assertEqual(data["workers"]["a"]["content"], "ok")
            self.assertIn("created", data["manager_log"])


if __name__ == "__main__":
    unittest.main()
