"""Standalone manager for deploying and scheduling kv-bench.

Workers are data-plane processes only.  All manager-side operations use SSH/SCP;
there is deliberately no manager client or control socket in kv-bench.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Protocol

try:
    import httpx
except ImportError:  # domain layer stays usable without third-party deps
    httpx = None


def atomic_write_json(path: str | os.PathLike[str], data: Any) -> None:
    """原子写 JSON：临时文件 + os.replace，避免写一半损坏。"""
    path = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(path)) or "."
    temporary = os.path.join(directory, f".{os.path.basename(path)}.tmp")
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    os.replace(temporary, path)


@dataclass(frozen=True)
class Node:
    name: str
    ip: str
    user: str = "root"
    ssh_port: int = 22
    workdir: str = "/opt/kv-bench"
    binary: str = "/opt/kv-bench/build/kv-bench"
    api_port: int = 18082
    password: str = field(default="", repr=False)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # 标签归一化：去空白、去重、保持顺序；兼容旧 nodes.json（无 tags 字段）。
        seen: list[str] = []
        for tag in self.tags:
            tag = tag.strip()
            if tag and tag not in seen:
                seen.append(tag)
        object.__setattr__(self, "tags", tuple(seen))


class NodeStore:
    def __init__(self, path: str | os.PathLike[str] = "nodes.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._nodes: dict[str, Node] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as stream:
                self._nodes = {entry["name"]: Node(**entry) for entry in json.load(stream)}

    def _save(self) -> None:
        atomic_write_json(self.path, [asdict(node) for node in self.list()])

    def upsert(self, node: Node) -> None:
        with self._lock:
            self._nodes[node.name] = node
            self._save()

    def remove(self, name: str) -> None:
        with self._lock:
            if self._nodes.pop(name, None) is None:
                raise KeyError(name)
            self._save()

    def get(self, name: str) -> Node:
        with self._lock:
            return self._nodes[name]

    def list(self) -> list[Node]:
        return sorted(self._nodes.values(), key=lambda node: node.name)


class TaskStore:
    """任务状态持久化（tasks.json）：manager 重启后任务与状态不丢。"""

    def __init__(self, path: str | os.PathLike[str] = "tasks.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._tasks: dict[str, TaskSpec] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as stream:
            for entry in json.load(stream):
                task = TaskSpec(
                    task_id=entry["task_id"],
                    workers={name: Node(**worker) for name, worker in entry["workers"].items()},
                    bench_items=[BenchItem(**item) for item in entry["bench_items"]],
                    options=entry.get("options", {}),
                    state=entry.get("state", "queued"),
                    result=entry.get("result", {}),
                    worker_ports=entry.get("worker_ports", {}),
                )
                self._tasks[task.task_id] = task

    def _save(self) -> None:
        # 调用方已持有 self._lock（upsert 内），直接遍历，避免重入死锁
        atomic_write_json(self.path, [asdict(task) for task in self._tasks.values()])

    def upsert(self, task: TaskSpec) -> None:
        with self._lock:
            self._tasks[task.task_id] = task
            self._save()

    def values(self) -> list[TaskSpec]:
        with self._lock:
            return list(self._tasks.values())

    def remove(self, task_id: str) -> None:
        with self._lock:
            if self._tasks.pop(task_id, None) is None:
                raise KeyError(task_id)
            self._save()


class DeployStatusStore:
    """节点部署状态持久化（deploy_status.json）。"""

    def __init__(self, path: str | os.PathLike[str] = "deploy_status.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._status: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as stream:
                self._status = json.load(stream)

    def _save(self) -> None:
        atomic_write_json(self.path, self._status)

    def update(self, name: str, **fields: Any) -> None:
        with self._lock:
            self._status[name] = {**self._status.get(name, {}), **fields}
            self._save()

    def get(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            return self._status.get(name)

    def all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._status)


class PortAllocator:
    """任务级 worker 端口分配：默认 18082 起顺延、空闲复用，多任务端口错开。"""

    def __init__(self, base: int = 18082, max_ports: int = 200):
        self.base = base
        self.max_ports = max_ports
        self._lock = threading.Lock()
        self._used: dict[int, str] = {}  # port -> task_id

    def allocate(self, task_id: str) -> int:
        with self._lock:
            for offset in range(self.max_ports):
                port = self.base + offset
                if port not in self._used:
                    self._used[port] = task_id
                    return port
            raise RuntimeError(
                f"no free worker port in [{self.base}, {self.base + self.max_ports})")

    def release(self, port: int) -> None:
        with self._lock:
            self._used.pop(port, None)

    def reserve(self, task_id: str, port: int) -> None:
        """重启恢复：把持久化的端口重新登记到本任务名下。"""
        with self._lock:
            self._used[port] = task_id

    def used_ports(self, task_id: str | None = None) -> list[int]:
        with self._lock:
            if task_id is None:
                return sorted(self._used)
            return sorted(port for port, owner in self._used.items() if owner == task_id)


class RunArtifacts:
    """任务运行产物目录 runs/{task_id}/：结果、任务快照、worker 日志、事件日志。"""

    def __init__(self, base_dir: str | os.PathLike[str] = "runs"):
        self.base = os.fspath(base_dir)

    def directory(self, task_id: str) -> str:
        return os.path.join(self.base, task_id)

    def _ensure(self, task_id: str) -> str:
        directory = self.directory(task_id)
        os.makedirs(directory, exist_ok=True)
        return directory

    def save_task(self, task: TaskSpec) -> None:
        directory = self._ensure(task.task_id)
        atomic_write_json(os.path.join(directory, "task.json"), asdict(task))

    def save_result(self, task: TaskSpec, result: dict[str, Any]) -> None:
        directory = self._ensure(task.task_id)
        atomic_write_json(os.path.join(directory, "result.json"), result)
        atomic_write_json(os.path.join(directory, "task.json"), asdict(task))

    def append_event(self, task_id: str, event: str) -> None:
        directory = self._ensure(task_id)
        with open(os.path.join(directory, "manager.log"), "a", encoding="utf-8") as stream:
            stream.write(f"{datetime.now().isoformat(timespec='seconds')}  {event}\n")

    def fetch_logs(self, executor: Executor, task: TaskSpec) -> dict[str, str]:
        """SSH tail 各 worker 的 kv-bench 日志到 logs/{worker}.log（失败写 .error）。

        任务专属 worker 的日志文件为 /var/log/kv-bench-worker-{task_id}-{port}.log。
        """
        directory = self._ensure(task.task_id)
        logs_dir = os.path.join(directory, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_glob = f"/var/log/kv-bench-worker-{task.task_id}-*.log"
        collected: dict[str, str] = {}
        for name, node in task.workers.items():
            try:
                content = executor.run(node, ["sh", "-c", f"cat {log_glob} 2>/dev/null | tail -n 500"])
                suffix = "log"
            except Exception as error:
                content = f"[log fetch failed: {error}]"
                suffix = "error"
            path = os.path.join(logs_dir, f"{name}.{suffix}")
            with open(path, "w", encoding="utf-8", errors="replace") as stream:
                stream.write(content)
            collected[name] = content
        return collected

    def read_logs(self, task_id: str) -> dict[str, Any]:
        """读取已保存的日志：{workers: {name: {content, error}}, manager_log}。"""
        directory = self.directory(task_id)
        workers: dict[str, Any] = {}
        logs_dir = os.path.join(directory, "logs")
        if os.path.isdir(logs_dir):
            for entry in sorted(os.listdir(logs_dir)):
                name, _, suffix = entry.rpartition(".")
                if suffix not in {"log", "error"}:
                    continue
                with open(os.path.join(logs_dir, entry), encoding="utf-8", errors="replace") as stream:
                    workers[name] = {"content": stream.read(), "error": suffix == "error"}
        manager_log = ""
        log_path = os.path.join(directory, "manager.log")
        if os.path.isfile(log_path):
            with open(log_path, encoding="utf-8", errors="replace") as stream:
                manager_log = stream.read()
        return {"workers": workers, "manager_log": manager_log}

    def remove(self, task_id: str) -> None:
        """删除任务的运行产物目录 runs/{task_id}/。"""
        shutil.rmtree(self.directory(task_id), ignore_errors=True)


@dataclass(frozen=True)
class BenchItem:
    src: str
    dst: str
    type: str = "forward"

    def __post_init__(self) -> None:
        if self.type not in {"forward", "reverse", "bidirectional"}:
            raise ValueError("type must be forward, reverse, or bidirectional")
        if self.src == self.dst:
            raise ValueError("src and dst must be different workers")


@dataclass
class TaskSpec:
    task_id: str
    workers: dict[str, Node]
    bench_items: list[BenchItem]
    options: dict[str, Any] = field(default_factory=dict)
    state: str = "queued"
    result: dict[str, Any] = field(default_factory=dict)
    # 任务专属 worker 端口（node -> [port, ...]）；支持单节点多 worker
    # 持久化到 tasks.json，重启后可继续停止/收集结果
    worker_ports: dict[str, list[int]] = field(default_factory=dict)


@dataclass(frozen=True)
class VersionCheck:
    versions: dict[str, tuple[str, ...]]
    consistent: bool
    reference: tuple[str, ...]


class Executor(Protocol):
    def run(self, node: Node, command: list[str]) -> str: ...

    def copy(self, node: Node, source: str, destination: str) -> None: ...


class WorkerClient(Protocol):
    def health(self, node: Node, port: int | None = None) -> dict[str, Any]: ...

    def start(self, node: Node, task_id: str, command: list[str], port: int | None = None) -> None: ...

    def stop(self, node: Node, task_id: str, port: int | None = None) -> None: ...

    def result(self, node: Node, task_id: str, port: int | None = None) -> dict[str, Any]: ...


class SshExecutor:
    """Remote executor using the system ssh/scp clients.

    无密码节点走 key 认证（BatchMode=yes 禁止交互提示，失败快速返回而非挂起）；
    所有命令 stdin=DEVNULL 并带超时，避免 ssh 等待输入/连接卡死 manager。
    """

    def _target(self, node: Node) -> str:
        return f"{node.user}@{node.ip}"

    def run(self, node: Node, command: list[str], timeout: float = 600.0) -> str:
        if node.password:
            cmd = ["sshpass", "-e", "ssh", "-p", str(node.ssh_port), self._target(node), "--", *command]
        else:
            cmd = ["ssh", "-p", str(node.ssh_port), "-o", "BatchMode=yes",
                   self._target(node), "--", *command]
        try:
            result = subprocess.run(
                cmd, check=True, text=True, capture_output=True,
                stdin=subprocess.DEVNULL, timeout=timeout,
                env={**os.environ, "SSHPASS": node.password},
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"ssh {node.name} ({node.ip}) timed out after {timeout:g}s") from error
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"ssh {node.name} ({node.ip}) failed: {error.stderr.strip() or error}"
            ) from error
        return result.stdout

    def copy(self, node: Node, source: str, destination: str, timeout: float = 600.0) -> None:
        if not os.path.exists(source):
            raise RuntimeError(f"local artifact not found: {source}")
        # scp 不会自动创建远端目录；先 mkdir -p（幂等），避免 exit 1
        self.run(node, ["mkdir", "-p", os.path.dirname(destination) or "."])
        if node.password:
            cmd = ["sshpass", "-e", "scp", "-P", str(node.ssh_port), source,
                   f"{self._target(node)}:{destination}"]
        else:
            cmd = ["scp", "-o", "BatchMode=yes", "-P", str(node.ssh_port), source,
                   f"{self._target(node)}:{destination}"]
        try:
            subprocess.run(
                cmd, check=True, text=True, capture_output=True,
                stdin=subprocess.DEVNULL, timeout=timeout,
                env={**os.environ, "SSHPASS": node.password},
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"scp to {node.name} ({node.ip}) timed out after {timeout:g}s") from error
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"scp to {node.name} ({node.ip}:{destination}) failed: {error.stderr.strip() or error}"
            ) from error


class HttpWorkerClient:
    """Worker API client over httpx (mature HTTP stack, explicit timeouts).

    ``trust_env=False``：worker 是内网控制面，必须直连。manager 机器上通常
    配置了 HTTP_PROXY/HTTPS_PROXY 环境代理（用于外网），若 httpx 默认跟随
    环境代理，内网 worker 调用会被塞进代理，代理连不上内网地址返回 504。
    """

    def _require_httpx(self) -> None:
        if httpx is None:
            raise RuntimeError(
                "httpx is required: pip install -r manager/requirements.txt")

    def _post(self, node: Node, path: str, payload: dict[str, Any]) -> None:
        self._require_httpx()
        try:
            with httpx.Client(timeout=10.0, trust_env=False) as client:
                response = client.post(
                    f"http://{node.ip}:{node.api_port}{path}", json=payload)
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error
        if response.status_code >= 300:
            raise RuntimeError(f"worker {node.name} returned HTTP {response.status_code}")

    def _url(self, node: Node, path: str, port: int | None) -> str:
        return f"http://{node.ip}:{port or node.api_port}{path}"

    def _post(self, node: Node, path: str, payload: dict[str, Any], port: int | None = None) -> None:
        self._require_httpx()
        try:
            with httpx.Client(timeout=10.0, trust_env=False) as client:
                response = client.post(self._url(node, path, port), json=payload)
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error
        if response.status_code >= 300:
            raise RuntimeError(f"worker {node.name} returned HTTP {response.status_code}")

    def start(self, node: Node, task_id: str, command: list[str], port: int | None = None) -> None:
        self._post(node, "/v1/tasks/start", {"task_id": task_id, "command": command}, port=port)

    def stop(self, node: Node, task_id: str, port: int | None = None) -> None:
        self._post(node, f"/v1/tasks/{task_id}/stop", {}, port=port)

    def health(self, node: Node, port: int | None = None) -> dict[str, Any]:
        self._require_httpx()
        try:
            with httpx.Client(timeout=5.0, trust_env=False) as client:
                response = client.get(self._url(node, "/v1/health", port))
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error

    def result(self, node: Node, task_id: str, port: int | None = None) -> dict[str, Any]:
        self._require_httpx()
        try:
            with httpx.Client(timeout=10.0, trust_env=False) as client:
                response = client.get(
                    self._url(node, f"/v1/tasks/{task_id}/result", port))
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error


class DeploymentManager:
    # 可注入的默认 server 就绪探测（测试/扩展用，None 时走 TCP 连接探测）；
    # 经 __init__ 复制为实例属性，避免类属性函数被绑定为方法
    default_server_probe = None

    def __init__(
        self,
        executor: Executor,
        worker_client: WorkerClient | None = None,
        node_store: NodeStore | None = None,
        task_store: TaskStore | None = None,
        deploy_status: DeployStatusStore | None = None,
        artifacts: RunArtifacts | None = None,
        ports: PortAllocator | None = None,
    ):
        self.executor = executor
        self.worker_client = worker_client
        self.node_store = node_store
        self.task_store = task_store
        self.deploy_status = deploy_status
        self.artifacts = artifacts
        self.ports = ports if ports is not None else PortAllocator()
        self.tasks: dict[str, TaskSpec] = {}
        self._lock = threading.Lock()
        self._starting: set[str] = set()  # 正在启动的任务（防止重复启动/并发修改）
        self.server_probe = DeploymentManager.default_server_probe
        self.port_attempts = 3          # 单节点启动 worker 尝试的端口数
        self.worker_start_attempts = 10  # 健康探测轮数
        self.worker_start_interval = 0.5
        self.server_ready_timeout = 30.0  # 被动端数据面端口监听等待（秒）
        self.server_ready_interval = 0.5
        if task_store is not None:  # 恢复已持久化的任务与其端口占用
            for task in task_store.values():
                self.tasks[task.task_id] = task
                for port_list in task.worker_ports.values():
                    for port in port_list:
                        self.ports.reserve(task.task_id, port)

    def _event(self, task_id: str, message: str) -> None:
        if self.artifacts is not None:
            self.artifacts.append_event(task_id, message)

    def urma_packages(self, node: Node) -> tuple[str, ...]:
        # Equivalent to: rpm -qa | grep urma. Filtering locally avoids shell
        # interpolation of node-controlled values.
        output = self.executor.run(node, ["rpm", "-qa"])
        return tuple(sorted(line.strip() for line in output.splitlines() if "urma" in line.lower()))

    def ensure_urma_consistency(
        self, nodes: list[Node], source_dir: str, umdk_root: str | None
    ) -> VersionCheck:
        versions = {node.name: self.urma_packages(node) for node in nodes}
        reference = next(iter(versions.values()), ())
        result = VersionCheck(versions, all(value == reference for value in versions.values()), reference)
        if not result.consistent:
            for node in nodes:
                if versions[node.name] != reference:
                    self.compile_node(node, source_dir, umdk_root)
        return result

    def compile_node(self, node: Node, source_dir: str, umdk_root: str | None) -> None:
        configure = ["cmake", "-S", source_dir, "-B", f"{node.workdir}/build"]
        if umdk_root:  # 未配置 UMDK_ROOT 时不加 -D，走系统/环境默认查找
            configure.append(f"-DUMDK_ROOT={umdk_root}")
        self.executor.run(node, configure)
        self.executor.run(node, ["cmake", "--build", f"{node.workdir}/build", "-j"])

    def deploy(self, nodes: list[Node], artifact: str, destination: str, source_dir: str, umdk_root: str | None) -> VersionCheck:
        """部署 = 仅版本一致性编译 + 拷贝二进制；worker 在任务启动时按任务拉起。"""
        if self.node_store is not None:
            for node in nodes:
                self.node_store.upsert(node)
        check = self.ensure_urma_consistency(nodes, source_dir, umdk_root)
        for node in nodes:
            self.executor.copy(node, artifact, destination)
            if self.deploy_status is not None:
                self.deploy_status.update(
                    node.name,
                    deployed_at=datetime.now().isoformat(timespec="seconds"),
                    artifact=artifact,
                    destination=destination,
                    versions=list(check.versions.get(node.name, ())),
                    consistent=check.consistent or check.versions.get(node.name) == check.reference,
                    worker_state="deployed",
                )
        return check

    def build_commands(self, task: TaskSpec) -> list[str]:
        return [command for commands in self.build_assignments(task).values() for command in commands]

    def build_assignments(self, task: TaskSpec) -> dict[str, list[str]]:
        assignments: dict[str, list[str]] = {name: [] for name in task.workers}
        common = {"op": "write", "threads": 1, "duration": 10, **task.options}
        # 多对一：同一节点已有被动命令时跳过（服务端支持多客户端接入）
        passive_done: set[str] = set()
        for item in task.bench_items:
            source = self._resolve_worker(task, item.src)
            destination = self._resolve_worker(task, item.dst)
            if source is None or destination is None:
                raise ValueError(f"unknown topology endpoint: {item.src}->{item.dst}")
            for index, (node, peer) in enumerate(((source, destination), (destination, source))):
                active = item.type == "bidirectional" or (item.type == "forward" and index == 0) or (item.type == "reverse" and index == 1)
                arguments = [node.binary]
                if active:
                    arguments += [f"--peer-ip={peer.ip}", f"--direction={item.type}"]
                else:
                    # 多对一去重：同一目标节点只起一个 server
                    if node.name in passive_done:
                        continue
                    passive_done.add(node.name)
                    arguments += [f"--direction={item.type}", "--no-interactive"]
                for key, value in common.items():
                    arguments.extend(self._format_option(key, value))
                assignments[node.name].append(" ".join(shlex.quote(str(value)) for value in arguments))
        return assignments

    @staticmethod
    def _format_option(key: str, value: Any) -> list[str]:
        """打流选项 -> CLI 参数。

        标量 -> --key=value；bool True -> --key（开关）；bool False ->
        默认开启的开关（mbind/drv-ext）转 --no-key，其余不传。
        """
        flag = f"--{key.replace('_', '-')}"
        if isinstance(value, bool):
            if value:
                return [flag]
            if key in {"mbind", "drv_ext"}:
                return [f"--no-{key.replace('_', '-')}"]
            return []
        return [f"{flag}={value}"]

    @staticmethod
    def _resolve_worker(task: TaskSpec, endpoint: str) -> Node | None:
        if endpoint in task.workers:
            return task.workers[endpoint]
        return next((node for node in task.workers.values() if node.ip == endpoint), None)

    def create_task(self, payload: dict[str, Any]) -> TaskSpec:
        workers = {entry["name"]: Node(**entry) for entry in payload["workers"]}
        items = [BenchItem(**entry) for entry in payload["bench_items"]]
        task = TaskSpec(payload.get("task_id") or f"task-{uuid.uuid4().hex[:8]}", workers, items, payload.get("options", {}))
        with self._lock:
            if task.task_id in self.tasks:
                raise ValueError("task already exists")
            self.tasks[task.task_id] = task
        self._persist_task(task, "created")
        return task

    def start_task(self, task_id: str) -> list[str]:
        """启动任务：queued 或 stopped（停止后可再次执行）-> running。

        顺序保证：先拉起所有节点任务 worker -> 先下发被动端（server）-> 轮询
        确认其数据面端口已监听 -> 再下发主动端（client）。

        SSH/探测等慢操作在全局锁外执行：锁内仅做状态校验与登记，避免一个任务
        启动卡住阻塞其它请求（创建/停止/查询等）。
        """
        with self._lock:
            task = self.tasks[task_id]
            if task.state not in {"queued", "stopped"}:
                raise ValueError(f"task is not startable (state={task.state})")
            if task_id in self._starting:
                raise ValueError("task is already starting")
            previous_state = task.state
            task.result = {}  # 重新执行时清掉旧结果
            self._starting.add(task_id)
        assignments = self.build_assignments(task)
        started: list[tuple[str, int]] = []
        try:
            # 1) 所有节点拉起任务专属 worker（支持单节点多 worker：每个命令一个）
            for node_name, cmds in assignments.items():
                node = task.workers[node_name]
                port_list = []
                for _ in cmds:
                    port = self._spawn_worker(task, node)
                    port_list.append(port)
                    started.append((node_name, port))
                task.worker_ports[node_name] = port_list
            # 2) 分类：含 --peer-ip 的是主动端(client)，否则是被动端(server)
            passive = [n for n, cmds in assignments.items() if any("--peer-ip" not in c for c in cmds)]
            active = [n for n, cmds in assignments.items() if any("--peer-ip" in c for c in cmds)]
            # 3) 先下发被动端，等其 server 监听后再下发主动端
            for node_name in passive:
                self._dispatch_commands(task, node_name, assignments[node_name])
            server_port = self._task_server_port(task)
            for node_name in passive:
                node = task.workers[node_name]
                if not self._wait_server_ready(node, server_port):
                    raise RuntimeError(
                        f"server {node.name} did not listen on :{server_port}")
            for node_name in active:
                self._dispatch_commands(task, node_name, assignments[node_name])
        except Exception:
            # 任一节点失败：回滚已拉起的 worker，释放端口，恢复原状态
            self._teardown_workers(task, started)
            with self._lock:
                task.state = previous_state
            raise
        finally:
            with self._lock:
                self._starting.discard(task_id)
        with self._lock:
            task.state = "running"
        self._persist_task(task, "started")
        return [command for values in assignments.values() for command in values]

    def _dispatch_commands(self, task: TaskSpec, node_name: str, commands: list[str]) -> None:
        if self.worker_client is None:
            return
        node = task.workers[node_name]
        ports = task.worker_ports.get(node_name, [])
        for idx, command in enumerate(commands):
            port = ports[idx] if idx < len(ports) else None
            self.worker_client.start(node, task.task_id, shlex.split(command),
                                     port=port)

    @staticmethod
    def _task_server_port(task: TaskSpec) -> int:
        try:
            return int(task.options.get("server_port") or 13857)
        except (TypeError, ValueError):
            return 13857

    def _wait_server_ready(self, node: Node, port: int) -> bool:
        deadline = time.time() + self.server_ready_timeout
        while time.time() < deadline:
            if self._server_listening(node, port):
                return True
            time.sleep(self.server_ready_interval)
        return False

    def _server_listening(self, node: Node, port: int) -> bool:
        if self.server_probe is not None:
            return bool(self.server_probe(node, port))
        try:
            with socket.create_connection((node.ip, port), timeout=1.0):
                return True
        except OSError:
            return False

    def update_task(self, task_id: str, payload: dict[str, Any]) -> TaskSpec:
        """修改任务（queued/stopped 时可用；修改后回到 queued 待执行）。"""
        with self._lock:
            task = self.tasks[task_id]
            if task.state == "running":
                raise ValueError("cannot edit a running task")
            if task_id in self._starting:
                raise ValueError("cannot edit a task that is starting")
            task.workers = {entry["name"]: Node(**entry) for entry in payload["workers"]}
            task.bench_items = [BenchItem(**entry) for entry in payload["bench_items"]]
            task.options = payload.get("options", {})
            task.result = {}
            task.state = "queued"
        self._persist_task(task, "updated")
        return task

    def delete_task(self, task_id: str) -> None:
        """删除任务：回收 worker/端口、移除持久化任务与其运行产物。"""
        with self._lock:
            if task_id in self._starting:
                raise ValueError("task is still starting")
            task = self.tasks.pop(task_id, None)
            if task is None:
                raise KeyError(task_id)
        self._teardown_workers(task, [(name, p) for name, plist in task.worker_ports.items() for p in plist])
        if self.task_store is not None:
            self.task_store.remove(task_id)
        if self.artifacts is not None:
            self.artifacts.remove(task_id)

    def stop_task(self, task_id: str) -> None:
        """停止任务：停 bench -> 杀 worker -> 释放端口（慢操作在锁外）。"""
        with self._lock:
            task = self.tasks[task_id]
            if task_id in self._starting:
                raise ValueError("task is still starting")
            ports = dict(task.worker_ports)
        if self.worker_client is not None:
            for node in task.workers.values():
                for port in ports.get(node.name, []):
                    try:
                        self.worker_client.stop(node, task.task_id, port=port)
                    except Exception:
                        pass  # worker 可能已不在
        self._teardown_workers(task, [(name, p) for name, plist in ports.items() for p in plist])
        with self._lock:
            task.state = "stopped"
        self._persist_task(task, "stopped")

    def _spawn_worker(self, task: TaskSpec, node: Node) -> int:
        """分配端口 -> ssh 拉起该任务的 worker -> 轮询 /v1/health；失败换下一个端口。"""
        for _ in range(self.port_attempts):
            port = self.ports.allocate(task.task_id)
            log_path = f"/var/log/kv-bench-worker-{task.task_id}-{port}.log"
            # 整行命令作为单个参数传给 ssh（远端 shell 原样执行），</dev/null 防挂住
            start_cmd = (
                f"nohup {shlex.quote(node.binary)} --worker --worker-port={port} "
                f">{log_path} 2>&1 </dev/null &"
            )
            try:
                self.executor.run(node, [start_cmd])
            except Exception:
                self.ports.release(port)
                raise
            if self._wait_worker_ready(node, port):
                return port
            # 进程可能已起（如端口被占后崩溃/不健康）：清理后换端口
            try:
                self.executor.run(node, ["pkill", "-f", f"worker-port={port}"])
            except Exception:
                pass
            self.ports.release(port)
        raise RuntimeError(f"worker {node.name} did not become healthy on any free port")

    def _wait_worker_ready(self, node: Node, port: int) -> bool:
        if self.worker_client is None:
            return True
        for attempt in range(self.worker_start_attempts):
            try:
                state = self.worker_client.health(node, port=port).get("state")
                if state in {"ready", "running"}:
                    return True
            except Exception:
                pass
            if attempt + 1 < self.worker_start_attempts:
                time.sleep(self.worker_start_interval)
        return False

    def _teardown_workers(self, task: TaskSpec, entries: list[tuple[str, int]]) -> None:
        """杀掉任务 worker 进程、清理日志、释放端口（尽力而为，不抛异常）。

        pkill 后发 SIGCHLD 给 init 强制回收僵尸进程。"""
        for node_name, port in entries:
            node = task.workers.get(node_name)
            if node is not None:
                try:
                    self.executor.run(node, ["sh", "-c",
                        f"pkill -f worker-port={port} 2>/dev/null; "
                        f"sleep 0.2; kill -s CHLD 1 2>/dev/null"])
                except Exception:
                    pass
                try:
                    self.executor.run(node, ["sh", "-c",
                        f"rm -f /var/log/kv-bench-worker-{task.task_id}*.log"])
                except Exception:
                    pass
            self.ports.release(port)
        with self._lock:
            for node_name, _ in entries:
                task.worker_ports.pop(node_name, None)

    def collect_result(self, task_id: str) -> dict[str, Any]:
        with self._lock:
            task = self.tasks[task_id]
            ports = dict(task.worker_ports)
        workers: dict[str, Any] = {}
        for name, node in task.workers.items():
            if self.worker_client is not None:
                port_list = ports.get(name) or [None]  # None = 默认端口（回退用）
                merged = None
                for port in port_list:
                    try:
                        r = self.worker_client.result(node, task_id, port=port)
                    except Exception as error:
                        r = {"state": "unreachable", "error": str(error)}
                    if merged is None:
                        merged = r
                    else:
                        for key in ("ops", "bytes", "errors"):
                            merged[key] = int(merged.get(key, 0)) + int(r.get(key, 0))
                workers[name] = merged or {"state": "unreachable"}
        with self._lock:
            # 任务停止后 worker 已被回收：返回上次收集的结果，避免显示全零
            any_reachable = any(w.get("state") != "unreachable" for w in workers.values())
            if task.result and not any_reachable:
                return task.result
            total = {"ops": 0, "bytes": 0, "errors": 0}
            for result in workers.values():
                for key in total:
                    total[key] += int(result.get(key, 0))
            duration = float(task.options.get("duration", 10) or 10)
            total["iops"] = total["ops"] / duration
            total["bandwidth_mb_s"] = total["bytes"] / duration / 1_000_000
            task.result = {"workers": workers, "aggregate": total}
        self._persist_task(task, "result collected")
        if self.artifacts is not None:
            self.artifacts.save_result(task, task.result)
        return task.result

    def collect_logs(self, task_id: str) -> dict[str, Any]:
        """SSH 拉取各 worker 日志并落盘，返回已保存的日志内容。"""
        with self._lock:
            task = self.tasks[task_id]
        if self.artifacts is None:
            return {"task_id": task_id, "directory": "", "workers": {}, "manager_log": ""}
        collected = self.artifacts.fetch_logs(self.executor, task)
        self._event(task_id, f"logs collected: {', '.join(collected)}")
        data = self.artifacts.read_logs(task_id)
        data.update(task_id=task_id, directory=self.artifacts.directory(task_id))
        return data

    def _persist_task(self, task: TaskSpec, event: str) -> None:
        if self.task_store is not None:
            self.task_store.upsert(task)
        if self.artifacts is not None:
            self._event(task.task_id, event)
            self.artifacts.save_task(task)


def serve(host: str, port: int) -> None:
    """Start the FastAPI manager (domain wiring + uvicorn)."""
    from .api import create_app
    import uvicorn

    manager = DeploymentManager(
        SshExecutor(), HttpWorkerClient(), NodeStore(),
        TaskStore(), DeployStatusStore(), RunArtifacts(),
    )
    uvicorn.run(create_app(manager), host=host, port=port)


def main() -> None:
    parser = argparse.ArgumentParser(description="kv-bench deployment and topology manager")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
