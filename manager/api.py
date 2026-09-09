"""FastAPI application for the kv-bench manager.

Replaces the former stdlib ``http.server`` layer (removed from
``manager/app.py``).  All domain logic (deploy, task planning, result
aggregation) stays in ``manager/app.py``; this module only wires routes,
pydantic validation and static frontend hosting.  Route semantics are
identical to the previous implementation, except that worker passwords are
never included in HTTP responses.
"""

from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .app import DeploymentManager, Node

DIST_DIR = Path(__file__).resolve().parent / "web" / "dist"

HINT_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>kv-bench Manager</title>
<style>
  body{font:14px/1.7 system-ui,sans-serif;background:#f5f7fa;color:#1f2937;margin:0;display:flex;align-items:center;justify-content:center;min-height:100vh}
  .box{background:#fff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,.06);max-width:680px;padding:32px 36px}
  h1{font-size:20px;margin:0 0 8px} code{background:#f3f4f6;border-radius:6px;padding:2px 7px;font-size:13px}
  pre{background:#111827;color:#e5e7eb;border-radius:8px;padding:14px 16px;overflow:auto}
</style></head><body><div class="box">
<h1>kv-bench Manager</h1>
<p>前端尚未构建。请在 <code>manager/web</code> 下构建 SPA，然后刷新本页：</p>
<pre>cd manager/web
npm install
npm run build</pre>
<p>API 文档：<code>/docs</code>（Swagger UI）</p>
</div></body></html>"""


# ---------------------------------------------------------------------------
# pydantic request models
# ---------------------------------------------------------------------------


class NodeIn(BaseModel):
    name: str = Field(min_length=1, description="节点名称")
    ip: str = Field(min_length=1, description="节点 IP")
    user: str = "root"
    ssh_port: int = 22
    workdir: str = "/opt/kv-bench"
    binary: str = "/opt/kv-bench/build/kv-bench"
    api_port: int = 18082
    password: str = ""
    tags: list[str] = Field(default_factory=list, description="节点标签（可多个）")


class NodePatchIn(BaseModel):
    """局部更新节点（当前仅支持 tags，保留密码等其余字段）。"""

    tags: list[str] | None = Field(default=None, description="替换后的标签列表")


class BenchItemIn(BaseModel):
    src: str = Field(description="拓扑源（worker 名称或 IP）")
    dst: str = Field(description="拓扑目的（worker 名称或 IP）")
    type: Literal["forward", "reverse", "bidirectional"] = "forward"


class TaskIn(BaseModel):
    task_id: str | None = None
    workers: list[NodeIn]
    bench_items: list[BenchItemIn]
    options: dict[str, Any] = Field(default_factory=dict)


class TaskUpdateIn(BaseModel):
    """修改任务（task_id 由 URL 指定，不可改）。"""

    workers: list[NodeIn]
    bench_items: list[BenchItemIn]
    options: dict[str, Any] = Field(default_factory=dict)


class DeployIn(BaseModel):
    nodes: list[NodeIn]
    artifact: str
    destination: str
    source_dir: str
    umdk_root: str = Field(default="", description="UMDK 根目录；留空则编译时不加 -DUMDK_ROOT")


# ---------------------------------------------------------------------------
# serialization helpers (passwords never leave the process)
# ---------------------------------------------------------------------------


def node_dict(node: Node) -> dict[str, Any]:
    return {key: value for key, value in asdict(node).items() if key != "password"}


def task_dict(task: Any) -> dict[str, Any]:
    data = asdict(task)
    data["workers"] = [node_dict(node) for node in task.workers.values()]
    return data


# ---------------------------------------------------------------------------
# application factory
# ---------------------------------------------------------------------------


def create_app(manager: DeploymentManager, dist_dir: str | Path | None = None) -> FastAPI:
    dist = Path(dist_dir) if dist_dir else DIST_DIR
    app = FastAPI(
        title="kv-bench Manager",
        version="1.0.0",
        description="kv-bench 部署与拓扑任务管理 API（节点 / 部署 / 任务 / 聚合结果）",
    )

    @app.exception_handler(KeyError)
    async def key_error_handler(_request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(_request: Request, exc: RuntimeError) -> JSONResponse:
        # SSH/SCP/worker 调用等运行期失败：返回 {"error": ...} 供前端展示
        return JSONResponse(status_code=500, content={"error": str(exc)})

    # ---- nodes ------------------------------------------------------------

    @app.get("/v1/nodes", summary="节点列表")
    @app.get("/v1/workers", summary="节点列表（别名）")
    def list_nodes() -> list[dict[str, Any]]:
        nodes = manager.node_store.list() if manager.node_store else []
        result = []
        for node in nodes:
            item = node_dict(node)
            item["deploy"] = manager.deploy_status.get(node.name) if manager.deploy_status else None
            result.append(item)
        return result

    @app.post("/v1/nodes", status_code=201, summary="保存节点")
    def save_node(payload: NodeIn) -> dict[str, str]:
        if manager.node_store is None:
            raise ValueError("node store is disabled")
        node = Node(**payload.model_dump())
        manager.node_store.upsert(node)
        return {"name": node.name, "state": "saved"}

    @app.delete("/v1/nodes/{name}", summary="删除节点")
    def delete_node(name: str) -> dict[str, str]:
        if manager.node_store is None:
            raise KeyError(name)
        manager.node_store.remove(name)
        return {"state": "deleted"}

    @app.patch("/v1/nodes/{name}", summary="更新节点标签")
    def patch_node(name: str, payload: NodePatchIn) -> dict[str, Any]:
        if manager.node_store is None:
            raise KeyError(name)
        node = manager.node_store.get(name)
        if payload.tags is not None:
            # replace 重建 Node（frozen）并触发 __post_init__ 标签归一化，
            # 密码等其余字段保持不变。
            manager.node_store.upsert(replace(node, tags=payload.tags))
        return node_dict(manager.node_store.get(name))

    # ---- deploy -----------------------------------------------------------

    @app.post("/v1/deploy", summary="部署 artifact 并启动 worker")
    def deploy(payload: DeployIn) -> dict[str, Any]:
        nodes = [Node(**entry.model_dump()) for entry in payload.nodes]
        result = manager.deploy(
            nodes, payload.artifact, payload.destination,
            payload.source_dir, payload.umdk_root,
        )
        return asdict(result)

    # ---- tasks ------------------------------------------------------------

    @app.get("/v1/tasks", summary="任务列表")
    def list_tasks() -> list[dict[str, Any]]:
        return [task_dict(task) for task in manager.tasks.values()]

    @app.post("/v1/tasks", status_code=201, summary="创建任务")
    def create_task(payload: TaskIn) -> dict[str, str]:
        task = manager.create_task({
            "task_id": payload.task_id,
            "workers": [entry.model_dump() for entry in payload.workers],
            "bench_items": [entry.model_dump() for entry in payload.bench_items],
            "options": payload.options,
        })
        return {"task_id": task.task_id, "state": task.state}

    @app.put("/v1/tasks/{task_id}", summary="修改任务（queued/stopped 时可用）")
    def update_task(task_id: str, payload: TaskUpdateIn) -> dict[str, str]:
        task = manager.update_task(task_id, {
            "workers": [entry.model_dump() for entry in payload.workers],
            "bench_items": [entry.model_dump() for entry in payload.bench_items],
            "options": payload.options,
        })
        return {"task_id": task.task_id, "state": task.state}

    @app.delete("/v1/tasks/{task_id}", summary="删除任务")
    def delete_task(task_id: str) -> dict[str, str]:
        manager.delete_task(task_id)
        return {"task_id": task_id, "state": "deleted"}

    @app.post("/v1/tasks/{task_id}/start", summary="启动任务（拉起任务 worker）")
    def start_task(task_id: str) -> dict[str, Any]:
        commands = manager.start_task(task_id)
        return {
            "task_id": task_id,
            "state": "running",
            "commands": commands,
            "worker_ports": dict(manager.tasks[task_id].worker_ports),
        }

    @app.post("/v1/tasks/{task_id}/stop", summary="停止任务")
    def stop_task(task_id: str) -> dict[str, str]:
        manager.stop_task(task_id)
        return {"task_id": task_id, "state": "stopped"}

    @app.get("/v1/tasks/{task_id}/result", summary="任务聚合结果")
    def task_result(task_id: str) -> dict[str, Any]:
        return manager.collect_result(task_id)

    @app.get("/v1/tasks/{task_id}/logs", summary="任务日志（拉取并落盘到 runs/{task_id}/）")
    def task_logs(task_id: str) -> dict[str, Any]:
        return manager.collect_logs(task_id)

    # ---- frontend ---------------------------------------------------------

    if (dist / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

        @app.get("/", include_in_schema=False)
        def index() -> FileResponse:
            return FileResponse(dist / "index.html", media_type="text/html")
    else:
        @app.get("/", include_in_schema=False)
        def index() -> HTMLResponse:
            return HTMLResponse(HINT_HTML)

    return app
