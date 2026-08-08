import sys
import queue
import threading
import json
import shlex
import uuid
import tkinter as tk
from contextlib import asynccontextmanager
from tkinter import filedialog
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from orchestrator.hierarchy import run_hierarchy
from orchestrator.orchestrator import run_session
from orchestrator.llm_client import call_agent
from orchestrator.models import EventEnvelope, to_dict
from orchestrator.scheduler import SchedulerLimits
from orchestrator.state_repository import StateRepository
from orchestrator import (
    artifact_manager,
    config,
    event_broker,
    path_utils,
    redaction,
    task_manager,
)

active_queues = {}
stop_flags = {} 
chat_input_queues = {} # Hàng đợi lưu tin nhắn chat tiếp theo
hierarchy_repository = StateRepository()
FRONTEND_DIST = Path(__file__).resolve().parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    def _auto_resume() -> None:
        for task in task_manager.list_auto_resumable_tasks():
            try:
                resume_task(task["id"])
            except Exception:
                # Startup must remain available even if one stale task cannot resume.
                task_manager.finish_task(
                    task["id"],
                    status="FAILED",
                    reason="auto_resume_startup_failed",
                )

    # Do not block accepting HTTP while stale tasks resume.
    threading.Thread(target=_auto_resume, name="auto-resume", daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(127\.0\.0\.1|localhost)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if (FRONTEND_DIST / "assets").is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=FRONTEND_DIST / "assets"),
        name="workspace-assets",
    )

_FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="none">
  <rect width="32" height="32" rx="8" fill="#0c111b"/>
  <rect x="3" y="3" width="26" height="26" rx="6" fill="#38d7e8"/>
  <path d="M9 16h4l2-5 3 10 2-5h3" stroke="#081015" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""


@app.get("/favicon.ico")
@app.get("/favicon.svg")
def get_favicon():
    for candidate in (
        FRONTEND_DIST / "favicon.svg",
        FRONTEND_DIST / "favicon.ico",
        Path(__file__).resolve().parent / "frontend" / "public" / "favicon.svg",
    ):
        if candidate.is_file():
            media = "image/svg+xml" if candidate.suffix == ".svg" else "image/x-icon"
            return FileResponse(candidate, media_type=media)
    return Response(content=_FAVICON_SVG, media_type="image/svg+xml")

def get_current_queue():
    thread_id = threading.get_ident()
    return active_queues.get(thread_id)

def is_current_thread_stopped():
    thread_id = threading.get_ident()
    return stop_flags.get(thread_id, False)

class TaskRequest(BaseModel):
    name: str | None = None
    root: str
    task: str
    files: str = ""
    mode: str
    project_mode: str = "edit"
    auto_apply: bool = True
    create_zip: bool = True
    model: str = "claude-sonnet-5"
    effort: str = "max"
    supervisor_model: str = "claude-sonnet-5"
    supervisor_effort: str = "max"
    director_model: str | None = None
    director_effort: str | None = None
    manager_model: str | None = None
    manager_effort: str | None = None
    reviewer_model: str = "claude-sonnet-5"
    reviewer_effort: str = "high"
    account_mode: str = "sticky" # Bổ sung tham số Sticky/Router
    test_cmd: str | None = None
    max_turns: int = Field(default=config.MAX_TURNS, ge=1, le=500)
    auto_continue: bool = False
    hierarchy_enabled: bool = False
    max_parallel_managers: int = Field(default=4, ge=1, le=32)
    # Total child agents for each Manager: N-1 Coders and one dedicated Tester.
    max_workers_per_manager: int = Field(default=5, ge=2, le=32)
    max_parallel_workers: int = Field(default=8, ge=1, le=64)

class ChatReplyRequest(BaseModel):
    message: str


class AgentConfigRequest(BaseModel):
    model: str
    effort: str


ALLOWED_AGENT_MODELS = {"claude-sonnet-5", "claude-sonnet-4-6"}
ALLOWED_AGENT_EFFORTS = {"low", "medium", "high", "max", "xhigh"}
    
@app.get("/style.css")
def get_css(): return FileResponse("style.css")

@app.get("/main.js")
def get_main_js(): return FileResponse("main.js")

@app.get("/api.js")
def get_api_js(): return FileResponse("api.js")

@app.get("/", response_class=HTMLResponse)
async def get_ui():
    workspace = FRONTEND_DIST / "index.html"
    if workspace.is_file():
        return workspace.read_text(encoding="utf-8")
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/legacy", response_class=HTMLResponse)
async def get_legacy_ui():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task_ui(task_id: str):
    with open("task.html", "r", encoding="utf-8") as f:
        html = f.read()
        return html.replace("{{TASK_ID}}", task_id)

@app.get("/api/pick-files")
def pick_files():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        file_paths = filedialog.askopenfilenames(title="Chọn các file Code cần xử lý")
        root.destroy()
        if file_paths:
            import os
            paths = [Path(p) for p in file_paths]
            common_root = os.path.commonpath([p.parent for p in paths])
            rel_files = [str(p.relative_to(common_root)).replace("\\", "/") for p in paths]
            return {"root": str(common_root).replace("\\", "/"), "files": ",".join(rel_files)}
        return {"root": "", "files": ""}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/pick-folder")
def pick_folder():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        folder = filedialog.askdirectory(title="Chọn thư mục project")
        root.destroy()
        return {"root": str(Path(folder).resolve()) if folder else ""}
    except Exception as e:
        return {"error": str(e)}


# ENDPOINT ĐỂ NHẬN TIN NHẮN CHAT TIẾP THEO
@app.post("/api/chat/{task_id}")
def chat_reply(task_id: str, req: ChatReplyRequest):
    if task_id in chat_input_queues:
        chat_input_queues[task_id].put(req.message)
        return {"status": "sent"}
    return {"error": "Luồng chat không tồn tại hoặc đã đóng"}

@app.post("/api/run")
def run_task(req: TaskRequest):
    return _start_task(req)


def _start_task(req: TaskRequest, resume_task_id: str | None = None):
    is_resume = resume_task_id is not None
    raw_root = req.root.strip()
    if req.mode == "orchestrator" and not raw_root:
        raise HTTPException(
            status_code=400,
            detail="Orchestrator mode yêu cầu project root.",
        )
    root_path = Path(raw_root).resolve() if raw_root else Path.cwd().resolve()
    if (
        req.mode == "orchestrator"
        and req.project_mode == "new_project"
        and not is_resume
    ):
        if root_path.exists() and not root_path.is_dir():
            raise HTTPException(status_code=400, detail="Destination phải là thư mục.")
        if root_path.exists() and any(root_path.iterdir()):
            raise HTTPException(
                status_code=400,
                detail="New Project yêu cầu thư mục đích trống.",
            )
        if not root_path.parent.is_dir():
            raise HTTPException(
                status_code=400,
                detail="Thư mục cha của destination không tồn tại.",
            )
    elif not root_path.is_dir() and not (
        is_resume
        and req.mode == "orchestrator"
        and req.project_mode == "new_project"
    ):
        raise HTTPException(status_code=400, detail="Project root không tồn tại hoặc không phải thư mục.")

    files_list = []
    try:
        for item in (f.strip() for f in req.files.split(",") if f.strip()):
            parts = item.split(":", 1)
            path_utils.ensure_context_path_safe(parts[0])
            normalized, _ = path_utils.resolve_under_root(root_path, parts[0])
            files_list.append(
                f"{normalized}:{parts[1]}" if len(parts) > 1 else normalized
            )
    except (path_utils.PathEscapeError, path_utils.SensitivePathError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    task_id = resume_task_id or str(uuid.uuid4())
    q = event_broker.ReplayEventBroker()
    active_queues[task_id] = q
    parsed_test_cmd = shlex.split(req.test_cmd, posix=False) if req.test_cmd else None
    if is_resume:
        task_manager.resume_task(task_id)
    else:
        task_manager.create_task(
            task_id,
            name=req.name or req.task[:60],
            mode=req.mode,
            prompt=req.task,
            root=str(root_path) if raw_root else "",
            files=files_list,
            settings={
                "worker_model": req.model,
                "worker_effort": req.effort,
                "supervisor_model": req.supervisor_model,
                "supervisor_effort": req.supervisor_effort,
                "director_model": req.director_model or req.supervisor_model,
                "director_effort": req.director_effort or req.supervisor_effort,
                "manager_model": req.manager_model or req.supervisor_model,
                "manager_effort": req.manager_effort or req.supervisor_effort,
                "reviewer_model": req.reviewer_model,
                "reviewer_effort": req.reviewer_effort,
                "account_mode": req.account_mode,
                "test_cmd": req.test_cmd,
                "project_mode": req.project_mode,
                "auto_apply": req.auto_apply,
                "create_zip": req.create_zip,
                "max_turns": req.max_turns,
                "auto_continue": req.auto_continue,
                "hierarchy_enabled": req.hierarchy_enabled,
                "max_parallel_managers": req.max_parallel_managers,
                "max_workers_per_manager": req.max_workers_per_manager,
                "max_parallel_workers": req.max_parallel_workers,
            },
        )
    
    def background_worker(
        t_id,
        root,
        files,
        task_desc,
        mode,
        model,
        effort,
        supervisor_model,
        supervisor_effort,
        reviewer_model,
        reviewer_effort,
        project_mode,
        auto_apply,
        create_zip,
        account_mode,
        test_cmd,
        max_turns,
        auto_continue,
        resume_session,
        q,
        hierarchy_enabled,
        director_model,
        director_effort,
        manager_model,
        manager_effort,
        max_parallel_managers,
        max_workers_per_manager,
        max_parallel_workers,
    ):
        thread_id = threading.get_ident()
        active_queues[thread_id] = q
        stop_flags[thread_id] = False

        session_holder = {"id": f"legacy-{t_id}"}

        def emit(event):
            raw = redaction.redact_event(dict(event))
            session_id = str(raw.get("session_id") or session_holder["id"])
            session_holder["id"] = session_id
            event_type = str(raw.get("type") or "event")
            envelope = EventEnvelope(
                task_id=t_id,
                session_id=session_id,
                event_type=event_type,
                payload={
                    key: value
                    for key, value in raw.items()
                    if key not in {
                        "type",
                        "task_id",
                        "session_id",
                        "workstream_id",
                        "work_item_id",
                        "agent_instance_id",
                        "call_id",
                    }
                },
                workstream_id=raw.get("workstream_id"),
                work_item_id=raw.get("work_item_id"),
                agent_instance_id=raw.get("agent_instance_id"),
                call_id=raw.get("call_id"),
            )
            stored = hierarchy_repository.append_event(envelope)
            wire = {
                **raw,
                **to_dict(stored),
                "type": event_type,
                "sequence": stored.sequence,
                "timestamp": stored.timestamp.isoformat(),
            }
            if event_type not in {"token", "thinking"}:
                task_event = dict(wire)
                for large_field in ("prompt", "patch", "diff", "text", "test_output"):
                    task_event.pop(large_field, None)
                if isinstance(task_event.get("payload"), dict):
                    task_event["payload"] = {
                        key: value
                        for key, value in task_event["payload"].items()
                        if key not in {
                            "prompt",
                            "patch",
                            "diff",
                            "text",
                            "test_output",
                        }
                    }
                task_manager.record_event(t_id, task_event)
            q.put(wire)
        
        try:
            import orchestrator.llm_client as llm_client
            llm_client.thread_local.model = model
            llm_client.thread_local.effort = effort
            llm_client.thread_local.worker_model = model
            llm_client.thread_local.worker_effort = effort
            llm_client.thread_local.supervisor_model = supervisor_model
            llm_client.thread_local.supervisor_effort = supervisor_effort
            llm_client.thread_local.reviewer_model = reviewer_model
            llm_client.thread_local.reviewer_effort = reviewer_effort
            llm_client.thread_local.director_model = director_model
            llm_client.thread_local.director_effort = director_effort
            llm_client.thread_local.manager_model = manager_model
            llm_client.thread_local.manager_effort = manager_effort
            llm_client.thread_local.account_mode = account_mode
            llm_client.thread_local.event_sink = emit
        except Exception:
            pass
        
        try:
            emit({
                "type": "status",
                "data": (
                    (
                        f"🚀 Hierarchy | Director: {director_model}/{director_effort} "
                        f"| Manager: {manager_model}/{manager_effort} "
                        if hierarchy_enabled
                        else f"🚀 Task {t_id} | Supervisor: {supervisor_model}/{supervisor_effort} "
                    )
                    + f"| Worker: {model}/{effort} | Tester: {reviewer_model}/{reviewer_effort}"
                ),
            })
            
            if mode == "orchestrator":
                llm_client.thread_local.is_continuation = False
                execution_root = root
                effective_task = task_desc
                if project_mode == "new_project":
                    execution_root = (
                        artifact_manager.get_staging_workspace(t_id)
                        if resume_session
                        else artifact_manager.create_workspace(
                            t_id,
                            root,
                            auto_apply=auto_apply,
                            create_zip=create_zip,
                        )
                    )
                    effective_task = (
                        "NEW PROJECT MODE: Xây dựng project hoàn toàn mới trong staging. "
                        "Hãy tự thiết kế bộ khung và delegate trực tiếp từng file mới; "
                        "backend đã cho phép tạo file trong staging nên KHÔNG request_context "
                        "lặp lại cho file chưa tồn tại. Đặt is_final_ticket=false "
                        "cho đến file cuối cùng. Không sửa state/rules/DECISIONS.\n\n"
                        f"YÊU CẦU USER:\n{task_desc}"
                    )
                else:
                    effective_task = (
                        "EDIT MODE (FOLDER-ONLY): User chỉ chọn thư mục gốc. "
                        "PROJECT TREE liệt kê path trên máy. Khi cần nội dung file, "
                        "dùng request_context; hoặc delegate_task trực tiếp nếu path "
                        "đã tồn tại — local file agent sẽ nạp file từ đĩa. "
                        "Không yêu cầu User chọn từng file trước.\n\n"
                        f"YÊU CẦU USER:\n{task_desc}"
                    )
                approved_file_callback = None
                if project_mode == "new_project":
                    def approved_file_callback(file_path):
                        progress_manifest = (
                            artifact_manager.materialize_approved_file(
                                t_id, file_path
                            )
                        )
                        task_manager.set_artifact(
                            t_id, progress_manifest
                        )
                        emit(
                            {
                                "type": "artifact_progress",
                                "file_path": file_path,
                                "status": progress_manifest["status"],
                                "files": progress_manifest["files"],
                            }
                        )
                if hierarchy_enabled:
                    result = run_hierarchy(
                        root=execution_root,
                        task_description=effective_task,
                        task_id=t_id,
                        source_files=files,
                        test_cmd=test_cmd,
                        allow_new_files=project_mode == "new_project",
                        limits=SchedulerLimits(
                            max_parallel_managers=max_parallel_managers,
                            max_workers_per_manager=max_workers_per_manager,
                            max_parallel_workers=max_parallel_workers,
                        ),
                        director_model=director_model,
                        director_effort=director_effort,
                        manager_model=manager_model,
                        manager_effort=manager_effort,
                        worker_model=model,
                        worker_effort=effort,
                        reviewer_model=reviewer_model,
                        reviewer_effort=reviewer_effort,
                        on_event=emit,
                        on_file_approved=approved_file_callback,
                        repository=hierarchy_repository,
                        resume_session=resume_session,
                        agent_config_resolver=(
                            lambda agent_id, role, default_model, default_effort: (
                                (
                                    task_manager.get_agent_override(t_id, agent_id)
                                    or {}
                                ).get("model", default_model),
                                (
                                    task_manager.get_agent_override(t_id, agent_id)
                                    or {}
                                ).get("effort", default_effort),
                            )
                        ),
                    )
                else:
                    resume_cycle = resume_session
                    auto_cycles = 0
                    while True:
                        result = run_session(
                            root=execution_root,
                            task_description=effective_task,
                            source_files=files,
                            test_cmd=test_cmd,
                            max_turns=max_turns,
                            allow_new_files=project_mode == "new_project",
                            resume_session=resume_cycle,
                            on_event=emit,
                            on_file_approved=approved_file_callback,
                        )
                        if (
                            result.stopped_reason == "max_turns_reached"
                            and auto_continue
                            and not stop_flags.get(thread_id)
                            and auto_cycles < config.AUTO_CONTINUE_MAX_CYCLES
                        ):
                            auto_cycles += 1
                            resume_cycle = True
                            emit(
                                {
                                    "type": "auto_continue",
                                    "cycle": auto_cycles,
                                    "turn_count": result.final_state.get(
                                        "turn_count", 0
                                    ),
                                }
                            )
                            continue
                        break
                if stop_flags.get(thread_id):
                    emit({"type": "error", "data": "🛑 Đã hủy thao tác do có lệnh ép dừng!"})
                    task_manager.finish_task(t_id, status="STOPPED", reason="user_stopped")
                else:
                    artifact_manifest = None
                    if (
                        project_mode == "new_project"
                        and result.stopped_reason == "task_completed"
                    ):
                        artifact_manifest = artifact_manager.finalize_workspace(t_id)
                        task_manager.set_artifact(t_id, artifact_manifest)
                        emit({
                            "type": "artifact_ready",
                            "status": artifact_manifest["status"],
                            "files": artifact_manifest["files"],
                            "download_url": (
                                f"/api/tasks/{t_id}/artifacts/download"
                                if artifact_manifest.get("zip_path")
                                else None
                            ),
                        })
                    finish_payload = {
                        "type": "finish", 
                        "reason": result.stopped_reason,
                        "turns": [
                            {
                                "tool": t.tool_name,
                                "detail": t.detail,
                                "accepted": t.accepted,
                            }
                            for t in result.turns
                        ],
                        "final_state": {
                            "last_worker_feedback": result.final_state.get("last_worker_feedback", ""),
                            "last_execution_result": result.final_state.get("last_execution_result"),
                            "last_reviewer_feedback": result.final_state.get("last_reviewer_feedback", ""),
                            "last_review_verdict": result.final_state.get("last_review_verdict"),
                            "reviewer_next_instructions": result.final_state.get("reviewer_next_instructions", ""),
                            "turn_count": result.final_state.get("turn_count", 0),
                        },
                        "artifact": artifact_manifest,
                    }
                    emit(finish_payload)
                    task_manager.finish_task(
                        t_id,
                        status=(
                            "COMPLETED"
                            if result.stopped_reason == "task_completed"
                            else (
                                "MAX_TURNS"
                                if result.stopped_reason == "max_turns_reached"
                                else "FAILED"
                            )
                        ),
                        reason=result.stopped_reason,
                        final_state=result.final_state,
                    )
            else:
                # --- CHẾ ĐỘ CHAT ---
                # Chat dùng cặp model/effort chính và không cần project context.
                llm_client.thread_local.agent_role = "worker"
                llm_client.thread_local.worker_model = model
                llm_client.thread_local.worker_effort = effort
                chat_input_queues[t_id] = queue.Queue()
                chat_history = []
                
                context_blocks = []
                for f in files:
                    rel_path = f.split(":", 1)[0]
                    _, file_path = path_utils.resolve_under_root(root, rel_path)
                    if file_path.exists():
                        content = file_path.read_text(encoding="utf-8", errors="replace")
                        context_blocks.append(f"### FILE: {f}\n```\n{content}\n```")
                
                if context_blocks:
                    current_msg = f"Yêu cầu ban đầu:\n{task_desc}\n\nNội dung code:\n" + "\n".join(context_blocks)
                else:
                    current_msg = task_desc
                
                is_first_turn = True
                
                # Vòng lặp chat vô tận cho đến khi bị ép dừng
                while not stop_flags.get(thread_id):
                    # NẾU ROUTER: Bắt buộc nhét tay mớ History vào mỗi lần gọi.
                    if account_mode == 'router':
                        if is_first_turn:
                            full_prompt = current_msg
                        else:
                            full_prompt = "\n\n".join(chat_history) + f"\n\nUser: {current_msg}"
                        llm_client.thread_local.is_continuation = False 
                    # NẾU STICKY: Chỉ ném tin nhắn mới, Claude API tự nối chat_uuid!
                    else:
                        full_prompt = current_msg
                        llm_client.thread_local.is_continuation = not is_first_turn

                    result = call_agent("Bạn là một trợ lý AI thông minh.", full_prompt, tools=[], require_json=False)
                    
                    if stop_flags.get(thread_id):
                        q.put({"type": "error", "data": "🛑 Luồng chat bị ép dừng!"})
                        break
                    
                    answer = result.raw_response.get("content", "")
                    
                    # Lưu lại lịch sử
                    chat_history.append(f"User: {current_msg}")
                    chat_history.append(f"Assistant: {answer}")
                    is_first_turn = False
                    
                    # Báo cho UI là xong 1 turn, chuẩn bị nhận tiếp
                    emit({"type": "finish_chat_turn"})
                    emit({"type": "status", "data": "⏳ Đang chờ tin nhắn tiếp theo..."})
                    
                    # Treo luồng chờ người dùng gõ tin nhắn mới
                    next_msg = None
                    while not stop_flags.get(thread_id):
                        try:
                            next_msg = chat_input_queues[t_id].get(timeout=1)
                            break
                        except queue.Empty:
                            continue
                            
                    if stop_flags.get(thread_id):
                        break
                        
                    if next_msg:
                        current_msg = next_msg
                        
                # Dọn dẹp hàng đợi khi thoát
                chat_input_queues.pop(t_id, None)
                task_manager.finish_task(
                    t_id,
                    status="STOPPED",
                    reason="chat_closed",
                )

        except Exception as e:
            emit({"type": "error", "data": str(e)})
            task_manager.finish_task(t_id, status="FAILED", reason=str(e))
        finally:
            emit({"type": "done"})
            active_queues.pop(thread_id, None)
            stop_flags.pop(thread_id, None)

    threading.Thread(
        target=background_worker,
        args=(
            task_id,
            root_path,
            files_list,
            req.task,
            req.mode,
            req.model,
            req.effort,
            req.supervisor_model,
            req.supervisor_effort,
            req.reviewer_model,
            req.reviewer_effort,
            req.project_mode,
            req.auto_apply,
            req.create_zip,
            req.account_mode,
            parsed_test_cmd,
            req.max_turns,
            req.auto_continue,
            is_resume,
            q,
            req.hierarchy_enabled,
            req.director_model or req.supervisor_model,
            req.director_effort or req.supervisor_effort,
            req.manager_model or req.supervisor_model,
            req.manager_effort or req.supervisor_effort,
            req.max_parallel_managers,
            req.max_workers_per_manager,
            req.max_parallel_workers,
        ),
        daemon=True,
    ).start()
    return {"status": "started", "task_id": task_id}


@app.get("/api/tasks")
def list_tasks():
    return {"tasks": task_manager.list_tasks()}


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    task = task_manager.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return task


def _event_wire(event: EventEnvelope) -> dict:
    data = to_dict(event)
    payload = dict(data.pop("payload", {}))
    return {
        **payload,
        **data,
        "type": event.event_type,
        "sequence": event.sequence,
    }


@app.get("/api/tasks/{task_id}/plan")
def get_task_plan(task_id: str):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    plan = hierarchy_repository.get_plan(task_id)
    return {"plan": to_dict(plan) if plan is not None else None}


@app.get("/api/tasks/{task_id}/timeline")
def get_task_timeline(task_id: str, after: int = 0, limit: int = 1000):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    safe_limit = max(1, min(limit, 5000))
    events = hierarchy_repository.replay_events(
        task_id,
        after_sequence=max(0, after),
        limit=safe_limit,
    )
    return {"events": [_event_wire(event) for event in events]}


@app.get("/api/tasks/{task_id}/attempts")
def get_task_attempts(task_id: str, work_item_id: str | None = None):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {
        "attempts": [
            to_dict(item)
            for item in hierarchy_repository.list_attempts(
                task_id,
                work_item_id,
            )
        ]
    }


@app.put("/api/tasks/{task_id}/agents/{agent_id}/config")
def update_agent_config(
    task_id: str,
    agent_id: str,
    request: AgentConfigRequest,
):
    if task_manager.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if request.model not in ALLOWED_AGENT_MODELS:
        raise HTTPException(status_code=400, detail="Model không được hỗ trợ.")
    if request.effort not in ALLOWED_AGENT_EFFORTS:
        raise HTTPException(status_code=400, detail="Effort không được hỗ trợ.")
    value = task_manager.set_agent_override(
        task_id,
        agent_id,
        model=request.model,
        effort=request.effort,
    )
    return {
        "status": "saved",
        "agent_id": agent_id,
        "applies_to": "next_model_call",
        **value,
    }


@app.post("/api/tasks/{task_id}/resume")
def resume_task(task_id: str):
    task = task_manager.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    if task.get("mode") != "orchestrator":
        raise HTTPException(
            status_code=400,
            detail="Resume bền vững hiện chỉ hỗ trợ Orchestrator task.",
        )
    resumable_statuses = {"STOPPED", "FAILED", "MAX_TURNS", "INTERRUPTED"}
    if task.get("status") not in resumable_statuses:
        raise HTTPException(
            status_code=409,
            detail=f"Task trạng thái {task.get('status')} không thể tiếp tục.",
        )
    settings = task.get("settings") or {}
    request = TaskRequest(
        name=task.get("name"),
        root=task.get("root", ""),
        task=task.get("prompt", ""),
        files=",".join(task.get("files") or []),
        mode=task.get("mode", "orchestrator"),
        project_mode=settings.get(
            "project_mode", task.get("project_mode", "edit")
        ),
        auto_apply=settings.get("auto_apply", True),
        create_zip=settings.get("create_zip", True),
        model=settings.get("worker_model", "claude-sonnet-5"),
        effort=settings.get("worker_effort", "max"),
        supervisor_model=settings.get(
            "supervisor_model", "claude-sonnet-5"
        ),
        supervisor_effort=settings.get("supervisor_effort", "max"),
        reviewer_model=settings.get(
            "reviewer_model", "claude-sonnet-5"
        ),
        reviewer_effort=settings.get("reviewer_effort", "high"),
        director_model=settings.get("director_model"),
        director_effort=settings.get("director_effort"),
        manager_model=settings.get("manager_model"),
        manager_effort=settings.get("manager_effort"),
        account_mode=settings.get("account_mode", "sticky"),
        test_cmd=settings.get("test_cmd"),
        max_turns=settings.get("max_turns", config.MAX_TURNS),
        auto_continue=settings.get("auto_continue", False),
        hierarchy_enabled=settings.get("hierarchy_enabled", False),
        max_parallel_managers=settings.get("max_parallel_managers", 4),
        max_workers_per_manager=settings.get("max_workers_per_manager", 4),
        max_parallel_workers=settings.get("max_parallel_workers", 8),
    )
    return _start_task(request, resume_task_id=task_id)


@app.get("/api/tasks/{task_id}/artifacts")
def get_task_artifacts(task_id: str):
    manifest = artifact_manager.get_manifest(task_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail="Task chưa có artifact.")
    return manifest


@app.get("/api/tasks/{task_id}/artifacts/download")
def download_task_artifact(task_id: str):
    zip_path = artifact_manager.get_zip_path(task_id)
    if zip_path is None:
        raise HTTPException(status_code=404, detail="Task chưa có ZIP để tải.")
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{task_id}-project.zip",
    )


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    task = task_manager.get_task(task_id)
    active_statuses = {
        "QUEUED", "RUNNING", "PLANNING", "MANAGING", "CODING",
        "REVIEWING", "REVISION", "WAITING_INPUT", "STOPPING", "RESUMING",
    }
    if task is not None and task.get("status") in active_statuses:
        raise HTTPException(status_code=409, detail="Không thể xóa task đang chạy.")
    active_queues.pop(task_id, None)
    chat_input_queues.pop(task_id, None)
    stop_flags.pop(task_id, None)
    removed = task_manager.delete_task(task_id) if task is not None else False
    purged = hierarchy_repository.delete_task(task_id)
    if not removed and sum(purged.values()) == 0:
        raise HTTPException(status_code=404, detail="Task không tồn tại.")
    return {"status": "deleted", "purged": purged}

@app.post("/api/stop/{task_id}")
def stop_task(task_id: str):
    q = active_queues.get(task_id)
    if q:
        task_manager.set_status(task_id, "STOPPING", "stopping")
        for t_id, thread_q in list(active_queues.items()):
            if thread_q == q and str(t_id) != task_id:
                stop_flags[t_id] = True
        q.put({"type": "error", "data": "🛑 Hệ thống đang ép buộc dừng AI, vui lòng đợi..."})
    return {"status": "stopping"}

@app.get("/api/stream/{task_id}")
def stream_output(task_id: str, after: int = 0):
    def event_stream():
        broker = active_queues.get(task_id)
        task = task_manager.get_task(task_id)
        if not broker:
            if task is None:
                yield f"data: {json.dumps({'type': 'error', 'data': 'Task ID đã đóng hoặc không tồn tại', 'fatal': True})}\n\n"
                return
            persisted = hierarchy_repository.replay_events(
                task_id,
                after_sequence=max(0, after),
                limit=5000,
            )
            if persisted:
                for envelope in persisted:
                    yield f"data: {json.dumps(_event_wire(envelope))}\n\n"
                return
            yield f"data: {json.dumps({'type': 'error', 'data': 'Task ID đã đóng hoặc không tồn tại', 'fatal': True})}\n\n"
            return
        last_sequence = max(0, after)
        while True:
            messages = broker.wait_after(last_sequence, timeout=15)
            if not messages:
                yield ": ping\n\n"
                continue
            for msg in messages:
                last_sequence = int(msg.get("_seq", last_sequence))
                yield f"data: {json.dumps(msg)}\n\n"
                if msg["type"] == "done":
                    return
    return StreamingResponse(event_stream(), media_type="text/event-stream")
