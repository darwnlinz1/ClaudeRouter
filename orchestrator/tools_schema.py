# -*- coding: utf-8 -*-

from . import config

REQUEST_CONTEXT_SCHEMA = {
    "name": "request_context",
    "description": "Yêu cầu backend nạp file (AST skeleton, line-range, hoặc full). Action này không thay đổi code.",
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "reason": {
                "type": "string",
                "enum": [
                    "need_skeleton",
                    "need_specific_lines",
                    "task_unclear",
                    "strategy_reset_needs_human",
                    "other",
                ],
                "description": "Lý do gửi action.",
            },
            "files_needed": {
                "type": "array",
                "items": {"type": "string", "minLength": 1, "maxLength": 500},
                "minItems": 1,
                "maxItems": 20,
                "uniqueItems": True,
                "description": "Cú pháp: 'file.py:skeleton', 'file.py:10-50', hoặc 'file.py'.",
            },
            "context_note": {
                "type": "string",
                "maxLength": 500,
                "description": "Ghi chú giải thích những gì cần làm rõ.",
            },
            "decisions_md_entry": {
                "type": ["string", "null"],
                "maxLength": 2000,
                "description": "Đề xuất ghi chú kiến trúc. Đặt null nếu không cần thiết.",
            },
        },
        "required": ["reason", "files_needed", "context_note", "decisions_md_entry"],
    },
}

DELEGATE_TASK_SCHEMA = {
    "name": "delegate_task",
    "description": "Yêu cầu backend giao một ticket trên một file cho Worker.",
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "file_path": {
                "type": "string",
                "minLength": 1,
                "maxLength": 500,
                "description": "File giao cho Worker xử lý.",
            },
            "instructions": {
                "type": "string",
                "minLength": 1,
                "maxLength": 12000,
                "description": "Lệnh cực kỳ chi tiết (Sửa hàm nào, dòng nào).",
            },
            "is_final_ticket": {
                "type": "boolean",
                "description": (
                    "Chỉ true khi đây là file/ticket cuối cùng của toàn bộ task. "
                    "Dùng false nếu còn file khác phải xử lý."
                ),
            },
            "context_note": {
                "type": "string",
                "maxLength": 500,
                "description": "Tóm tắt task đang giao.",
            },
            "decisions_md_entry": {
                "type": ["string", "null"],
                "maxLength": 2000,
                "description": "Đề xuất ghi vào DECISIONS.md (đặt null nếu không có).",
            },
        },
        "required": [
            "file_path",
            "instructions",
            "is_final_ticket",
            "context_note",
            "decisions_md_entry",
        ],
    },
}

SUBMIT_PATCH_SCHEMA = {
    "name": "submit_patch",
    "description": "Worker gửi báo cáo cho patch SEARCH/REPLACE đi kèm.",
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "task_status": {
                "type": "string",
                "enum": ["in_progress", "completed", "failed"],
                "description": "Trạng thái nộp code.",
            },
            "worker_feedback": {
                "type": "string",
                "description": (
                    "Báo cáo ngắn gọn những gì Worker vừa sửa, các giả định đã dùng "
                    "và mọi rủi ro hoặc việc cần Supervisor lưu ý."
                ),
            },
        },
        "required": ["task_status", "worker_feedback"],
    },
}

REVIEW_PATCH_SCHEMA = {
    "name": "review_patch",
    "description": (
        "Reviewer/Tester đánh giá patch sau khi backend đã chạy Syntax/Sandbox Test. "
        "Reviewer không được sửa code."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "verdict": {
                "type": "string",
                "enum": ["approved", "revise"],
                "description": "approved nếu patch đúng và đủ; revise nếu cần làm lại.",
            },
            "reviewer_feedback": {
                "type": "string",
                "description": "Nhận xét ngắn gọn, có bằng chứng và nêu rủi ro còn lại.",
            },
            "next_instructions": {
                "type": "string",
                "description": (
                    "Chỉ thị cụ thể cho Supervisor/Worker ở lượt sau; để trống khi approved."
                ),
            },
        },
        "required": ["verdict", "reviewer_feedback", "next_instructions"],
    },
}

# --- DIRECTOR / MANAGER HIERARCHY ---

_CONTRACT_PROPERTIES = {
    "contract_id": {
        "type": "string",
        "minLength": 1,
        "maxLength": 160,
        "description": "Stable identifier for this Work Contract.",
    },
    "contract_version": {
        "type": "integer",
        "minimum": 1,
        "maximum": 1000,
        "description": "Monotonic version of this Work Contract.",
    },
    "input_artifacts": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 500},
        "maxItems": 64,
        "uniqueItems": True,
        "description": "Artifact identifiers or project-relative paths required as inputs.",
    },
    "expected_outputs": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
        "minItems": 1,
        "maxItems": 32,
        "uniqueItems": True,
        "description": (
            "Concrete artifacts or observable deliverables this contract produces. "
            "Runtime binary outputs such as background.jpg may be listed here only "
            "when the Worker's source code creates them at runtime."
        ),
    },
    "read_scopes": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 500},
        "maxItems": 32,
        "uniqueItems": True,
        "description": "Project-relative paths the assignee may read.",
    },
    "write_scopes": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 500},
        "minItems": 1,
        "maxItems": 32,
        "uniqueItems": True,
        "description": (
            "Project-relative UTF-8 source/text/config/doc/script/.gitkeep files "
            "exclusively owned for Worker writes. Never include images, media, fonts, "
            "archives, databases, SVG, or binary runtime outputs."
        ),
    },
    "acceptance_criteria": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
        "minItems": 1,
        "maxItems": 20,
        "uniqueItems": True,
    },
    "test_requirements": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
        "minItems": 1,
        "maxItems": 20,
        "uniqueItems": True,
        "description": "Commands, checks, or scenarios that must pass before acceptance.",
    },
    "evidence_requirements": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
        "minItems": 1,
        "maxItems": 20,
        "uniqueItems": True,
        "description": "Specific test, inspection, or artifact evidence needed for acceptance.",
    },
    "consumers": {
        "type": "array",
        "items": {"type": "string", "minLength": 1, "maxLength": 160},
        "minItems": 1,
        "maxItems": 32,
        "uniqueItems": True,
        "description": "Stable identifiers for downstream work or final users of the outputs.",
    },
    "risk_level": {
        "type": "string",
        "enum": ["low", "medium", "high", "critical"],
    },
    "approval_policy": {
        "type": "string",
        "enum": ["never", "risk_based", "always"],
        "description": (
            "Human approval policy for side effects in this work contract. "
            "Defaults to risk_based when omitted."
        ),
    },
    "priority": {
        "type": "integer",
        "minimum": 0,
        "maximum": 100,
        "description": "Relative scheduling priority; larger values run first when otherwise ready.",
    },
}

_CONTRACT_REQUIRED = [
    "contract_id",
    "contract_version",
    "input_artifacts",
    "expected_outputs",
    "read_scopes",
    "write_scopes",
    "acceptance_criteria",
    "test_requirements",
    "evidence_requirements",
    "consumers",
    "risk_level",
    "priority",
]

_WORKSTREAM_ITEM_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "id": {"type": "string", "minLength": 1, "maxLength": 120},
        "title": {"type": "string", "minLength": 1, "maxLength": 300},
        "goal": {"type": "string", "minLength": 1, "maxLength": 4000},
        "dependencies": {
            "type": "array",
            "items": {"type": "string", "minLength": 1, "maxLength": 120},
            "maxItems": 32,
            "uniqueItems": True,
        },
        **_CONTRACT_PROPERTIES,
    },
    "required": [
        "id",
        "title",
        "goal",
        "dependencies",
        *_CONTRACT_REQUIRED,
    ],
}

SUBMIT_WORKSTREAM_PLAN_SCHEMA = {
    "name": "submit_workstream_plan",
    "description": (
        "Director submits a justified workstream DAG whose selected fan-out "
        "fits the runtime manager cap."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string", "minLength": 1, "maxLength": 3000},
            "requested_manager_count": {
                "type": "integer",
                "minimum": 1,
                "maximum": 32,
                "description": (
                    "Selected manager fan-out. It must equal workstreams.length and "
                    "must not exceed the runtime manager cap supplied in INPUT DATA."
                ),
            },
            "selected_fanout_reason": {
                "type": "string",
                "minLength": 1,
                "maxLength": 2000,
                "description": (
                    "Why this fan-out matches the number of substantial independent "
                    "work packages and their ownership boundaries."
                ),
            },
            "workstreams": {
                "type": "array",
                "items": _WORKSTREAM_ITEM_SCHEMA,
                "minItems": 1,
                "maxItems": 32,
            },
        },
        "required": [
            "summary",
            "requested_manager_count",
            "selected_fanout_reason",
            "workstreams",
        ],
    },
}

COMPLETE_PLAN_SCHEMA = {
    "name": "complete_plan",
    "description": (
        "Director nghiệm thu tích hợp; verdict revise cung cấp recovery summary "
        "cho vòng Director có giới hạn."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "verdict": {"type": "string", "enum": ["approved", "revise"]},
            "summary": {"type": "string", "minLength": 1, "maxLength": 4000},
            "remaining_risks": {
                "type": "array",
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
                "maxItems": 20,
            },
        },
        "required": ["verdict", "summary", "remaining_risks"],
    },
}

_WORK_ITEM_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "id": {"type": "string", "minLength": 1, "maxLength": 120},
        "title": {"type": "string", "minLength": 1, "maxLength": 300},
        "goal": {"type": "string", "minLength": 1, "maxLength": 4000},
        "file_path": {
            "type": "string",
            "minLength": 1,
            "maxLength": 500,
            "description": (
                "Required primary Worker target. It must be UTF-8 source, text, config, "
                "documentation, script, or .gitkeep; never background.jpg or another "
                "image/media/font/archive/database/SVG/binary artifact."
            ),
        },
        "instructions": {"type": "string", "minLength": 1, "maxLength": 12000},
        "dependencies": {
            "type": "array",
            "items": {"type": "string", "minLength": 1, "maxLength": 120},
            "maxItems": 64,
            "uniqueItems": True,
        },
        **_CONTRACT_PROPERTIES,
        "write_scopes": {
            **_CONTRACT_PROPERTIES["write_scopes"],
            "maxItems": config.MAX_FILES_PER_WORK_PACKAGE,
        },
        "test_focus": {"type": "string", "maxLength": 3000},
    },
    "required": [
        "id",
        "title",
        "goal",
        "file_path",
        "instructions",
        "dependencies",
        *_CONTRACT_REQUIRED,
        "test_focus",
    ],
}

SUBMIT_WORK_ITEM_PLAN_SCHEMA = {
    "name": "submit_work_item_plan",
    "description": (
        "Manager submits a justified sub-DAG of substantial requirement "
        "packages whose selected fan-out fits the runtime worker cap."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string", "minLength": 1, "maxLength": 3000},
            "requested_worker_count": {
                "type": "integer",
                "minimum": 1,
                "maximum": 32,
                "description": (
                    "Selected worker fan-out. It must equal work_items.length and "
                    "must not exceed the runtime worker cap supplied in INPUT DATA."
                ),
            },
            "selected_fanout_reason": {
                "type": "string",
                "minLength": 1,
                "maxLength": 2000,
                "description": (
                    "Why this fan-out matches the number of substantial independent "
                    "work packages and their ownership boundaries."
                ),
            },
            "work_items": {
                "type": "array",
                "items": _WORK_ITEM_SCHEMA,
                "minItems": 1,
                "maxItems": 32,
            },
        },
        "required": [
            "summary",
            "requested_worker_count",
            "selected_fanout_reason",
            "work_items",
        ],
    },
}

COMPLETE_WORKSTREAM_SCHEMA = {
    "name": "complete_workstream",
    "description": (
        "Manager nghiệm thu workstream; verdict revise phải đưa chỉ thị recovery "
        "chỉ cho item failed/blocked."
    ),
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "verdict": {"type": "string", "enum": ["approved", "revise"]},
            "summary": {"type": "string", "minLength": 1, "maxLength": 3000},
            "next_instructions": {"type": "string", "maxLength": 3000},
        },
        "required": ["verdict", "summary", "next_instructions"],
    },
}

# --- PHÂN QUYỀN TOOL CHO TỪNG AGENT ---
SUPERVISOR_TOOLS = [REQUEST_CONTEXT_SCHEMA, DELEGATE_TASK_SCHEMA]
WORKER_TOOLS = [SUBMIT_PATCH_SCHEMA]
REVIEWER_TOOLS = [REVIEW_PATCH_SCHEMA]
DIRECTOR_PLAN_TOOLS = [SUBMIT_WORKSTREAM_PLAN_SCHEMA]
DIRECTOR_REVIEW_TOOLS = [COMPLETE_PLAN_SCHEMA]
MANAGER_PLAN_TOOLS = [SUBMIT_WORK_ITEM_PLAN_SCHEMA]
MANAGER_REVIEW_TOOLS = [COMPLETE_WORKSTREAM_SCHEMA]
