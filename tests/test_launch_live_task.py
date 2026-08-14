from __future__ import annotations

import io
import json

from scripts import launch_live_task


def test_prompt_file_is_bom_safe_and_payload_keeps_complete_prompt(tmp_path):
    destination = tmp_path / "destination"
    prompt_file = tmp_path / "prompt.md"
    prompt = "Build the complete project.\n\nRequirement: keep this final line.\n"
    prompt_file.write_bytes(b"\xef\xbb\xbf" + prompt.encode("utf-8"))
    args = launch_live_task.build_parser().parse_args(
        [
            str(destination),
            "--prompt-file",
            str(prompt_file),
            "--test-command",
            'python -m pytest -k "live flow"',
            "--max-managers",
            "6",
            "--max-parallel-workers-per-manager",
            "3",
        ]
    )

    loaded = launch_live_task.read_prompt(args.prompt_file)
    payload = launch_live_task.build_payload(args, loaded)

    assert loaded == prompt
    assert payload["task"] == prompt
    assert payload["approval_mode"] == "staging_auto"
    assert payload["project_mode"] == "new_project"
    assert payload["test_cmd"] == 'python -m pytest -k "live flow"'
    assert payload["max_managers"] == 6
    assert payload["max_parallel_workers_per_manager"] == 3


def test_prompt_defaults_to_bom_safe_stdin(tmp_path):
    args = launch_live_task.build_parser().parse_args([str(tmp_path / "destination")])

    prompt = launch_live_task.read_prompt(
        args.prompt_file,
        io.BytesIO(b"\xef\xbb\xbfFull stdin prompt\nsecond line\n"),
    )

    assert prompt == "Full stdin prompt\nsecond line\n"


def test_prompt_can_be_replayed_from_cursor_transcript(tmp_path):
    transcript = tmp_path / "chat.jsonl"
    transcript.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "role": "user",
                        "message": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "<user_query>ignore</user_query>",
                                }
                            ]
                        },
                    }
                ),
                json.dumps(
                    {
                        "role": "user",
                        "message": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        "<user_query>prefix\n# VAI TRÒ\n"
                                        "Build everything.\n</user_query>"
                                    ),
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                ),
            ]
        ),
        encoding="utf-8",
    )

    prompt = launch_live_task.read_prompt_from_transcript(
        transcript,
        "# VAI TRÒ",
    )

    assert prompt == "# VAI TRÒ\nBuild everything.\n"


def test_timeline_jsonl_and_diagnostics_summary_are_machine_readable():
    events = [
        {"sequence": 1, "type": "model_request_started"},
        {"sequence": 2, "type": "protocol_retry", "reason": "transport"},
        {"sequence": 3, "type": "event_schema_validation_repaired"},
        {"sequence": 4, "type": "model_request_completed"},
    ]
    stream = io.StringIO()

    launch_live_task.append_timeline_jsonl(stream, events)
    summary = launch_live_task.summarize_diagnostics(
        events,
        {"id": "task-live", "status": "COMPLETED", "phase": "completed"},
    )

    assert [json.loads(line) for line in stream.getvalue().splitlines()] == events
    assert summary["model_requests"] == {
        "started": 1,
        "completed": 1,
        "failed": 0,
        "aborted": 0,
        "protocol_retries": 1,
    }
    assert summary["event_schema"] == {"repaired": 1, "rejected": 0}


def test_wait_ignores_transient_unfinalized_interruption(monkeypatch, tmp_path):
    pages = [
        {
            "events": [{"sequence": 1, "type": "model_request_started"}],
            "has_more": False,
        },
        {"events": [], "has_more": False},
        {"events": [], "has_more": False},
        {
            "events": [{"sequence": 2, "type": "hierarchy_completed"}],
            "has_more": False,
        },
        {"events": [], "has_more": False},
    ]
    tasks = [
        {"id": "task-live", "status": "INTERRUPTED", "finished_at": None},
        {"id": "task-live", "status": "INTERRUPTED", "finished_at": None},
        {"id": "task-live", "status": "CODING", "finished_at": None},
        {
            "id": "task-live",
            "status": "COMPLETED",
            "finished_at": "2026-08-13T21:00:00+00:00",
        },
        {
            "id": "task-live",
            "status": "COMPLETED",
            "finished_at": "2026-08-13T21:00:00+00:00",
        },
    ]

    class FakeClient:
        def get(self, path):
            return pages.pop(0) if "/timeline?" in path else tasks.pop(0)

    monkeypatch.setattr(launch_live_task.time, "sleep", lambda _seconds: None)

    task, events = launch_live_task.wait_for_task(
        FakeClient(),
        "task-live",
        timeline_path=tmp_path / "timeline.jsonl",
        poll_interval=0.05,
        timeout=0,
    )

    assert task["status"] == "COMPLETED"
    assert [event["sequence"] for event in events] == [1, 2]
