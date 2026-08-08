from orchestrator import task_manager


def test_task_manager_tracks_reviewed_file_stats(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-1",
        name="Update API",
        mode="orchestrator",
        prompt="Update API",
        root=str(tmp_path),
        files=["server.py"],
        settings={"worker_model": "test-model"},
    )

    task_manager.record_event(
        "task-1",
        {
            "type": "execution_result",
            "file_path": "server.py",
            "accepted": True,
            "additions": 12,
            "deletions": 3,
            "worker_feedback": "Updated endpoint.",
            "execution_result": "Pass",
        },
    )
    task_manager.finish_task("task-1", status="COMPLETED")

    task = task_manager.get_task("task-1")
    assert task["status"] == "COMPLETED"
    assert task["changed_files"]["server.py"] == {
        "additions": 12,
        "deletions": 3,
    }
    assert (tmp_path / "tasks.json").exists()
    task_manager._records.clear()


def test_agent_progress_tracks_each_role_phase(monkeypatch, tmp_path):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    task_manager.create_task(
        "task-agents",
        name="Trace agents",
        mode="orchestrator",
        prompt="Trace agents",
        root=str(tmp_path),
        files=[],
        settings={},
    )

    task_manager.record_event(
        "task-agents",
        {
            "type": "agent_progress",
            "role": "worker",
            "stage": "patching",
            "message": "Building patch",
        },
    )

    task = task_manager.get_task("task-agents")
    assert task["status"] == "CODING"
    assert task["phase"] == "patching"
    assert task["current_agent"] == "worker"
    task_manager._records.clear()


def test_auto_resumable_list_only_returns_interrupted_enabled_tasks(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(task_manager, "TASKS_FILE", tmp_path / "tasks.json")
    task_manager._records.clear()
    for task_id, enabled in (("auto", True), ("manual", False)):
        task_manager.create_task(
            task_id,
            name=task_id,
            mode="orchestrator",
            prompt="Continue",
            root=str(tmp_path),
            files=[],
            settings={"auto_continue": enabled},
        )
        task_manager.set_status(task_id, "INTERRUPTED", "interrupted")

    tasks = task_manager.list_auto_resumable_tasks()

    assert [task["id"] for task in tasks] == ["auto"]
    task_manager._records.clear()
