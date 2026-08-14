from dataclasses import replace

from orchestrator.models import PlanStatus, TaskPlan, WorkItem, WorkStatus, Workstream
from orchestrator.reconciliation import reconcile_completion
from orchestrator.test_evidence import integration_test_passed


def _plan(*, item_status: WorkStatus, plan_status: PlanStatus) -> TaskPlan:
    item = WorkItem(
        id="core:api",
        workstream_id="core",
        title="API",
        goal="Implement API",
        acceptance_criteria=("API works",),
        write_scopes=("src/api.py",),
        status=item_status,
    )
    stream = Workstream(
        id="core",
        title="Core",
        goal="Build core",
        acceptance_criteria=("Core works",),
        work_items=(item,),
        requested_worker_count=1,
        status=(WorkStatus.APPROVED if item_status == WorkStatus.APPROVED else WorkStatus.FAILED),
    )
    return TaskPlan(
        task_id="task-1",
        session_id="session-1",
        goal="Deliver",
        workstreams=(stream,),
        requested_manager_count=1,
        status=plan_status,
    )


def test_completion_reconciliation_balances_every_entity_and_call():
    report = reconcile_completion(
        _plan(
            item_status=WorkStatus.APPROVED,
            plan_status=PlanStatus.COMPLETED,
        ),
        item_evidence={"core:api": {"accepted": True}},
        call_outcomes={"call-1": "completed", "call-2": "failed"},
    )

    assert report["balanced"] is True
    assert report["work_items"]["planned"] == 1
    assert report["work_items"]["completed"] == 1
    assert report["agents"]["planned"] == 4
    assert report["calls"]["terminal"] == 2


def test_completion_reconciliation_reports_in_flight_call():
    report = reconcile_completion(
        _plan(
            item_status=WorkStatus.FAILED,
            plan_status=PlanStatus.FAILED,
        ),
        item_evidence={"core:api": {"failure_kind": "preflight"}},
        call_outcomes={"call-1": "in_flight"},
    )

    assert report["balanced"] is False
    assert report["work_items"]["preflight_failed"] == 1
    assert report["calls"]["invalid_ids"] == ["call-1"]
    assert report["errors"] == ["calls terminal partition is incomplete"]


def test_completion_reconciliation_requires_call_or_explicit_agent_disposition():
    plan = _plan(
        item_status=WorkStatus.APPROVED,
        plan_status=PlanStatus.COMPLETED,
    )
    expected = {
        "director_1",
        "manager:core",
        "tester:core",
        "worker:core:api",
    }
    complete = reconcile_completion(
        plan,
        director_agent_id="director_1",
        called_agent_ids=expected,
    )
    assert complete["balanced"] is True
    assert complete["agent_calls"]["planned"] == 4

    missing_tester = reconcile_completion(
        plan,
        director_agent_id="director_1",
        called_agent_ids=expected - {"tester:core"},
    )
    assert missing_tester["balanced"] is False
    assert missing_tester["agent_calls"]["invalid_ids"] == ["tester:core"]
    assert "agent_calls terminal partition is incomplete" in missing_tester["errors"]

    explicitly_blocked = reconcile_completion(
        plan,
        director_agent_id="director_1",
        called_agent_ids=expected - {"tester:core"},
        agent_dispositions={"tester:core": "blocked"},
    )
    assert explicitly_blocked["balanced"] is True
    assert explicitly_blocked["agent_calls"]["blocked"] == 1


def test_started_agent_requires_model_start_or_allowlisted_capacity_reason():
    plan = _plan(
        item_status=WorkStatus.FAILED,
        plan_status=PlanStatus.FAILED,
    )
    called = {"director:root", "manager:core", "tester:core"}

    paperwork_failure = reconcile_completion(
        plan,
        started_agent_ids={"worker:core:api"},
        called_agent_ids=called,
        agent_dispositions={"worker:core:api": "failed"},
        agent_no_call_reasons={"worker:core:api": "scheduler"},
    )
    assert paperwork_failure["balanced"] is False
    assert paperwork_failure["started_agent_calls"]["invalid_ids"] == ["worker:core:api"]

    account_exhaustion = reconcile_completion(
        plan,
        started_agent_ids={"worker:core:api"},
        called_agent_ids=called,
        agent_dispositions={"worker:core:api": "failed"},
        agent_no_call_reasons={"worker:core:api": "account_unavailable"},
    )
    assert account_exhaustion["balanced"] is True
    assert account_exhaustion["started_agent_calls"]["failed"] == 1


def test_completion_reconciliation_rejects_nonterminal_work_statuses():
    report = reconcile_completion(
        _plan(
            item_status=WorkStatus.RUNNING,
            plan_status=PlanStatus.RUNNING,
        ),
        call_outcomes={},
    )

    assert report["balanced"] is False
    assert report["work_items"]["invalid_ids"] == ["core:api"]
    assert "work_items terminal partition is incomplete" in report["errors"]


def test_completion_reconciliation_rejects_unproven_typed_approval():
    plan = _plan(
        item_status=WorkStatus.APPROVED,
        plan_status=PlanStatus.COMPLETED,
    )
    stream = plan.workstreams[0]
    item = replace(
        stream.work_items[0],
        metadata={"contract_mode": "typed"},
    )
    plan = replace(plan, workstreams=(replace(stream, work_items=(item,)),))

    report = reconcile_completion(
        plan,
        item_evidence={
            item.id: {
                "accepted": True,
                "test_status": "passed",
            }
        },
    )

    assert report["balanced"] is False
    assert item.id in report["contract_violations"]
    assert report["errors"] == ["typed contract approval evidence is incomplete"]


def test_completion_reconciliation_requires_final_integration_pass_for_deferred_item():
    plan = _plan(item_status=WorkStatus.APPROVED, plan_status=PlanStatus.COMPLETED)
    stream = plan.workstreams[0]
    item = replace(stream.work_items[0], metadata={"contract_mode": "typed"})
    plan = replace(plan, workstreams=(replace(stream, work_items=(item,)),))
    base_evidence = {
        "accepted": True,
        "reviewer_verdict": "approved",
        "patch_sha256": "durable",
        "syntax_status": "passed",
        "test_scope": "integration",
    }

    deferred = reconcile_completion(
        plan,
        item_evidence={
            item.id: {
                **base_evidence,
                "test_status": "deferred",
            }
        },
    )
    passed = reconcile_completion(
        plan,
        item_evidence={
            item.id: {
                **base_evidence,
                "test_status": "passed",
            }
        },
    )

    assert deferred["balanced"] is False
    assert deferred["contract_violations"][item.id] == ["passing test evidence"]
    assert passed["balanced"] is True


def test_integration_gate_does_not_treat_skipped_test_states_as_passed():
    assert integration_test_passed("passed") is True
    assert integration_test_passed("not_configured") is False
    assert integration_test_passed("no_tests") is False
    assert integration_test_passed("deferred") is False


def test_partial_completion_separates_terminal_coverage_from_success():
    plan = _plan(
        item_status=WorkStatus.ABANDONED,
        plan_status=PlanStatus.PARTIAL,
    )
    stream = replace(plan.workstreams[0], status=WorkStatus.ABANDONED)
    plan = replace(plan, workstreams=(stream,))
    manager_id = stream.manager_agent_id or "manager:core"
    manager_report = {
        "manager_id": manager_id,
        "workstream_id": stream.id,
        "status": "abandoned",
    }

    covered = reconcile_completion(
        plan,
        expected_manager_ids=[manager_id],
        manager_terminal_reports=[manager_report],
        director_final_review_count=1,
    )
    missing = reconcile_completion(
        plan,
        expected_manager_ids=[manager_id],
        manager_terminal_reports=[],
        director_final_review_count=1,
    )
    duplicate = reconcile_completion(
        plan,
        expected_manager_ids=[manager_id],
        manager_terminal_reports=[manager_report, manager_report],
        director_final_review_count=2,
    )

    assert covered["covered"] is True
    assert covered["balanced"] is True
    assert covered["successful"] is False
    assert missing["covered"] is False
    assert duplicate["covered"] is False
    assert duplicate["director_final_review"]["exactly_once"] is False


def test_partial_plan_stays_balanced_when_typed_approval_evidence_is_incomplete():
    approved_item = WorkItem(
        id="core:api",
        workstream_id="core",
        title="API",
        goal="Implement API",
        acceptance_criteria=("API works",),
        write_scopes=("src/api.py",),
        status=WorkStatus.APPROVED,
        metadata={"contract_mode": "typed"},
    )
    abandoned_item = WorkItem(
        id="docs:readme",
        workstream_id="docs",
        title="Docs",
        goal="Write docs",
        acceptance_criteria=("Docs exist",),
        write_scopes=("README.md",),
        status=WorkStatus.ABANDONED,
        metadata={"contract_mode": "typed"},
    )
    approved_stream = Workstream(
        id="core",
        title="Core",
        goal="Build core",
        acceptance_criteria=("Core works",),
        work_items=(approved_item,),
        requested_worker_count=1,
        status=WorkStatus.APPROVED,
    )
    abandoned_stream = Workstream(
        id="docs",
        title="Docs",
        goal="Document",
        acceptance_criteria=("Docs exist",),
        work_items=(abandoned_item,),
        requested_worker_count=1,
        status=WorkStatus.ABANDONED,
    )
    plan = TaskPlan(
        task_id="task-partial-typed",
        session_id="session-1",
        goal="Deliver what we can",
        workstreams=(approved_stream, abandoned_stream),
        requested_manager_count=2,
        status=PlanStatus.PARTIAL,
    )
    manager_core = approved_stream.manager_agent_id or "manager:core"
    manager_docs = abandoned_stream.manager_agent_id or "manager:docs"

    report = reconcile_completion(
        plan,
        item_evidence={
            approved_item.id: {
                "accepted": True,
                "test_status": "deferred",
                "test_scope": "integration",
            },
            abandoned_item.id: {
                "accepted": False,
                "failure_kind": "agent_abandoned",
            },
        },
        expected_manager_ids=[manager_core, manager_docs],
        manager_terminal_reports=[
            {
                "manager_id": manager_core,
                "workstream_id": approved_stream.id,
                "status": "completed",
            },
            {
                "manager_id": manager_docs,
                "workstream_id": abandoned_stream.id,
                "status": "abandoned",
            },
        ],
        director_final_review_count=1,
    )

    assert report["covered"] is True
    assert report["balanced"] is True
    assert report["successful"] is False
    assert approved_item.id in report["contract_violations"]
    assert "typed contract approval evidence is incomplete" not in report["errors"]
