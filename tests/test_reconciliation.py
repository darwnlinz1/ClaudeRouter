from dataclasses import replace

from orchestrator.models import PlanStatus, TaskPlan, WorkItem, WorkStatus, Workstream
from orchestrator.reconciliation import reconcile_completion


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
        status=(
            WorkStatus.APPROVED
            if item_status == WorkStatus.APPROVED
            else WorkStatus.FAILED
        ),
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
    assert report["errors"] == [
        "typed contract approval evidence is incomplete"
    ]
