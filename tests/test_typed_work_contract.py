from dataclasses import FrozenInstanceError

import pytest

from orchestrator.models import (
    HandoffEnvelope,
    RiskLevel,
    WorkContract,
    WorkItem,
    WorkStatus,
    canonical_json,
    canonical_sha256,
    handoff_from_dict,
    to_dict,
    work_contract_from_dict,
    work_item_from_dict,
    workstream_from_dict,
)
from orchestrator.system_prompt_director import PLAN_PROMPT as DIRECTOR_PLAN_PROMPT
from orchestrator.system_prompt_manager import PLAN_PROMPT as MANAGER_PLAN_PROMPT
from orchestrator.tools_schema import (
    SUBMIT_WORK_ITEM_PLAN_SCHEMA,
    SUBMIT_WORKSTREAM_PLAN_SCHEMA,
)


def make_contract(**overrides) -> WorkContract:
    values = {
        "id": "contract-auth-v1",
        "version": 1,
        "input_artifacts": ("api-spec",),
        "expected_outputs": ("src/auth/service.py",),
        "read_scopes": ("src/auth",),
        "write_scopes": ("src/auth/service.py",),
        "acceptance_criteria": ("Login succeeds",),
        "evidence_requirements": ("Targeted auth tests pass",),
        "consumers": ("api-workstream",),
        "risk_level": RiskLevel.HIGH,
        "priority": 7,
    }
    values.update(overrides)
    return WorkContract(**values)


def test_work_contract_is_immutable_and_normalizes_typed_values():
    contract = make_contract(
        input_artifacts=["api-spec"],
        risk_level="high",
    )

    assert contract.input_artifacts == ("api-spec",)
    assert contract.risk_level is RiskLevel.HIGH
    with pytest.raises(FrozenInstanceError):
        contract.version = 2


def test_work_contract_canonical_hash_is_byte_stable():
    contract = make_contract(test_requirements=("pytest tests/test_auth.py",))

    assert canonical_json({"b": 2, "a": 1}) == canonical_json(
        {"a": 1, "b": 2}
    )
    assert contract.sha256 == canonical_sha256(contract)
    assert contract.sha256 == work_contract_from_dict(
        to_dict(contract)
    ).sha256
    assert len(contract.sha256) == 64


def test_workstream_promotes_legacy_metadata_contract_to_first_class_field():
    contract = make_contract(
        id="stream-contract",
        write_scopes=("src/auth/service.py",),
    )
    stream = workstream_from_dict(
        {
            "id": "api-workstream",
            "title": "API",
            "goal": "Build API",
            "acceptance_criteria": list(contract.acceptance_criteria),
            "write_scopes": list(contract.write_scopes),
            "metadata": {"work_contract": to_dict(contract)},
        }
    )

    assert stream.contract == contract
    assert workstream_from_dict(to_dict(stream)) == stream


def test_handoff_envelope_round_trips_with_immutable_evidence():
    handoff = HandoffEnvelope(
        handoff_id="handoff-1",
        task_id="task-1",
        contract_id="contract-auth-v1",
        contract_version=1,
        source_agent_id="manager-1",
        target_agent_id="worker-1",
        signal_type="delegate_work_item",
        artifacts=("api-spec",),
        evidence={"requirements": ["pytest"], "approved": False},
        workstream_id="api-workstream",
        work_item_id="auth",
    )

    restored = handoff_from_dict(to_dict(handoff))

    assert restored == handoff
    assert handoff.producer_agent_id == "manager-1"
    assert handoff.consumer_agent_id == "worker-1"
    assert restored.sha256 == handoff.sha256
    with pytest.raises(TypeError):
        handoff.evidence["approved"] = True


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"id": "  "}, "non-empty"),
        ({"version": 0}, "at least 1"),
        ({"input_artifacts": ("api-spec", "api-spec")}, "duplicates"),
        ({"expected_outputs": ()}, "expected_outputs"),
        ({"evidence_requirements": ()}, "evidence_requirements"),
        ({"consumers": ()}, "consumers"),
        ({"consumers": ("",)}, "non-empty"),
        ({"read_scopes": ("../secret",)}, "project-relative"),
        ({"write_scopes": ("C:\\outside.py",)}, "project-relative"),
        ({"risk_level": "unknown"}, "risk_level"),
    ],
)
def test_work_contract_rejects_invalid_identity_sequences_scopes_and_risk(
    overrides, message
):
    with pytest.raises(ValueError, match=message):
        make_contract(**overrides)


def test_work_contract_and_work_item_round_trip_losslessly():
    contract = make_contract()
    item = WorkItem(
        id="auth",
        workstream_id="api-workstream",
        title="Authentication",
        goal="Implement authentication",
        acceptance_criteria=contract.acceptance_criteria,
        dependencies=("api-spec",),
        write_scopes=contract.write_scopes,
        priority=contract.priority,
        contract=contract,
    )

    restored_contract = work_contract_from_dict(to_dict(contract))
    restored_item = work_item_from_dict(to_dict(item))

    assert restored_contract == contract
    assert restored_item == item
    assert restored_item.contract is not None
    assert restored_item.contract.risk_level is RiskLevel.HIGH


def test_work_item_rejects_contract_fields_that_disagree_with_assignment():
    with pytest.raises(ValueError, match="write_scopes"):
        WorkItem(
            id="auth",
            workstream_id="api-workstream",
            title="Authentication",
            goal="Implement authentication",
            acceptance_criteria=("Login succeeds",),
            write_scopes=("src/auth/router.py",),
            priority=7,
            contract=make_contract(),
        )


def test_work_item_without_persisted_contract_gets_compatible_default():
    legacy = {
        "id": "legacy-item",
        "workstream_id": "legacy-stream",
        "title": "Legacy",
        "goal": "Preserve old state",
        "acceptance_criteria": ["Old behavior remains"],
        "dependencies": ["foundation"],
        "write_scopes": ["src/legacy.py"],
        "priority": 3,
    }

    item = work_item_from_dict(legacy)

    assert item.contract == WorkContract(
        id="legacy-item-contract",
        version=1,
        input_artifacts=("foundation",),
        expected_outputs=("src/legacy.py",),
        write_scopes=("src/legacy.py",),
        acceptance_criteria=("Old behavior remains",),
        test_requirements=("Old behavior remains",),
        evidence_requirements=("Old behavior remains",),
        consumers=("legacy-stream",),
        priority=3,
    )


def test_legacy_positional_work_item_construction_keeps_metadata_position():
    item = WorkItem(
        "legacy-item",
        "legacy-stream",
        "Legacy",
        "Preserve positional callers",
        ("Still works",),
        (),
        ("src/legacy.py",),
        WorkStatus.PENDING,
        2,
        {"legacy": True},
    )

    assert item.metadata == {"legacy": True}
    assert item.contract is not None
    assert item.contract.priority == 2


@pytest.mark.parametrize(
    ("plan_schema", "list_name", "count_name"),
    [
        (SUBMIT_WORKSTREAM_PLAN_SCHEMA, "workstreams", "requested_manager_count"),
        (SUBMIT_WORK_ITEM_PLAN_SCHEMA, "work_items", "requested_worker_count"),
    ],
)
def test_planning_schemas_require_contract_and_bounded_fanout(
    plan_schema, list_name, count_name
):
    schema = plan_schema["input_schema"]
    item_schema = schema["properties"][list_name]["items"]
    contract_fields = {
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
    }

    assert schema["additionalProperties"] is False
    assert item_schema["additionalProperties"] is False
    assert contract_fields <= set(item_schema["properties"])
    assert contract_fields <= set(item_schema["required"])
    assert "selected_fanout_reason" in schema["required"]

    count = schema["properties"][count_name]
    assert count == {
        "type": "integer",
        "minimum": 1,
        "maximum": 32,
        "description": count["description"],
    }
    assert f"{list_name}.length" in count["description"]
    assert "runtime" in count["description"].lower()
    assert "cap" in count["description"].lower()


@pytest.mark.parametrize("prompt", [DIRECTOR_PLAN_PROMPT, MANAGER_PLAN_PROMPT])
def test_plan_prompts_select_dynamic_fanout_and_require_work_contract(prompt):
    lowered = prompt.lower()

    assert "upper bound" in lowered
    assert "1 through" in lowered
    assert "selected_fanout_reason" in prompt
    assert "substantial independent" in lowered
    assert "non-conflicting write ownership" in lowered
    assert "never invent filler" in lowered
    assert "input_artifacts" in prompt
    assert "expected_outputs" in prompt
    assert "evidence_requirements" in prompt
    assert "hard contract" not in lowered
    assert "return exactly" not in lowered


def test_director_prompt_discourages_serialising_the_plan():
    """A dependency edge costs parallelism, so the prompt must say so.

    A Director that chains every workstream produces a plan where only one
    stream can run at a time: eleven workers were spawned and at most five
    could ever be in flight, then two, then one. The earlier wording ("add a
    dependency only when one stream truly needs another") was too soft to stop
    it treating a shared API contract as a real artifact dependency.
    """
    # The prompt is hard-wrapped, so compare on collapsed whitespace.
    lowered = " ".join(DIRECTOR_PLAN_PROMPT.lower().split())

    assert "removes parallelism" in lowered
    assert "cannot be written without reading a file another stream produces" in lowered
    # The specific false dependencies that serialised real runs.
    assert "not dependencies" in lowered
    assert "empty `dependencies` list" in lowered
