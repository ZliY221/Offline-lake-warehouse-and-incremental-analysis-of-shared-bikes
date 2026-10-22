"""Read-only schema change rehearsal across an offline data portfolio."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetSchemaChange:
    dataset: str
    current_fields: tuple[str, ...]
    proposed_fields: tuple[str, ...]
    dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class SchemaConsumerContract:
    consumer: str
    dataset: str
    required_fields: tuple[str, ...]


def rehearse_schema_changes(changes: list[DatasetSchemaChange],
                            contracts: list[SchemaConsumerContract]) -> dict[str, Any]:
    """Assess proposed schemas without changing catalog or stored data."""
    if not changes:
        raise ValueError("schema changes are required")
    names = [item.dataset.strip() for item in changes]
    if (any(not name for name in names)
            or any(item.dataset != item.dataset.strip() for item in changes)
            or len(names) != len(set(names))):
        raise ValueError("dataset names must be non-empty and unique")
    known = set(names)
    by_name = {item.dataset: item for item in changes}
    for item in changes:
        current = list(item.current_fields)
        proposed = list(item.proposed_fields)
        if (not current or not proposed or len(current) != len(set(current))
                or len(proposed) != len(set(proposed))
                or any(not field.strip() or field != field.strip()
                       for field in current + proposed)):
            raise ValueError("schema fields must be non-empty and unique")
        if (item.dataset in item.dependencies
                or not set(item.dependencies) <= known
                or len(item.dependencies) != len(set(item.dependencies))):
            raise ValueError("dataset dependency is invalid")

    contract_keys: set[tuple[str, str]] = set()
    for contract in contracts:
        key = (contract.consumer.strip(), contract.dataset.strip())
        if (not key[0] or contract.consumer != key[0]
                or contract.dataset != key[1] or key[1] not in known
                or key in contract_keys or not contract.required_fields
                or len(contract.required_fields) != len(set(contract.required_fields))
                or any(not field.strip() or field != field.strip()
                       for field in contract.required_fields)):
            raise ValueError("consumer contract is invalid")
        contract_keys.add(key)

    results: list[dict[str, Any]] = []
    blocking_contracts = 0
    review_count = 0
    for name in names:
        change = by_name[name]
        current = set(change.current_fields)
        proposed = set(change.proposed_fields)
        removed = sorted(current - proposed)
        added = sorted(proposed - current)
        impacted = []
        for contract in sorted(
                (row for row in contracts if row.dataset == name),
                key=lambda row: row.consumer):
            missing = sorted(set(contract.required_fields) - proposed)
            if missing:
                impacted.append({"consumer": contract.consumer,
                                 "missing_fields": missing})
        dependents = sorted(item.dataset for item in changes if name in item.dependencies)
        if impacted:
            status = "BLOCKED"
            blocking_contracts += len(impacted)
        elif removed and dependents:
            status = "REVIEW"
            review_count += 1
        else:
            status = "READY"
        results.append({"dataset": name, "status": status,
                        "added_fields": added, "removed_fields": removed,
                        "dependent_datasets": dependents,
                        "impacted_consumers": impacted})

    portfolio_status = ("BLOCKED" if blocking_contracts else
                        "REVIEW" if review_count else "READY")
    return {"status": portfolio_status, "dataset_count": len(changes),
            "blocking_contract_count": blocking_contracts,
            "review_dataset_count": review_count, "datasets": results,
            "schema_changes_applied": False}
