"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import IngestionEvent


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers = [("idempotency-key", idempotency_key.encode("utf-8"))]
    if traceparent:
        headers.append(("traceparent", traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    latest_events = {}
    for event in events:
        key = event.idempotency_key
        if key not in latest_events:
            latest_events[key] = event
        else:
            existing = latest_events[key]
            if (event.occurred_at, event.event_id) > (existing.occurred_at, existing.event_id):
                latest_events[key] = event
    
    return sorted(latest_events.values(), key=lambda e: e.idempotency_key)


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    from lab28_platform.contracts import FEATURE_REFS
    return {
        "features": list(FEATURE_REFS),
        "entities": {"asker_id": [asker_id]},
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    has_mandatory_error = False
    has_optional_error = False
    
    for probe in probes:
        if not probe.get("ready", False):
            if probe.get("mandatory", False):
                has_mandatory_error = True
            else:
                has_optional_error = True
                
    if has_mandatory_error:
        return "not_ready"
    if has_optional_error:
        return "degraded"
    return "ready"
