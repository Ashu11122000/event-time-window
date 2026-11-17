
from datetime import datetime
from typing import Any


# Simulated e-commerce order events
EVENTS = [
    {
        "event_id": "E1",
        "customer_id": "CUST101",
        "event_type": "ORDER_CREATED",
        "amount": 500,
        "event_time": "2026-11-17T10:01:00Z",
        "arrival_order": 1,
    },
    {
        "event_id": "E2",
        "customer_id": "CUST102",
        "event_type": "ORDER_CREATED",
        "amount": 700,
        "event_time": "2026-11-17T10:02:00Z",
        "arrival_order": 2,
    },
    {
        "event_id": "E3",
        "customer_id": "CUST103",
        "event_type": "ORDER_CREATED",
        "amount": 300,
        "event_time": "2026-11-17T10:04:00Z",
        "arrival_order": 3,
    },
    {
        "event_id": "E4",
        "customer_id": "CUST104",
        "event_type": "ORDER_CREATED",
        "amount": 200,
        "event_time": "2026-11-17T10:03:00Z",
        "arrival_order": 4,
    },
    {
        "event_id": "E5",
        "customer_id": "CUST105",
        "event_type": "ORDER_CREATED",
        "amount": 900,
        "event_time": "2026-11-17T10:08:00Z",
        "arrival_order": 5,
    },
]


def get_sample_events() -> list[dict[str, Any]]:
    """Return simulated events in their arrival order."""
    return [event.copy() for event in EVENTS]


def parse_event_time(event_time: str) -> datetime:
    """Convert ISO 8601 event time into a Python datetime."""
    return datetime.fromisoformat(event_time.replace("Z", "+00:00"))
