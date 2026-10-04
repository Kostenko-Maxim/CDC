"""Pure CDC replay logic; no infrastructure dependencies."""

import json
from collections import Counter


def unwrap(value):
    """Support both schemaless JSON and JsonConverter schema/payload envelopes."""
    if isinstance(value, dict) and "schema" in value and "payload" in value:
        return value["payload"]
    return value


class OrderState:
    """Apply a partition's ordered changelog, including deletes and tombstones.

    Offsets are scoped to partitions. Replaying a previously applied offset is
    idempotent. Callers must supply records in ascending offset order within
    each partition; this class deliberately rejects unseen out-of-order input.
    """

    def __init__(self):
        self.rows = {}
        self.operations = Counter()
        self.seen = set()
        self.high_watermarks = {}
        self.duplicates = 0

    def apply(self, record, partition=0):
        offset = record["offset"]
        identity = (partition, offset)
        if identity in self.seen:
            self.duplicates += 1
            return
        if offset <= self.high_watermarks.get(partition, -1):
            raise ValueError(f"Out-of-order offset {offset} in partition {partition}")
        key = unwrap(record.get("key"))
        event = unwrap(record.get("value"))
        if not isinstance(key, dict) or "order_id" not in key:
            raise ValueError("Expected an order_id in the Kafka key")
        order_id = key["order_id"]
        if event is None:
            self.rows.pop(order_id, None)
            self.operations["tombstone"] += 1
        elif event.get("op") in ("r", "c", "u"):
            row = event.get("after")
            if row is None or row.get("order_id") != order_id:
                raise ValueError("CDC after image does not match its key")
            self.rows[order_id] = row
            self.operations[event["op"]] += 1
        elif event.get("op") == "d":
            self.rows.pop(order_id, None)
            self.operations["d"] += 1
        else:
            raise ValueError(f"Unsupported CDC event: {event}")
        self.seen.add(identity)
        self.high_watermarks[partition] = offset

    def summary(self):
        return {
            "live_orders": len(self.rows),
            "events": len(self.seen),
            "duplicates_skipped": self.duplicates,
            "operations": dict(self.operations),
            "high_watermarks": self.high_watermarks,
        }


def read_jsonl(content):
    for number, line in enumerate(content.splitlines(), 1):
        if line.strip():
            try:
                yield json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSON on line {number}") from error
