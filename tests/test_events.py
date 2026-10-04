import unittest

from cdc_pipeline.events import OrderState, read_jsonl


def event(offset, operation, order_id=42, status="new", wrapped=False):
    key = {"order_id": order_id}
    value = None if operation is None else {
        "op": operation,
        "before": {"order_id": order_id, "status": "new"} if operation in ("u", "d") else None,
        "after": {"order_id": order_id, "status": status} if operation != "d" else None,
    }
    if wrapped:
        key = {"schema": {}, "payload": key}
        value = {"schema": {}, "payload": value} if value else None
    return {"offset": offset, "key": key, "value": value}


class ReplayTests(unittest.TestCase):
    def test_snapshot_update_delete_and_tombstone(self):
        state = OrderState()
        state.apply(event(0, "r"))
        state.apply(event(1, "u", status="paid"))
        self.assertEqual(state.rows[42]["status"], "paid")
        state.apply(event(2, "d"))
        state.apply(event(3, None))
        self.assertEqual(state.rows, {})
        self.assertEqual(state.summary()["operations"], {"r": 1, "u": 1, "d": 1, "tombstone": 1})

    def test_redelivery_does_not_resurrect_deleted_order(self):
        state = OrderState()
        created = event(10, "c")
        state.apply(created)
        state.apply(event(11, "d"))
        state.apply(created)
        self.assertEqual(state.rows, {})
        self.assertEqual(state.duplicates, 1)

    def test_same_offset_in_distinct_partitions_is_not_duplicate(self):
        state = OrderState()
        state.apply(event(0, "r", order_id=1), partition=0)
        state.apply(event(0, "r", order_id=2), partition=1)
        self.assertEqual(len(state.rows), 2)
        self.assertEqual(state.duplicates, 0)

    def test_schema_payload_wrapper(self):
        state = OrderState()
        state.apply(event(0, "c", wrapped=True))
        state.apply(event(1, "u", status="shipped", wrapped=True))
        self.assertEqual(state.rows[42]["status"], "shipped")

    def test_compacted_offsets_may_have_gaps(self):
        state = OrderState()
        state.apply(event(700, "u", status="paid"))
        state.apply(event(1000, "u", order_id=7))
        self.assertEqual(len(state.rows), 2)

    def test_unknown_lower_offset_is_rejected(self):
        state = OrderState()
        state.apply(event(9, "c"))
        with self.assertRaisesRegex(ValueError, "Out-of-order"):
            state.apply(event(7, "u"))

    def test_after_key_mismatch_is_rejected(self):
        record = event(0, "c")
        record["value"]["after"]["order_id"] = 43
        with self.assertRaisesRegex(ValueError, "does not match"):
            OrderState().apply(record)

    def test_tombstone_without_prior_create_is_safe(self):
        state = OrderState()
        state.apply(event(0, None))
        self.assertEqual(state.rows, {})

    def test_new_schema_fields_survive_replay(self):
        state = OrderState()
        state.apply(event(0, "r"))
        record = event(1, "u")
        record["value"]["after"]["channel"] = "mobile"
        state.apply(record)
        self.assertEqual(state.rows[42]["channel"], "mobile")

    def test_corrupted_jsonl_reports_line_number(self):
        with self.assertRaisesRegex(ValueError, "line 2"):
            list(read_jsonl('{"offset": 1}\nwrong\n'))


if __name__ == "__main__":
    unittest.main()
