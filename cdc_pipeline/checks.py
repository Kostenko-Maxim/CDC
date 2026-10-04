import json
import os
import time
import uuid

from confluent_kafka import Consumer, TopicPartition

from .events import OrderState, read_jsonl, unwrap
from .runtime import api, contents, db, objects, s3, save_report, wait_running


def status():
    connectors = {name: api(f"/connectors/{name}/status") for name in api("/connectors")}
    with db() as connection:
        slots = connection.execute(
            "SELECT slot_name, active, wal_status, "
            "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint "
            "FROM pg_replication_slots").fetchall()
        publications = connection.execute(
            "SELECT pubname, schemaname, tablename FROM pg_publication_tables").fetchall()
        order_count = connection.execute("SELECT count(*) FROM shop.orders").fetchone()[0]
    files = objects(s3())
    save_report("status.json", {"connectors": connectors, "replication_slots": slots,
                               "publications": publications, "orders": order_count,
                               "s3_files": len(files),
                               "s3_bytes": sum(item["Size"] for item in files)})


def inspect(limit=3):
    client = s3()
    files = objects(client)
    print(f"Objects: {len(files)}; bytes: {sum(item['Size'] for item in files)}")
    for item in files[-limit:]:
        rows = list(read_jsonl(contents(client, item["Key"]).decode()))
        print(json.dumps({"file": item["Key"], "bytes": item["Size"],
                          "records": len(rows), "first_record": rows[0] if rows else None},
                         indent=2, ensure_ascii=False))


def replay(compare=False):
    client = s3()
    state = OrderState()
    for item in objects(client):
        partition = int(item["Key"].split("/")[-2])
        for record in read_jsonl(contents(client, item["Key"]).decode()):
            state.apply(record, partition)
    report = state.summary()
    if compare:
        with db() as connection:
            current = {row[0]: row[1] for row in connection.execute(
                "SELECT order_id, row_to_json(o)::jsonb FROM shop.orders o")}
        # PostgreSQL and Debezium format timestamptz differently; compare domain fields.
        fields = ["order_id", "customer_id", "product_id", "quantity", "amount", "status"]
        mismatched = [key for key in current.keys() & state.rows.keys()
                      if any(current[key][field] != state.rows[key][field] for field in fields)]
        missing = sorted(current.keys() - state.rows.keys())
        extra = sorted(state.rows.keys() - current.keys())
        report["comparison"] = {"matched": not (missing or extra or mismatched),
                                "missing": missing[:20], "extra": extra[:20],
                                "mismatched": mismatched[:20]}
        save_report("replay.json", report)
        if not report["comparison"]["matched"]:
            raise RuntimeError("Replay differs from PostgreSQL. Stop generator and wait for sink flush.")
    else:
        save_report("replay.json", report)
    from .runtime import ARTIFACTS
    (ARTIFACTS / "orders-state.json").write_text(
        json.dumps(list(state.rows.values()), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")


def verify(timeout=180):
    wait_running("shop-source")
    wait_running("orders-s3-sink")
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
                         "group.id": "verify-" + str(uuid.uuid4()),
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    topic_deadline = time.monotonic() + timeout
    while time.monotonic() < topic_deadline:
        metadata = consumer.list_topics(timeout=10)
        topic = metadata.topics.get("cdc.shop.orders")
        if topic is not None and not topic.error:
            break
        time.sleep(2)
    else:
        consumer.close()
        raise TimeoutError("Source has not created cdc.shop.orders")
    consumer.assign([TopicPartition("cdc.shop.orders", 0, 0)])
    marker = "verify-" + uuid.uuid4().hex
    with db() as connection:
        order_id = connection.execute(
            "INSERT INTO shop.orders (customer_id, product_id, quantity, amount, status) "
            "VALUES (1, 1, 42, 89.99, %s) RETURNING order_id", (marker,)).fetchone()[0]
        connection.execute("UPDATE shop.orders SET status = %s, updated_at = now() "
                           "WHERE order_id = %s", (marker + "-updated", order_id))
        connection.execute("DELETE FROM shop.orders WHERE order_id = %s", (order_id,))
    expected = ["c", "u", "d", "tombstone"]
    kafka_ops = []
    s3_ops = []
    snapshot_events = 0
    cache = set()
    client = s3()
    deadline = time.monotonic() + timeout
    last_scan = 0.0
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(0.2)
            if message is not None:
                if message.error():
                    raise RuntimeError(str(message.error()))
                key = unwrap(json.loads(message.key()))
                value = unwrap(json.loads(message.value())) if message.value() is not None else None
                if key.get("order_id") == order_id:
                    op = value["op"] if value else "tombstone"
                    if op in ("u", "d") and value["before"] is None:
                        raise AssertionError("Expected a full before image with REPLICA IDENTITY FULL")
                    kafka_ops.append(op)
            if time.monotonic() - last_scan >= 2:
                for item in objects(client):
                    if item["Key"] in cache:
                        continue
                    for record in read_jsonl(contents(client, item["Key"]).decode()):
                        value = unwrap(record.get("value"))
                        if value and value.get("op") == "r":
                            snapshot_events += 1
                        if unwrap(record["key"]).get("order_id") == order_id:
                            s3_ops.append(value["op"] if value else "tombstone")
                    cache.add(item["Key"])
                last_scan = time.monotonic()
            if kafka_ops == expected and s3_ops == expected and snapshot_events > 0:
                report = {"passed": True, "marker_order_id": order_id,
                          "kafka_operations": kafka_ops, "s3_operations": s3_ops,
                          "snapshot_events_in_s3": snapshot_events,
                          "s3_files_checked": len(cache)}
                save_report("verification.json", report)
                return
        raise TimeoutError(f"CDC delivery timed out: Kafka={kafka_ops}, S3={s3_ops}, "
                           f"snapshot_events={snapshot_events}")
    finally:
        consumer.close()
