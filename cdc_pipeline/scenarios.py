"""Explicit, isolated operational experiments for the local demo cluster."""
import time

import pyarrow.parquet as pq
from pyarrow import BufferReader

from .runtime import (api, contents, db, manifest, objects, register, s3,
                      save_report, wait_running)
from .events import read_jsonl

EXTRA_SOURCES = {
    "products-source": ("cdc_products_slot", "cdc_products_pub"),
    "parquet-source": ("cdc_parquet_slot", "cdc_parquet_pub"),
    "stock-source": ("cdc_stock_slot", "cdc_stock_pub"),
}


def source_variant(name, table, prefix, schemas=False):
    value = manifest("source-debezium.json")
    value["name"] = name
    config = value["config"]
    config["slot.name"], config["publication.name"] = EXTRA_SOURCES[name]
    config["table.include.list"] = table
    config["topic.prefix"] = prefix
    config["key.converter.schemas.enable"] = str(schemas).lower()
    config["value.converter.schemas.enable"] = str(schemas).lower()
    config.pop("heartbeat.interval.ms", None)
    config.pop("heartbeat.action.query", None)
    return value


def drop_source_state(name):
    slot, publication = EXTRA_SOURCES[name]
    if name in api("/connectors"):
        raise RuntimeError(f"Delete connector {name} before dropping its state")
    with db() as connection:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            row = connection.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name=%s", (slot,)).fetchone()
            if not row:
                break
            if not row[0]:
                connection.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"Slot {slot} is still active; nothing was dropped")
        from psycopg import sql
        connection.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(publication)))


def cleanup():
    for name in ["stock-s3-sink", "parquet-s3-sink", *EXTRA_SOURCES]:
        if name in api("/connectors"):
            api(f"/connectors/{name}", "DELETE")
    for name in EXTRA_SOURCES:
        drop_source_state(name)
    print("Extra connectors, their inactive slots and publications removed. Topics and S3 files retained.")


def run(name):
    if name in ("pause-sink", "resume-sink", "pause-source", "resume-source"):
        action, kind = name.split("-")
        connector = "shop-source" if kind == "source" else "orders-s3-sink"
        api(f"/connectors/{connector}/{action}", "PUT")
        print(f"Requested {action} for {connector}. Inspect status, consumer lag and WAL.")
    elif name == "schema":
        with db() as connection:
            connection.execute("ALTER TABLE shop.orders ADD COLUMN IF NOT EXISTS "
                               "channel TEXT NOT NULL DEFAULT 'web'")
            row = connection.execute(
                "INSERT INTO shop.orders (customer_id, product_id, quantity, amount, status, channel) "
                "VALUES (1,1,44,179.98,'schema-demo','mobile') RETURNING order_id").fetchone()
        print(f"Added channel; order {row[0]} has channel=mobile. Compare old/new JSONL files.")
    elif name == "orphan":
        register(source_variant("products-source", "shop.products", "cdc-products"))
        api("/connectors/products-source", "DELETE")
        print("Deleted products-source. Its slot and publication remain; inspect status, then cleanup.")
    elif name == "heartbeat":
        value = source_variant("products-source", "shop.products", "cdc-products")
        register(value)
        print("Idle products capture has no action heartbeat. Order writes generate WAL outside its publication.")
        time.sleep(12)
        with db() as connection:
            before = connection.execute(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                "WHERE slot_name='cdc_products_slot'").fetchone()
        value["config"].update({
            "table.include.list": "shop.products,shop.dbz_heartbeat",
            "heartbeat.interval.ms": "10000",
            "heartbeat.action.query": "UPDATE shop.dbz_heartbeat SET ts=now() WHERE id=1"})
        register(value)
        time.sleep(22)
        with db() as connection:
            after = connection.execute(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                "WHERE slot_name='cdc_products_slot'").fetchone()
        save_report("heartbeat.json", {"confirmed_flush_lsn_before": before,
                                       "confirmed_flush_lsn_after": after,
                                       "advanced": before != after})
    elif name == "parquet":
        register(source_variant("parquet-source", "shop.orders", "cdc-pq", schemas=True))
        value = manifest("sink-s3.json")
        value["name"] = "parquet-s3-sink"
        value["config"].update({
            "topics": "cdc-pq.shop.orders",
            "format.output.type": "parquet",
            "format.output.fields": "value",
            "format.output.envelope": "false",
            "file.name.template": "{{topic}}/{{partition}}/{{start_offset:padding=true}}.parquet",
            "key.converter.schemas.enable": "true",
            "value.converter.schemas.enable": "true",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.delete.tombstone.handling.mode": "drop"})
        register(value)
        client = s3()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            wait_running("parquet-s3-sink")
            files = objects(client, "cdc-pq.shop.orders/")
            if files:
                table = pq.read_table(BufferReader(contents(client, files[0]["Key"])))
                save_report("parquet.json", {"file": files[0]["Key"],
                                            "rows": table.num_rows, "schema": str(table.schema)})
                return
            time.sleep(3)
        raise TimeoutError("No Parquet output arrived")
    elif name == "compaction":
        with db() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS shop.stock (product_id INTEGER PRIMARY KEY, "
                               "warehouse TEXT NOT NULL DEFAULT 'central', qty INTEGER NOT NULL DEFAULT 0, "
                               "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        value = source_variant("stock-source", "shop.stock", "cdc-stock")
        value["config"].update({
            "topic.creation.default.cleanup.policy": "compact",
            "topic.creation.default.segment.ms": "10000",
            "topic.creation.default.min.cleanable.dirty.ratio": "0.01",
            "topic.creation.default.min.compaction.lag.ms": "0",
            "topic.creation.default.delete.retention.ms": "1000"})
        register(value)
        with db() as connection:
            connection.execute("INSERT INTO shop.stock(product_id,qty) SELECT g,0 "
                               "FROM generate_series(1,100) g ON CONFLICT (product_id) DO UPDATE SET qty=0")
            for _ in range(30):
                connection.execute("UPDATE shop.stock SET qty=qty+1, updated_at=now()")
            for _ in range(35):
                connection.execute("UPDATE shop.stock SET qty=qty+1, updated_at=now() WHERE product_id=1")
                time.sleep(2)
        sink = manifest("sink-s3.json")
        sink["name"] = "stock-s3-sink"
        sink["config"]["topics"] = "cdc-stock.shop.stock"
        register(sink)
        client = s3()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            files = objects(client, "cdc-stock.shop.stock/")
            if files:
                records = [record for item in files for record in
                           read_jsonl(contents(client, item["Key"]).decode())]
                keys = {record["key"]["product_id"] for record in records}
                if len(keys) == 100:
                    save_report("compaction.json", {
                        "unique_product_keys": len(keys), "exported_records": len(records),
                        "last_observed_offset": max(record["offset"] for record in records),
                        "note": "Compaction is asynchronous; offset gaps are preserved."})
                    return
            time.sleep(3)
        raise TimeoutError("Stock sink did not export all 100 product keys")
    elif name == "restore":
        register(manifest("source-debezium.json"))
        register(manifest("sink-s3.json"))
        api("/connectors/shop-source/resume", "PUT")
        api("/connectors/orders-s3-sink/resume", "PUT")
        print("Main connector configurations restored and resume requested. Added columns remain.")
    else:
        raise ValueError(f"Unknown scenario: {name}")
