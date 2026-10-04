import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083").rstrip("/")
BUCKET = os.environ.get("S3_BUCKET", "cdc-lake")
ARTIFACTS = Path("/artifacts")


def db():
    return psycopg.connect(autocommit=True)


def api(path, method="GET", data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = Request(CONNECT_URL + path, data=body, method=method,
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            content = response.read()
            return json.loads(content) if content else None
    except HTTPError as error:
        raise RuntimeError(f"Connect {method} {path}: HTTP {error.code}: "
                           f"{error.read().decode()}") from error


def s3():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        region_name="us-east-1",
                        config=Config(signature_version="s3v4",
                                      s3={"addressing_style": "path"}))


def objects(client, prefix="cdc.shop.orders/"):
    result = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        result.extend(page.get("Contents", []))
    return sorted(result, key=lambda item: (
        int(item["Key"].split("/")[-2]),
        int(item["Key"].rsplit("/", 1)[-1].split(".")[0])))


def contents(client, key):
    response = client.get_object(Bucket=BUCKET, Key=key)
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


def manifest(filename):
    value = json.loads((Path("/app/connectors") / filename).read_text())
    if "aws.s3.bucket.name" in value["config"]:
        value["config"]["aws.s3.bucket.name"] = BUCKET
    return value


def register(value):
    api(f"/connectors/{value['name']}/config", "PUT", value["config"])
    api(f"/connectors/{value['name']}/resume", "PUT")
    api(f"/connectors/{value['name']}/restart?includeTasks=true&onlyFailed=true", "POST")
    wait_running(value["name"])


def wait_running(name, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = api(f"/connectors/{name}/status")
        tasks = last.get("tasks", [])
        if any(task["state"] == "FAILED" for task in tasks):
            raise RuntimeError(json.dumps(last, indent=2))
        if last["connector"]["state"] == "RUNNING" and tasks and all(
                task["state"] == "RUNNING" for task in tasks):
            return
        time.sleep(2)
    raise TimeoutError(f"Connector {name} did not start: {last}")


def bootstrap():
    client = s3()
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        client.create_bucket(Bucket=BUCKET)
    register(manifest("source-debezium.json"))
    register(manifest("sink-s3.json"))
    print(f"Registered shop-source and orders-s3-sink. Bucket: {BUCKET}")


def save_report(name, report):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    path = ARTIFACTS / name
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return path
