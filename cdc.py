#!/usr/bin/env python3
"""Portable host CLI. Only Python's standard library and Docker Compose needed."""
import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def compose(*arguments, check=True):
    return subprocess.run(["docker", "compose", *arguments], cwd=ROOT, check=check)


def tool(*arguments):
    (ROOT / "artifacts").mkdir(exist_ok=True)
    compose("--profile", "tools", "run", "--rm", "--no-deps", "-T", "tools", *arguments)


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL -> Kafka -> MinIO CDC lab")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "up", "down", "register", "status", "verify", "demo", "cleanup"):
        sub.add_parser(command)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--limit", type=int, default=3)
    replay = sub.add_parser("replay")
    replay.add_argument("--compare", action="store_true")
    scenario = sub.add_parser("scenario")
    scenario.add_argument("name", choices=["pause-sink", "resume-sink", "pause-source",
                                         "resume-source", "schema", "orphan", "heartbeat",
                                         "parquet", "compaction", "restore"])
    apply = sub.add_parser("apply")
    apply.add_argument("filename", help="JSON manifest filename inside connectors/")
    sql = sub.add_parser("sql")
    sql.add_argument("query", nargs="?")
    logs = sub.add_parser("logs")
    logs.add_argument("service", nargs="?", default="connect")
    generator = sub.add_parser("generator")
    generator.add_argument("action", choices=["start", "stop"])
    reset = sub.add_parser("reset")
    reset.add_argument("--yes", action="store_true", help="Delete this project's volumes")
    args = parser.parse_args()
    if args.command == "init":
        target = ROOT / ".env"
        if not target.exists():
            shutil.copyfile(ROOT / ".env.example", target)
        print("Local .env is ready; edit it to change demo credentials or host ports.")
        return
    if not (ROOT / ".env").exists():
        parser.error("Run: python cdc.py init")
    if args.command == "up":
        compose("up", "-d", "--build", "--wait", "--wait-timeout", "600")
        compose("--profile", "tools", "build", "tools")
        tool("register")
        print("Kafka UI: http://localhost:8080 | MinIO console: http://localhost:9001")
    elif args.command == "down":
        compose("down")
    elif args.command == "reset":
        if not args.yes:
            parser.error("Reset deletes local DB, Kafka log and S3 objects. Use --yes explicitly.")
        compose("--profile", "tools", "down", "--volumes", "--remove-orphans")
    elif args.command == "logs":
        compose("logs", "--tail", "150", args.service)
    elif args.command == "generator":
        compose("start" if args.action == "start" else "stop", "generator")
    elif args.command == "sql":
        if args.query:
            compose("exec", "-T", "postgres", "psql", "-U", "cdc", "-d", "shop", "-v",
                    "ON_ERROR_STOP=1", "-c", args.query)
        else:
            compose("exec", "postgres", "psql", "-U", "cdc", "-d", "shop")
    elif args.command == "demo":
        tool("verify")
        tool("inspect", "--limit", "1")
        tool("status")
    elif args.command == "replay":
        if args.compare:
            # Stable source state is required to compare the two systems.
            compose("stop", "generator")
            tool("replay", "--compare", "--settle")
            print("Generator is stopped. Resume: python cdc.py generator start")
        else:
            tool("replay")
    elif args.command == "inspect":
        tool("inspect", "--limit", str(args.limit))
    elif args.command == "scenario":
        tool("scenario", args.name)
    elif args.command == "apply":
        if Path(args.filename).name != args.filename or not args.filename.endswith(".json"):
            parser.error("Use a JSON filename inside connectors/")
        tool("apply", args.filename)
    else:
        tool(args.command)


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, OSError) as error:
        print(f"Command failed: {error}", file=sys.stderr)
        sys.exit(1)
