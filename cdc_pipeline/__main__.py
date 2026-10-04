import argparse
import sys

from . import checks, generator, runtime, scenarios


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("register", "status", "generate", "verify", "cleanup"):
        sub.add_parser(name)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--limit", type=int, default=3)
    replay = sub.add_parser("replay")
    replay.add_argument("--compare", action="store_true")
    replay.add_argument("--settle", action="store_true")
    scenario = sub.add_parser("scenario")
    scenario.add_argument("name")
    apply = sub.add_parser("apply")
    apply.add_argument("filename")
    args = parser.parse_args()
    if args.command == "register":
        runtime.bootstrap()
    elif args.command == "generate":
        generator.generate()
    elif args.command == "verify":
        checks.verify()
    elif args.command == "status":
        checks.status()
    elif args.command == "inspect":
        checks.inspect(args.limit)
    elif args.command == "replay":
        if args.settle:
            checks.verify()
            # verify's marker transaction is after the generator has stopped;
            # observing its tombstone in S3 proves prior ordered changes arrived.
        checks.replay(args.compare)
    elif args.command == "scenario":
        scenarios.run(args.name)
    elif args.command == "apply":
        runtime.register(runtime.manifest(args.filename))
        print(f"Applied {args.filename}")
    elif args.command == "cleanup":
        scenarios.cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
