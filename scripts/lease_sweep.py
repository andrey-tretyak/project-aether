"""Sweep lease length x heartbeat mode under event-loop stalls; print a results table.

Each cell runs scripts/chaos_test.py with a mid-run SIGKILL while a fraction of tasks
block the worker's event loop. Shorter leases recover faster; the question is how
many healthy-but-busy workers get declared dead along the way.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run_cell(lease: float, mode: str, args: argparse.Namespace) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
        out = Path(handle.name)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "chaos_test.py"),
        "--kill-after", str(args.kill_after),
        "--allow-false-deaths",
        "--measure-only",
        "--json", str(out),
        "--set", f"AETHER_LEASE_S={lease}",
        "--set", f"AETHER_HEARTBEAT_MODE={mode}",
        "--set", f"AETHER_MOCK_STALL_RATE={args.stall_rate}",
        "--set", f"AETHER_MOCK_STALL_S={args.stall_s}",
    ]
    if args.redis_url:
        cmd += ["--redis-url", args.redis_url]
    subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        return json.loads(out.read_text())
    except (OSError, json.JSONDecodeError):
        return {"passed": False, "error": "no metrics written"}
    finally:
        out.unlink(missing_ok=True)


def _fmt(values: list[float]) -> str:
    return f"{statistics.median(values):.2f}s / {max(values):.2f}s" if values else "n/a"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--leases", default="1.0,1.5,2.0,3.0")
    parser.add_argument("--modes", default="loop,thread")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--stall-rate", type=float, default=0.15, help="fraction of tasks that block the event loop")
    parser.add_argument("--stall-s", type=float, default=1.2, help="how long each of those blocks it")
    parser.add_argument("--kill-after", type=float, default=2.0)
    parser.add_argument("--redis-url", default=None)
    args = parser.parse_args()

    leases = [float(x) for x in args.leases.split(",")]
    modes = args.modes.split(",")

    print(
        f"{args.runs} runs per cell; {args.stall_rate:.0%} of tasks block the event loop for {args.stall_s}s; "
        f"one worker SIGKILLed {args.kill_after}s in.\n"
    )
    print(
        "| Lease | Heartbeat | Workflows completed | Recovery after kill (median / worst) "
        "| Live workers declared dead | Repeated executions |"
    )
    print("| --- | --- | --- | --- | --- | --- |")
    for lease in leases:
        for mode in modes:
            results = [run_cell(lease, mode, args) for _ in range(args.runs)]
            passed = sum(r.get("passed", False) for r in results)
            recoveries = [r["recovered_after_s"] for r in results if r.get("recovered_after_s") is not None]
            false_deaths = sum(r.get("false_deaths", 0) for r in results)
            extra = sum(r.get("extra_executions", 0) for r in results)
            print(
                f"| {lease:g}s | {mode} | {passed}/{len(results)} | {_fmt(recoveries)} | {false_deaths} | {extra} |",
                flush=True,
            )
            for r in results:
                if not r.get("passed"):
                    print(f"<!-- {lease:g}s {mode}: {r.get('error', 'unknown error')} -->", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
