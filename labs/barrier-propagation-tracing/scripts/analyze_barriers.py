#!/usr/bin/env python3
"""Анализ traced barrier logs.

Usage:
    python3 analyze_barriers.py /tmp/barriers.log

Парсит строки вида:
    [barrier-tracer] ckpt=42 op=StatefulProcess task=2/4 phase=SNAPSHOT_START t=1731234567890

Строит таблицу:
    ckpt | per-stage timestamps относительно начала checkpoint | total duration
"""
import re
import sys
from collections import defaultdict

LINE_RE = re.compile(
    r"\[barrier-tracer\]\s+"
    r"ckpt=(?P<ckpt>\d+)\s+"
    r"op=(?P<op>\S+)\s+"
    r"task=(?P<task>\d+/\d+)\s+"
    r"phase=(?P<phase>\S+)\s+"
    r"t=(?P<t>\d+)"
)


def main(path: str) -> None:
    events = defaultdict(list)  # ckpt -> [(op, phase, t)]

    with open(path, "r") as f:
        for line in f:
            m = LINE_RE.search(line)
            if not m:
                continue
            ckpt = int(m["ckpt"])
            events[ckpt].append((m["op"], m["phase"], int(m["t"])))

    if not events:
        print("Нет barrier events в файле", file=sys.stderr)
        sys.exit(1)

    print(f"{'ckpt':>6} {'op':<20} {'phase':<18} {'t_offset_ms':>12}  duration_ms")
    print("-" * 70)

    for ckpt in sorted(events.keys()):
        evs = sorted(events[ckpt], key=lambda x: x[2])
        t0 = evs[0][2]
        for op, phase, t in evs:
            offset = t - t0
            print(f"{ckpt:>6} {op:<20} {phase:<18} {offset:>12}")
        # last phase - first phase = total checkpoint duration as seen by operators
        total = evs[-1][2] - evs[0][2]
        print(f"  -> total duration: {total}ms\n")

    # Aggregate per-phase stats
    print("\n=== Aggregate per-operator durations (avg, ms) ===")
    per_op = defaultdict(lambda: defaultdict(list))
    for ckpt, evs in events.items():
        phase_t = {(op, phase): t for op, phase, t in evs}
        for (op, phase), t in phase_t.items():
            if phase == "SNAPSHOT_END":
                start = phase_t.get((op, "SNAPSHOT_START"))
                if start:
                    per_op[op]["snapshot_duration"].append(t - start)

    for op, stats in per_op.items():
        for key, values in stats.items():
            if values:
                avg = sum(values) / len(values)
                print(f"{op:<20} {key:<22} avg={avg:.1f}ms n={len(values)}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: analyze_barriers.py <logs>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
