#!/usr/bin/env python3
"""manifest_*.json の metadata キーが entries[].i にすべて存在することを検証する。"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


def check_file(path: str) -> list[str]:
    errs: list[str] = []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return [f"{path}: root must be object"]
    entries = data.get("entries")
    metadata = data.get("metadata")
    if not isinstance(entries, list):
        errs.append(f"{path}: missing entries array")
        return errs
    ids: set[str] = set()
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            errs.append(f"{path}: entries[{i}] not object")
            continue
        slug = e.get("i")
        if isinstance(slug, str) and slug.strip():
            ids.add(slug.strip())
    if not isinstance(metadata, dict):
        return errs
    for k in metadata:
        if k not in ids:
            errs.append(f"{path}: metadata orphan key {k!r}")
    return errs


def main() -> int:
    p = argparse.ArgumentParser(description="Validate manifest_scp-*.json metadata keys")
    p.add_argument(
        "dir",
        nargs="?",
        default=os.path.join(os.path.dirname(__file__), "..", "list", "jp"),
        help="Directory containing manifest_*.json",
    )
    args = p.parse_args()
    d = os.path.abspath(args.dir)
    if not os.path.isdir(d):
        print(f"ERROR: not a directory: {d}", file=sys.stderr)
        return 1
    names = sorted(x for x in os.listdir(d) if x.startswith("manifest_") and x.endswith(".json"))
    if not names:
        print(f"WARN: no manifest_*.json in {d}", file=sys.stderr)
        return 0
    failed = False
    for n in names:
        path = os.path.join(d, n)
        for err in check_file(path):
            print(err, file=sys.stderr)
            failed = True
    if failed:
        return 1
    print(f"OK: {len(names)} manifest(s) in {d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
