#!/usr/bin/env python3
"""Tales マルチフォーム UI 同梱リリース向けゲート: manifest_tales.json の lu 十分率を検証する。

data-scp-docs の `main` 上のファイルを `--url` で指して実行し、通過してから
App Store ビルドを出すと、著者行・子行の更新日が空にならない。
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from typing import Any


def load_manifest(source: str) -> dict[str, Any]:
    if source.startswith("http://") or source.startswith("https://"):
        req = urllib.request.Request(
            source,
            headers={"User-Agent": "scp-docs-verify-manifest-tales/1.0"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
        return json.loads(body.decode("utf-8"))
    with open(source, encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Verify manifest_tales.json lu coverage for release (Tales list dates)"
    )
    p.add_argument(
        "path_or_url",
        nargs="?",
        default="",
        help="Local path or https URL to manifest_tales.json",
    )
    p.add_argument(
        "--url",
        type=str,
        default="",
        help="URL to fetch (overrides positional when set)",
    )
    p.add_argument(
        "--min-lu-ratio",
        type=float,
        default=0.85,
        help="Minimum fraction of entries with positive integer lu (default 0.85)",
    )
    args = p.parse_args()
    src = (args.url or args.path_or_url).strip()
    if not src:
        print("ERROR: provide path or --url", file=sys.stderr)
        return 1
    try:
        data = load_manifest(src)
    except Exception as e:
        print(f"ERROR: load failed: {e}", file=sys.stderr)
        return 1
    if not isinstance(data, dict):
        print("ERROR: root must be object", file=sys.stderr)
        return 1
    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        print("ERROR: entries must be non-empty list", file=sys.stderr)
        return 1
    if data.get("listVersion") is None:
        print("ERROR: missing listVersion", file=sys.stderr)
        return 1
    if int(data.get("schemaVersion") or 0) < 2:
        print("ERROR: schemaVersion must be >= 2", file=sys.stderr)
        return 1
    with_lu = 0
    for e in entries:
        if not isinstance(e, dict):
            continue
        lu = e.get("lu")
        if isinstance(lu, int) and lu > 0:
            with_lu += 1
    n = len(entries)
    ratio = with_lu / n if n else 0.0
    print(
        "INFO: "
        f"entries={n} with_lu={with_lu} ratio={ratio:.4f} "
        f"listVersion={data.get('listVersion')} schemaVersion={data.get('schemaVersion')}"
    )
    if ratio + 1e-9 < float(args.min_lu_ratio):
        print(
            f"ERROR: lu ratio {ratio:.4f} < required {args.min_lu_ratio}. "
            "Run scripts/harvester.py (with lu enrichment) and push "
            "data-scp-docs `main` so MultiformContentSyncService can bump clients.",
            file=sys.stderr,
        )
        return 1
    print("OK: manifest_tales lu gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
