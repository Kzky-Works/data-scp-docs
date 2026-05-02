#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

HEADER = ["tag", "yomi", "note"]
SCHEMA_VERSION = 1


def read_review_tsv(path: Path) -> tuple[list[dict[str, str]], int]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    skipped_empty_yomi = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames != HEADER:
            raise ValueError(f"{path}: expected TSV header {HEADER}, got {reader.fieldnames}")
        for line_no, row in enumerate(reader, start=2):
            tag = (row.get("tag") or "").strip()
            yomi = (row.get("yomi") or "").strip()
            if not tag:
                raise ValueError(f"{path}:{line_no}: empty tag")
            if tag in seen:
                raise ValueError(f"{path}:{line_no}: duplicate tag {tag!r}")
            seen.add(tag)
            if not yomi:
                skipped_empty_yomi += 1
                continue
            entries.append({"tag": tag, "yomi": yomi})
    return entries, skipped_empty_yomi


def build_json(input_path: Path, output_path: Path, *, source: str) -> tuple[int, int]:
    entries, skipped = read_review_tsv(input_path)
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "entries": entries,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return len(entries), skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build app JSON from reviewed SCP-JP tag yomi TSV.")
    parser.add_argument(
        "-i",
        "--input",
        default="data-scp-docs/list/jp/tag_yomi_review.tsv",
        help="Path to reviewed TSV",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data-scp-docs/list/jp/tag_yomi.json",
        help="Path to output JSON",
    )
    parser.add_argument(
        "--source",
        default="data-scp-docs/list/jp/tag_yomi_review.tsv",
        help="Source label stored in JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    written, skipped = build_json(Path(args.input), Path(args.output), source=args.source)
    print(f"wrote {written} yomi entries to {args.output}; skipped {skipped} empty yomi rows")


if __name__ == "__main__":
    main()
