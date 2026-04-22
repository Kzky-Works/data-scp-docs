#!/usr/bin/env python3
"""
SCP-JP Wikidot の system:page-tags を巡回し、記事 URL の種別ごとにタグを「混ぜず」に集計して
カテゴリ別 JSON に出力する。

問題回避: `/scp-N`（本家メイン和訳）と `/scp-N-jp`（日本支部オリジナル）・`/scp-N-j`（Joke）は
別エントリとして扱う（従来の単一 mergeKey への混在マージは行わない）。

標準ライブラリのみ。data-scp-docs リポジトリが正。
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from _wikidot_tags_common import PAGE_TAGS_INDEX, collect_slugs_for_tag, extract_tag_refs_from_index
from _wikidot_tags_common import fetch_html_retry


DEFAULT_UA = "ScpDocsCatalogBot/1.0 (+https://github.com/Kzky-Works/data-scp-docs)"

# オブジェクトクラスとしてタグ文字列から昇格させる語（タグ一覧と区別）
_OBJECT_CLASS_LOWER = frozenset(
    {
        "safe",
        "euclid",
        "keter",
        "thaumiel",
        "neutralized",
        "explained",
        "apollyon",
        "デマーカー",
        "提案中",
    }
)

CATEGORIES = ("scp_jp", "scp", "joke", "tales", "canon", "goi")

RE_SCP_JP = re.compile(r"^scp-(\d+)-jp$", re.IGNORECASE)
RE_SCP_JOKE = re.compile(r"^scp-(\d+)-j$", re.IGNORECASE)
RE_SCP_MAIN = re.compile(r"^scp-(\d+)$", re.IGNORECASE)
# 国際支部の /scp-N-xx（-jp 以外）は本スキーマでは別管理のためページタグから除外
RE_SCP_INTL = re.compile(r"^scp-\d+-[a-z]{2}$", re.IGNORECASE)


def series_for_number(n: int) -> int:
    if n <= 999:
        return 0
    if n <= 1999:
        return 1
    if n <= 2999:
        return 2
    if n <= 3999:
        return 3
    return 4


def classify_slug(slug: str) -> tuple[str | None, dict[str, Any]]:
    """
    ページスラッグをカテゴリへ分類。国際支部記事など対象外は (None, {})。
    Returns: (category or None, identity dict)
    """
    raw = slug.strip()
    lower = raw.lower()

    if RE_SCP_INTL.match(lower) and not lower.endswith("-jp"):
        return (None, {})

    m = RE_SCP_JP.match(lower)
    if m:
        n = int(m.group(1))
        if not (1 <= n <= 4999):
            return (None, {})
        return (
            "scp_jp",
            {"series": series_for_number(n), "scpNumber": n, "slug": raw},
        )

    m = RE_SCP_JOKE.match(lower)
    if m:
        n = int(m.group(1))
        if not (1 <= n <= 4999):
            return (None, {})
        return (
            "joke",
            {"series": series_for_number(n), "scpNumber": n, "slug": raw},
        )

    m = RE_SCP_MAIN.match(lower)
    if m:
        n = int(m.group(1))
        if not (1 <= n <= 4999):
            return (None, {})
        return (
            "scp",
            {"series": series_for_number(n), "scpNumber": n, "slug": raw},
        )

    return (_classify_non_scp_bucket(lower), {"slug": raw})


def _classify_non_scp_bucket(lower: str) -> str:
    """SCP 番号形式以外。ヒューリスティック（ hub 専用クロールなし）。"""
    if "goi-format" in lower or lower.startswith("goc-") or lower.startswith("goi-"):
        return "goi"
    if "canon" in lower or lower.startswith("canon-"):
        return "canon"
    if lower.startswith("component:") or lower.startswith("fragment:"):
        return "goi"
    return "tales"


def entry_key_for(category: str, identity: dict[str, Any]) -> str:
    if category in ("scp_jp", "scp", "joke"):
        return f"{identity['series']}_{identity['scpNumber']}_{category}"
    return identity["slug"].lower()


def absolute_url(slug: str) -> str:
    return f"https://scp-jp.wikidot.com/{slug}"


def split_object_class(tags: set[str]) -> tuple[str | None, set[str]]:
    """known OC 語を 1 個だけ objectClass にし、tags から除く。"""
    oc: str | None = None
    rest: set[str] = set()
    for t in tags:
        tl = t.strip().lower()
        if tl in _OBJECT_CLASS_LOWER and oc is None:
            # 元の表記を保持（先頭に合わせた表記）
            oc = t.strip()
        else:
            rest.add(t)
    return oc, rest


def load_scp_list(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def enrich_scp_entry(category: str, identity: dict[str, Any], scp_list: dict[str, Any] | None) -> str | None:
    if scp_list is None or category not in ("scp_jp", "scp", "joke"):
        return None
    entries = scp_list.get("entries")
    if not isinstance(entries, list):
        return None
    series = identity["series"]
    sn = identity["scpNumber"]
    for e in entries:
        if not isinstance(e, dict):
            continue
        if e.get("series") == series and e.get("scpNumber") == sn:
            if category == "scp_jp":
                t = e.get("title")
                return t.strip() if isinstance(t, str) else None
            if category == "scp":
                mt = e.get("mainlistTranslationTitle")
                if isinstance(mt, str) and mt.strip():
                    return mt.strip()
                return None
            if category == "joke":
                t = e.get("title")
                return t.strip() if isinstance(t, str) else None
    return None


def next_list_version(out_path: Path) -> int:
    if not out_path.is_file():
        return 1
    try:
        with out_path.open(encoding="utf-8") as f:
            prev = json.load(f)
        v = prev.get("listVersion", 0)
        return int(v) + 1
    except (json.JSONDecodeError, TypeError, ValueError):
        return 1


def write_catalog(
    *,
    kind: str,
    out_path: Path,
    entries_out: list[dict[str, Any]],
    sync_ts: str,
) -> None:
    lv = next_list_version(out_path)
    payload = {
        "kind": kind,
        "schemaVersion": 1,
        "listVersion": lv,
        "generatedAt": sync_ts,
        "entries": entries_out,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def build_entries_for_category(
    category: str,
    raw: dict[str, dict[str, Any]],
    scp_list: dict[str, Any] | None,
    sync_ts: str,
) -> list[dict[str, Any]]:
    """raw: entry_key -> {identity dict with .tags set}"""
    move_oc = category in ("scp_jp", "scp", "joke")
    rows: list[dict[str, Any]] = []

    for _ek, bucket in sorted(raw.items(), key=lambda x: x[0]):
        identity = {k: v for k, v in bucket.items() if k != "tags"}
        tags = bucket.get("tags")
        if not isinstance(tags, set):
            tags = set()
        oc = None
        tag_list = tags
        if move_oc:
            oc, tag_list = split_object_class(tags)

        if category in ("scp_jp", "scp", "joke"):
            row: dict[str, Any] = {
                "series": identity["series"],
                "scpNumber": identity["scpNumber"],
                "slug": identity["slug"],
                "url": absolute_url(identity["slug"]),
                "title": enrich_scp_entry(category, identity, scp_list),
                "tags": sorted(tag_list),
                "tagsSyncedAt": sync_ts,
            }
            if oc:
                row["objectClass"] = oc
            rows.append(row)
        else:
            slug = identity["slug"]
            rows.append(
                {
                    "slug": slug,
                    "url": absolute_url(slug),
                    "title": None,
                    "tags": sorted(tag_list),
                    "tagsSyncedAt": sync_ts,
                }
            )

    if category in ("scp_jp", "scp", "joke"):
        rows.sort(key=lambda r: (r["series"], r["scpNumber"]))
    else:
        rows.sort(key=lambda r: r["slug"].lower())
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="page-tags → カテゴリ別カタログ JSON")
    ap.add_argument("--out-dir", type=Path, default=Path("docs/catalog"), help="出力ディレクトリ")
    ap.add_argument("--scp-list-path", type=Path, default=Path("docs/scp_list.json"), help="タイトル補完用")
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--user-agent", default=DEFAULT_UA)
    ap.add_argument("--max-tags", type=int, default=0)
    ap.add_argument("--shuffle-tags", action="store_true")
    ap.add_argument("--dry-run-tags", action="store_true")
    args = ap.parse_args()

    user_agent = args.user_agent

    index_html = fetch_html_retry(PAGE_TAGS_INDEX, user_agent, retries=4, sleep=args.sleep)
    time.sleep(args.sleep)
    tag_refs = extract_tag_refs_from_index(index_html)
    print(f"[info] tags in cloud: {len(tag_refs)}", file=sys.stderr)

    if args.dry_run_tags:
        print(json.dumps([{"path": t.path, "display_name": t.display_name} for t in tag_refs], ensure_ascii=False, indent=2))
        return 0

    if args.shuffle_tags:
        random.shuffle(tag_refs)
    if args.max_tags > 0:
        tag_refs = tag_refs[: args.max_tags]

    # entry_key -> {identity fields + tags set}
    accum: dict[str, dict[str, dict[str, Any]]] = {c: {} for c in CATEGORIES}

    for i, tag in enumerate(tag_refs):
        label = tag.display_name
        try:
            slugs = collect_slugs_for_tag(tag, user_agent, args.sleep, retries=4)
        except Exception as e:
            print(f"[warn] tag skip {tag.path}: {e}", file=sys.stderr)
            continue
        finally:
            time.sleep(args.sleep)

        for slug in slugs:
            cat, identity = classify_slug(slug)
            if cat is None or cat not in accum:
                continue
            ek = entry_key_for(cat, identity)
            bucket = accum[cat].setdefault(ek, {**identity, "tags": set()})
            if "tags" not in bucket:
                bucket["tags"] = set()
            assert isinstance(bucket["tags"], set)
            bucket["tags"].add(label)

        if (i + 1) % 25 == 0 or i == 0:
            print(f"[info] progress {i + 1}/{len(tag_refs)} tag={label!r} slugs={len(slugs)}", file=sys.stderr)

    scp_list = load_scp_list(args.scp_list_path)
    sync_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    out_dir = args.out_dir
    mapping = [
        ("scp_jp", "scp_jp.json"),
        ("scp", "scp.json"),
        ("joke", "joke.json"),
        ("tales", "tales.json"),
        ("canon", "canon.json"),
        ("goi", "goi.json"),
    ]

    for cat, fname in mapping:
        entries = build_entries_for_category(cat, accum[cat], scp_list, sync_ts)
        path = out_dir / fname
        write_catalog(kind=cat, out_path=path, entries_out=entries, sync_ts=sync_ts)
        print(f"[done] {path} entries={len(entries)}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
