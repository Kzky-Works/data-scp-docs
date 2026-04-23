#!/usr/bin/env python3
"""
ScpDocs 3 系統フィード用: `scp.json`（本家メイン和訳一覧、`SCPArticleListPayload` 互換）を生成する。

アプリの `AppRemoteConfig.scpENListJSONPathComponent` は **`scp.json`**（`scp-en.json` ではない）。

各 entry:
  - u / i: `https://scp-jp.wikidot.com/scp-NNN`（`-jp` なし）
  - t: 本家メイン和訳一覧（scp-series …）の行タイトル
  - o: 任意。日本支部オリジナル（`-jp`）の行タイトル = `docs/scp_list.json` の `title`
  - c / g: `docs/scp_list.json` の objectClass / tags をマージ
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import time

import requests

from _scp_app_feed_common import (
    HTTP_HEADERS,
    REQUEST_DELAY_SEC,
    atomic_write_json,
    fetch_mainlist_rows,
    load_scp_list_entry_blobs,
    mainlist_article_path,
    mainlist_article_url,
    mainlist_stable_id,
)

OBJECT_CLASS_RE = re.compile(
    r"<strong>\s*(?:オブジェクトクラス|Object Class)\s*:\s*</strong>\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)
OBJECT_CLASS_RE_LOOSE = re.compile(
    r"(?:オブジェクトクラス|Object Class)\s*[:：]\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)


def extract_object_class_from_html(html: str) -> str | None:
    m = OBJECT_CLASS_RE.search(html)
    if not m:
        m = OBJECT_CLASS_RE_LOOSE.search(html)
    if not m:
        return None
    oc = m.group(1).strip()
    return oc if oc else None


def fetch_article_metadata_class_only(
    session: requests.Session,
    article_path: str,
    *,
    delay_sec: float,
    retries: int = 5,
    verbose: bool = False,
) -> str | None:
    url = urljoin("https://scp-jp.wikidot.com/", article_path)
    last_err: Exception | None = None
    for attempt in range(retries):
        time.sleep(delay_sec)
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=90)
            if r.status_code in (403, 429, 503) and attempt < retries - 1:
                time.sleep(min(120, 15 * (2**attempt)))
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            oc = extract_object_class_from_html(r.text)
            if verbose:
                print(f"INFO: {article_path} c={oc!r}", file=sys.stderr)
            return oc
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(90, 8 * (attempt + 1)))
    if verbose and last_err:
        print(f"WARN: {article_path} {last_err!r}", file=sys.stderr)
    return None


def build_entries(
    session: requests.Session,
    *,
    blobs: dict[tuple[int, int], dict[str, Any]],
    with_article_metadata: bool,
    metadata_delay_sec: float,
    metadata_max_articles: int | None,
    verbose: bool,
) -> list[dict[str, Any]]:
    rows = fetch_mainlist_rows(session)
    if not rows:
        raise RuntimeError("No mainlist rows parsed")
    out: list[dict[str, Any]] = []
    meta_done = 0
    for r in rows:
        series = int(r["series"])
        n = int(r["scpNumber"])
        key = (series, n)
        t = str(r["title"]).strip()
        u = mainlist_article_url(n)
        i = mainlist_stable_id(n)
        b = blobs.get(key, {})
        o_val = b.get("title")
        oc = b.get("objectClass")
        if isinstance(oc, str) and not oc.strip():
            oc = None
        tags = b.get("tags")

        if with_article_metadata and (metadata_max_articles is None or meta_done < metadata_max_articles):
            path = mainlist_article_path(n)
            try:
                oc2 = fetch_article_metadata_class_only(
                    session, path, delay_sec=metadata_delay_sec, verbose=verbose
                )
                if oc2:
                    oc = oc2
                meta_done += 1
            except Exception as ex:
                print(f"WARN: metadata {path}: {ex}", file=sys.stderr)

        entry: dict[str, Any] = {"u": u, "i": i, "t": t}
        if isinstance(oc, str) and oc.strip():
            entry["c"] = oc.strip()
        if isinstance(o_val, str) and o_val.strip():
            entry["o"] = o_val.strip()
        if isinstance(tags, list) and tags:
            entry["g"] = list(tags)
        out.append(entry)
    return out


def default_out_path() -> str:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(root, "scp.json")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Generate ScpDocs SCPArticleListPayload JSON (scp.json, EN/mainlist feed)."
    )
    p.add_argument("--out", default=None, help="Output path (default: <repo>/scp.json)")
    p.add_argument(
        "--merge-metadata-from",
        default=None,
        metavar="PATH",
        help="Merge title (-jp 行), objectClass, tags from scp_list.json (e.g. docs/scp_list.json).",
    )
    p.add_argument(
        "--with-article-metadata",
        action="store_true",
        help="Fetch each mainlist article page to refresh objectClass only (slow).",
    )
    p.add_argument("--metadata-delay-sec", type=float, default=None)
    p.add_argument("--metadata-max-articles", type=int, default=None)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    out_path = args.out or default_out_path()

    blobs = load_scp_list_entry_blobs(args.merge_metadata_from or "")
    session = requests.Session()
    delay = args.metadata_delay_sec if args.metadata_delay_sec is not None else REQUEST_DELAY_SEC

    try:
        entries = build_entries(
            session,
            blobs=blobs,
            with_article_metadata=args.with_article_metadata,
            metadata_delay_sec=delay,
            metadata_max_articles=args.metadata_max_articles,
            verbose=args.verbose,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "listVersion": int(now.timestamp()),
        "schemaVersion": 1,
        "generatedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "entries": entries,
    }

    try:
        atomic_write_json(out_path, payload, verbose=args.verbose)
    except Exception as e:
        print(f"ERROR: validate/write failed: {e}", file=sys.stderr)
        return 1

    print(
        f"Wrote {out_path} ({len(entries)} entries); scp_list keys: {len(blobs)}.",
        file=sys.stderr if args.verbose else sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
