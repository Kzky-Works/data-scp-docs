#!/usr/bin/env python3
"""
ScpDocs 3 系統フィード用: `docs/list/jp/scp-int.json`（国際支部和訳パス、`SCPArticleListPayload` 互換）を生成する。

エントリは `docs/scp_list.json` の `hubLinkedPaths`（例: `/scp-173-fr`）を正とする。
タイトルは scp-international から辿る各一覧 HTML の `<li>` から可能な限り抽出し、
足りない分は既存 `scp-int.json` の `--merge-titles-from` で引き継ぎ、それでも無ければ `i` を表示用に使う。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from _scp_app_feed_common import (
    REQUEST_DELAY_SEC,
    atomic_write_json,
    default_jp_list_feed_dir,
    extract_title_from_li,
    fetch_html_with_retry,
    repo_root,
)

INTERNATIONAL_HUB_URL = "https://scp-jp.wikidot.com/scp-international"
MAX_INTL_LIST_PAGES = 150

INTL_LIST_SUBSTRINGS = (
    "liste-fr",
    "lista-pl",
    "scp-list-ru",
    "scp-serie-de",
    "series-1-pt",
    "scp-it-serie",
    "scp-it-e-",
    "serie-scp-es",
    "scp-series-ua",
    "scp-series-vn",
    "scp-series-ko",
    "scp-series-cn",
    "scp-series-th",
    "scp-series-cs",
    "scp-series-zh",
    "scp-series-sk",
    "series-archive",
    "scp-series-unofficial",
    "joke-scp-series-unofficial",
)

INTL_SCP_ARTICLE_PATH_RE = re.compile(r"^/scp-\d+-[a-z]{2}$")


def is_english_main_series_list(path: str) -> bool:
    pl = path.lower()
    if pl == "/scp-series":
        return True
    return bool(re.match(r"^/scp-series-\d+$", pl))


def looks_like_intl_branch_list_page(path: str) -> bool:
    pl = path.lower()
    if pl in {"/", "/scp-international"}:
        return False
    if "scp-series-jp" in pl:
        return False
    if is_english_main_series_list(path):
        return False
    return any(s in pl for s in INTL_LIST_SUBSTRINGS)


INTL_CRAWL_DELAY_SEC = 0.4


def discover_intl_list_urls(
    session: requests.Session, *, verbose: bool, delay_sec: float
) -> list[str]:
    html = fetch_html_with_retry(session, INTERNATIONAL_HUB_URL, delay_sec=delay_sec)
    soup = BeautifulSoup(html, "html.parser")
    list_urls: list[str] = []
    seen_u: set[str] = set()
    for a in soup.find_all("a", href=True):
        raw = (a.get("href") or "").strip()
        if not raw or raw.startswith("#"):
            continue
        absu = urljoin(INTERNATIONAL_HUB_URL, raw)
        pu = urlparse(absu)
        if pu.netloc != "scp-jp.wikidot.com":
            continue
        path = pu.path or "/"
        if not looks_like_intl_branch_list_page(path):
            continue
        u = urlunparse(("https", "scp-jp.wikidot.com", path, "", "", ""))
        if u not in seen_u:
            seen_u.add(u)
            list_urls.append(u)
    list_urls.sort()
    if verbose:
        print(f"INFO: discovered {len(list_urls)} intl list URLs (cap {MAX_INTL_LIST_PAGES})", file=sys.stderr)
    return list_urls


def extract_intl_titles_from_list_html(html: str) -> dict[str, str]:
    """一覧ページから /scp-nn-xx → 行タイトル（可能なら）。"""
    soup = BeautifulSoup(html, "html.parser")
    base = "https://scp-jp.wikidot.com/"
    out: dict[str, str] = {}
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        raw = (a.get("href") or "").strip()
        if not raw or raw.startswith("#"):
            continue
        path = urlparse(urljoin(base, raw)).path
        if not INTL_SCP_ARTICLE_PATH_RE.match(path):
            continue
        if path.endswith("-jp"):
            continue
        t = extract_title_from_li(li)
        if not t:
            t = a.get_text(separator=" ", strip=True)
        if not t:
            continue
        prev = out.get(path)
        if prev is None or len(t) > len(prev):
            out[path] = t
    return out


def crawl_intl_title_map(
    session: requests.Session, *, verbose: bool, delay_sec: float
) -> dict[str, str]:
    titles: dict[str, str] = {}
    list_urls = discover_intl_list_urls(session, verbose=verbose, delay_sec=delay_sec)
    n_max = min(len(list_urls), MAX_INTL_LIST_PAGES)
    for i, u in enumerate(list_urls[:n_max]):
        if verbose and i > 0 and i % 10 == 0:
            print(f"INFO: intl list crawl {i}/{n_max}", file=sys.stderr)
        try:
            html = fetch_html_with_retry(session, u, retries=6, delay_sec=delay_sec)
        except Exception as e:
            print(f"WARN: skip intl list {u}: {e}", file=sys.stderr)
            continue
        part = extract_intl_titles_from_list_html(html)
        for p, t in part.items():
            prev = titles.get(p)
            if prev is None or len(t) > len(prev):
                titles[p] = t
    if verbose:
        print(f"INFO: intl title map size {len(titles)}", file=sys.stderr)
    return titles


def load_hub_paths(path: str) -> list[str]:
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"hub paths file not found: {path}")
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    hub = payload.get("hubLinkedPaths")
    if not isinstance(hub, list) or not hub:
        raise ValueError("hubLinkedPaths missing or empty — run scp_list international hub job first")
    out: list[str] = []
    for p in hub:
        if not isinstance(p, str) or not p.startswith("/scp-"):
            continue
        if not INTL_SCP_ARTICLE_PATH_RE.match(p):
            continue
        if p.endswith("-jp"):
            continue
        out.append(p)
    # 重複パスを除去（順序維持）
    seen: set[str] = set()
    deduped: list[str] = []
    for p in sorted(out):
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def load_previous_titles(path: str | None) -> dict[str, str]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"WARN: could not load merge-titles-from {path}: {e}", file=sys.stderr)
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return {}
    m: dict[str, str] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        i = e.get("i")
        t = e.get("t")
        if isinstance(i, str) and i.strip() and isinstance(t, str) and t.strip():
            m[i.strip()] = t.strip()
    return m


def fallback_title_from_path(path: str) -> str:
    """一覧クロール不能時の最低限の表示用タイトル（例: /scp-001-cs → SCP-001-CS）。"""
    m = re.match(r"^/scp-(\d+)-([a-z]{2})$", path)
    if not m:
        return path.lstrip("/").upper()
    return f"SCP-{m.group(1)}-{m.group(2).upper()}"


def build_entries(
    hub_paths: list[str],
    crawled_titles: dict[str, str],
    prev_titles: dict[str, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in hub_paths:
        i = path.lstrip("/")
        u = "https://scp-jp.wikidot.com" + path
        t = (
            crawled_titles.get(path)
            or prev_titles.get(i)
            or fallback_title_from_path(path)
        )
        out.append({"u": u, "i": i, "t": t})
    return out


def default_out_path() -> str:
    return os.path.join(default_jp_list_feed_dir(), "scp-int.json")


def default_merge_titles_path() -> str | None:
    p = os.path.join(default_jp_list_feed_dir(), "scp-int.json")
    return p if os.path.isfile(p) else None


def main() -> int:
    p = argparse.ArgumentParser(description="Generate ScpDocs SCPArticleListPayload JSON (scp-int.json).")
    p.add_argument("--out", default=None, help="Output path (default: <repo>/docs/list/jp/scp-int.json)")
    p.add_argument(
        "--hub-paths-from",
        default=None,
        metavar="PATH",
        help="scp_list.json path (default: <repo>/docs/scp_list.json)",
    )
    p.add_argument(
        "--merge-titles-from",
        default=None,
        metavar="PATH",
        help="Previous scp-int.json to reuse titles when crawl misses (default: docs/list/jp/scp-int.json if exists)",
    )
    p.add_argument(
        "--no-merge-titles",
        action="store_true",
        help="Ignore default scp-int.json merge (use only crawl + path fallback).",
    )
    p.add_argument(
        "--skip-intl-crawl",
        action="store_true",
        help="Do not crawl international list pages (titles only from merge-titles-from / fallback).",
    )
    p.add_argument(
        "--intl-delay-sec",
        type=float,
        default=INTL_CRAWL_DELAY_SEC,
        help=f"Delay between Wikidot requests during intl crawl (default: {INTL_CRAWL_DELAY_SEC}).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    out_path = args.out or default_out_path()
    hub_path = args.hub_paths_from or os.path.join(repo_root(), "docs", "scp_list.json")
    merge_titles: str | None
    if args.no_merge_titles:
        merge_titles = None
    elif args.merge_titles_from is not None:
        merge_titles = args.merge_titles_from
    else:
        merge_titles = default_merge_titles_path()

    try:
        hub_paths = load_hub_paths(hub_path)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    session = requests.Session()
    crawled: dict[str, str] = {}
    if not args.skip_intl_crawl:
        try:
            crawled = crawl_intl_title_map(
                session, verbose=args.verbose, delay_sec=float(args.intl_delay_sec)
            )
        except Exception as e:
            print(f"WARN: intl crawl failed, continuing with merge/fallback only: {e}", file=sys.stderr)

    prev = load_previous_titles(merge_titles)
    entries = build_entries(hub_paths, crawled, prev)

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

    n_hit = sum(1 for p in hub_paths if p in crawled)
    print(
        f"Wrote {out_path} ({len(entries)} entries); crawl title hits: {n_hit}/{len(hub_paths)}; "
        f"merge titles keys: {len(prev)}.",
        file=sys.stderr if args.verbose else sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
