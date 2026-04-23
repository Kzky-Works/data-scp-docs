#!/usr/bin/env python3
"""
ScpDocs 3 系統フィード用: `docs/list/jp/scp-jp.json`（`SCPArticleListPayload` 互換）を生成する。

スキーマ（iOS `SCPArticleListPayload` / `SCPArticle` と一致）:
  - listVersion, schemaVersion, generatedAt, entries
  - 各 entry: u（絶対 URL）, i（安定 ID）, t（一覧タイトル）, c?（オブジェクトクラス）,
    o?（本家メイン和訳一覧の行タイトル = 旧 scp_list の mainlistTranslationTitle）, g?（タグ）

データ源:
  - 日本支部オリジナル一覧: scp-series-jp … scp-series-jp-5
  - 本家メイン和訳一覧の行タイトル: scp-series … scp-series-5（`o` に格納）
  - objectClass / tags: `--merge-metadata-from docs/scp_list.json` または `--with-article-metadata`

`update_list.py` と同じ HTTP マナー（User-Agent・間隔・再試行）を用いる。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from _scp_app_feed_common import default_jp_list_feed_dir

REQUEST_DELAY_SEC = 2.5

HTTP_HEADERS = {
    "User-Agent": "ScpDocsAppFeedBot/1.0 (+https://github.com/Kzky-Works/data-scp-docs; contact: repo owner)",
    "Accept-Language": "ja,en;q=0.8",
}

SERIES_PAGES_JP: list[tuple[int, str]] = [
    (0, "https://scp-jp.wikidot.com/scp-series-jp"),
    (1, "https://scp-jp.wikidot.com/scp-series-jp-2"),
    (2, "https://scp-jp.wikidot.com/scp-series-jp-3"),
    (3, "https://scp-jp.wikidot.com/scp-series-jp-4"),
    (4, "https://scp-jp.wikidot.com/scp-series-jp-5"),
]

SERIES_PAGES_MAINLIST: list[tuple[int, str]] = [
    (0, "https://scp-jp.wikidot.com/scp-series"),
    (1, "https://scp-jp.wikidot.com/scp-series-2"),
    (2, "https://scp-jp.wikidot.com/scp-series-3"),
    (3, "https://scp-jp.wikidot.com/scp-series-4"),
    (4, "https://scp-jp.wikidot.com/scp-series-5"),
]

SCP_JP_HREF_RE = re.compile(r"^/scp-(\d+)-jp$")
SCP_MAINLIST_HREF_RE = re.compile(r"^/scp-(\d+)$")

OBJECT_CLASS_RE = re.compile(
    r"<strong>\s*(?:オブジェクトクラス|Object Class)\s*:\s*</strong>\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)
OBJECT_CLASS_RE_LOOSE = re.compile(
    r"(?:オブジェクトクラス|Object Class)\s*[:：]\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)

_TAG_SKIP_LOWER = {
    "scp",
    "jp",
    "euclid",
    "keter",
    "safe",
    "thaumiel",
    "explained",
    "neutralized",
    "apollyon",
    "デマーカー",
    "提案中",
    "アーカイブ",
}


@dataclass(frozen=True)
class SeriesRange:
    lo: int
    hi: int


def range_for_series(series: int) -> SeriesRange:
    ranges = {
        0: SeriesRange(1, 999),
        1: SeriesRange(1000, 1999),
        2: SeriesRange(2000, 2999),
        3: SeriesRange(3000, 3999),
        4: SeriesRange(4000, 4999),
    }
    return ranges[series]


def jp_article_path(scp_number: int) -> str:
    n = int(scp_number)
    return f"/scp-{n:03d}-jp" if n < 1000 else f"/scp-{n}-jp"


def jp_article_url(scp_number: int) -> str:
    return "https://scp-jp.wikidot.com" + jp_article_path(scp_number)


def jp_stable_id(scp_number: int) -> str:
    p = jp_article_path(scp_number)
    return p.lstrip("/")


def parse_scp_number_jp_href(href: str) -> int | None:
    m = SCP_JP_HREF_RE.match((href or "").strip())
    if not m:
        return None
    return int(m.group(1), 10)


def parse_scp_number_mainlist_href(href: str) -> int | None:
    raw = (href or "").strip()
    if not raw or raw.startswith("#"):
        return None
    path = urlparse(urljoin("https://scp-jp.wikidot.com/", raw)).path
    m = SCP_MAINLIST_HREF_RE.match(path)
    if not m:
        return None
    return int(m.group(1), 10)


def extract_title_from_li(li) -> str | None:
    full = li.get_text(separator="", strip=False).strip()
    if " - " not in full:
        return None
    _, title = full.split(" - ", 1)
    t = title.strip()
    return t if t else None


def fetch_html_with_retry(
    session: requests.Session,
    url: str,
    *,
    retries: int = 8,
    transient_status: tuple[int, ...] = (502, 503, 429),
) -> str:
    last_err: Exception | None = None
    for attempt in range(retries):
        time.sleep(REQUEST_DELAY_SEC)
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=90)
            if r.status_code in transient_status and attempt < retries - 1:
                wait = min(180, 12 * (2**attempt))
                time.sleep(wait)
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(120, 8 * (attempt + 1)))
    assert last_err is not None
    raise last_err


def fetch_series_jp_rows(series: int, list_url: str, session: requests.Session) -> list[dict[str, Any]]:
    html = fetch_html_with_retry(session, list_url)
    soup = BeautifulSoup(html, "html.parser")
    rng = range_for_series(series)
    out: list[dict[str, Any]] = []
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "") or ""
        n = parse_scp_number_jp_href(href)
        if n is None or not (rng.lo <= n <= rng.hi):
            continue
        title = extract_title_from_li(li)
        if not title:
            continue
        out.append({"series": series, "scpNumber": n, "title": title})
    return out


def fetch_mainlist_title_map(series: int, list_url: str, session: requests.Session) -> dict[int, str]:
    html = fetch_html_with_retry(session, list_url)
    soup = BeautifulSoup(html, "html.parser")
    rng = range_for_series(series)
    by_num: dict[int, str] = {}
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "") or ""
        n = parse_scp_number_mainlist_href(href)
        if n is None or not (rng.lo <= n <= rng.hi):
            continue
        title = extract_title_from_li(li)
        if not title:
            continue
        by_num[n] = title
    return by_num


def extract_object_class_from_html(html: str) -> str | None:
    m = OBJECT_CLASS_RE.search(html)
    if not m:
        m = OBJECT_CLASS_RE_LOOSE.search(html)
    if not m:
        return None
    oc = m.group(1).strip()
    return oc if oc else None


def extract_article_tags_from_html(html: str, object_class: str | None) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    div = soup.select_one("div.page-tags")
    if not div:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for a in div.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        m = re.search(r"/system:page-tags/tag/([^#]+)", href)
        if not m:
            continue
        raw = unquote(m.group(1)).strip()
        if not raw:
            continue
        low = raw.lower()
        if low in _TAG_SKIP_LOWER:
            continue
        if object_class and low == object_class.strip().lower():
            continue
        if re.match(r"^\d+jp$", low):
            continue
        if raw not in seen:
            seen.add(raw)
            out.append(raw)
    return out


def fetch_article_metadata(
    session: requests.Session,
    article_path: str,
    *,
    delay_sec: float,
    retries: int = 5,
    verbose: bool = False,
) -> tuple[str | None, list[str]]:
    url = urljoin("https://scp-jp.wikidot.com/", article_path)
    last_err: Exception | None = None
    for attempt in range(retries):
        time.sleep(delay_sec)
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=90)
            if r.status_code in (403, 429, 503) and attempt < retries - 1:
                wait = min(120, 15 * (2**attempt))
                if verbose:
                    print(
                        f"INFO: {article_path} HTTP {r.status_code}, sleep {wait}s retry",
                        file=sys.stderr,
                    )
                time.sleep(wait)
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            html = r.text
            oc = extract_object_class_from_html(html)
            tags = extract_article_tags_from_html(html, oc)
            if verbose:
                print(f"INFO: {article_path} ok c={oc!r} tags={len(tags)}", file=sys.stderr)
            return oc, tags
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(90, 8 * (attempt + 1)))
    assert last_err is not None
    raise last_err


def load_merge_metadata_from_scp_list(path: str) -> dict[tuple[int, int], dict[str, Any]]:
    """docs/scp_list.json の (series, scpNumber) → objectClass, tags, mainlistTranslationTitle."""
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"WARN: could not load {path}: {e}", file=sys.stderr)
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return {}
    out: dict[tuple[int, int], dict[str, Any]] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        try:
            s = int(e["series"])
            n = int(e["scpNumber"])
        except (KeyError, TypeError, ValueError):
            continue
        blob: dict[str, Any] = {}
        oc = e.get("objectClass")
        if isinstance(oc, str) and oc.strip():
            blob["objectClass"] = oc.strip()
        tg = e.get("tags")
        if isinstance(tg, list) and tg and all(isinstance(t, str) and str(t).strip() for t in tg):
            blob["tags"] = [str(t).strip() for t in tg]
        mt = e.get("mainlistTranslationTitle")
        if isinstance(mt, str) and mt.strip():
            blob["mainlistTranslationTitle"] = mt.strip()
        if blob:
            out[(s, n)] = blob
    return out


def build_entries(
    session: requests.Session,
    *,
    merge: dict[tuple[int, int], dict[str, Any]],
    with_article_metadata: bool,
    metadata_delay_sec: float,
    metadata_max_articles: int | None,
    verbose: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for series, url in SERIES_PAGES_JP:
        part = fetch_series_jp_rows(series, url, session)
        if not part:
            raise RuntimeError(f"No JP series rows for series={series} url={url}")
        rows.extend(part)
    rows.sort(key=lambda r: (int(r["series"]), int(r["scpNumber"])))

    mainlist_by_key: dict[tuple[int, int], str] = {}
    for series, url in SERIES_PAGES_MAINLIST:
        m = fetch_mainlist_title_map(series, url, session)
        for n, title in m.items():
            mainlist_by_key[(series, n)] = title

    out: list[dict[str, Any]] = []
    meta_done = 0
    for r in rows:
        series = int(r["series"])
        n = int(r["scpNumber"])
        key = (series, n)
        t = str(r["title"]).strip()
        u = jp_article_url(n)
        i = jp_stable_id(n)
        merged = merge.get(key, {})
        o_val = merged.get("mainlistTranslationTitle") or mainlist_by_key.get(key)
        oc = merged.get("objectClass")
        tags = merged.get("tags")

        if with_article_metadata and (metadata_max_articles is None or meta_done < metadata_max_articles):
            path = jp_article_path(n)
            try:
                oc2, tags2 = fetch_article_metadata(
                    session,
                    path,
                    delay_sec=metadata_delay_sec,
                    verbose=verbose,
                )
                oc = oc2 or oc
                tags = tags2 if tags2 else tags
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


def validate_app_feed_payload(payload: dict[str, Any]) -> None:
    if payload.get("schemaVersion") != 1:
        raise ValueError(f"schemaVersion must be 1, got {payload.get('schemaVersion')!r}")
    lv = payload.get("listVersion")
    if not isinstance(lv, int) or lv <= 0:
        raise ValueError(f"listVersion must be positive int, got {lv!r}")
    gen = payload.get("generatedAt")
    if not isinstance(gen, str) or not gen.strip():
        raise ValueError("generatedAt must be non-empty ISO8601 string")
    entries = payload.get("entries")
    if not isinstance(entries, list) or len(entries) == 0:
        raise ValueError("entries must be non-empty list")
    seen_i: set[str] = set()
    for idx, e in enumerate(entries):
        if not isinstance(e, dict):
            raise ValueError(f"entries[{idx}] is not an object")
        for req in ("u", "i", "t"):
            v = e.get(req)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"entries[{idx}].{req} invalid")
        if not str(e["u"]).lower().startswith("https://"):
            raise ValueError(f"entries[{idx}].u must be https URL")
        idi = e["i"].strip()
        if idi in seen_i:
            raise ValueError(f"duplicate i: {idi!r}")
        seen_i.add(idi)
        c = e.get("c")
        if c is not None and (not isinstance(c, str) or not c.strip()):
            raise ValueError(f"entries[{idx}].c must be non-empty string or omitted")
        o = e.get("o")
        if o is not None and (not isinstance(o, str) or not o.strip()):
            raise ValueError(f"entries[{idx}].o must be non-empty string or omitted")
        g = e.get("g")
        if g is not None:
            if not isinstance(g, list):
                raise ValueError(f"entries[{idx}].g must be list or omitted")
            for j, tag in enumerate(g):
                if not isinstance(tag, str) or not tag.strip():
                    raise ValueError(f"entries[{idx}].g[{j}] invalid")


def atomic_write_json(out_path: str, payload: dict[str, Any], *, verbose: bool) -> None:
    validate_app_feed_payload(payload)
    tmp = f"{out_path}.tmp.{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, out_path)
    except BaseException:
        if os.path.isfile(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise
    if verbose:
        print(
            f"Wrote {out_path} ({len(payload['entries'])} entries, listVersion={payload['listVersion']}).",
            file=sys.stderr,
        )


def default_out_path() -> str:
    return os.path.join(default_jp_list_feed_dir(), "scp-jp.json")


def main() -> int:
    p = argparse.ArgumentParser(description="Generate ScpDocs SCPArticleListPayload JSON (scp-jp.json).")
    p.add_argument("--out", default=None, help="Output path (default: <repo>/docs/list/jp/scp-jp.json)")
    p.add_argument(
        "--merge-metadata-from",
        default=None,
        metavar="PATH",
        help="Merge objectClass, tags, mainlistTranslationTitle from scp_list.json (e.g. docs/scp_list.json).",
    )
    p.add_argument(
        "--with-article-metadata",
        action="store_true",
        help="Fetch each -jp article page for objectClass/tags (slow; use delay).",
    )
    p.add_argument(
        "--metadata-delay-sec",
        type=float,
        default=None,
        help=f"Delay between article requests (default: {REQUEST_DELAY_SEC}).",
    )
    p.add_argument(
        "--metadata-max-articles",
        type=int,
        default=None,
        help="Stop after N article fetches (for testing).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    out_path = args.out or default_out_path()

    merge = load_merge_metadata_from_scp_list(args.merge_metadata_from or "")
    session = requests.Session()
    delay = args.metadata_delay_sec if args.metadata_delay_sec is not None else REQUEST_DELAY_SEC

    try:
        entries = build_entries(
            session,
            merge=merge,
            with_article_metadata=args.with_article_metadata,
            metadata_delay_sec=delay,
            metadata_max_articles=args.metadata_max_articles,
            verbose=args.verbose,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc)
    generated_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    list_version = int(now.timestamp())

    payload: dict[str, Any] = {
        "listVersion": list_version,
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "entries": entries,
    }

    try:
        atomic_write_json(out_path, payload, verbose=args.verbose)
    except Exception as e:
        print(f"ERROR: validate/write failed: {e}", file=sys.stderr)
        return 1

    print(
        f"Wrote {out_path} ({len(entries)} entries); merge keys: {len(merge)}. "
        "Use --with-article-metadata to refresh c/g from Wikidot.",
        file=sys.stderr if args.verbose else sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
