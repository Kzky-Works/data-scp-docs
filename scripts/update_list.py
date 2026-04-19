#!/usr/bin/env python3
"""
SCP-JP シリーズ一覧（Wikidot）からタイトルを取得し、アプリの SCPListRemotePayload 互換 JSON を生成する。

要件: ScpDocs の SCPListRemotePayload / SCPListRemoteEntry（series: 0…4, scpNumber: Int）に一致。
各エントリは `scp-series-jp` 由来の `title` に加え、`scp-series` 一覧から `mainlistTranslationTitle`（本家メインリスト和訳の行タイトル）を付与する。
`hubLinkedPaths` は scp-international から辿る国際支部和訳（/scp-数字-2文字、-jp 以外）。
Phase 14: 各エントリに任意フィールド `objectClass`（文字列）・`tags`（文字列配列）を付けられる。
個別記事からの取得は `--with-article-metadata`（負荷が高いため遅延秒数に注意）。
"""

from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

# 取得間隔（秒）。短時間の連続アクセスを避ける。
REQUEST_DELAY_SEC = 2.5
# 国際ハブ配下の一覧ページ取得にも同間隔を使う。
REQUEST_DELAY_HUB_SEC = 2.5
# scp-international から辿る一覧ページの最大取得数（CI 時間・相手サーバ負荷の上限）。
MAX_INTL_LIST_PAGES = 150

# User-Agent（ブロック回避・識別用）
HTTP_HEADERS = {
    "User-Agent": "ScpDocsListBot/1.0 (+https://github.com/scp-docs; contact: repo owner)",
    "Accept-Language": "ja,en;q=0.8",
}

# SCPJPSeries.rawValue → Wikidot 一覧 URL（日本支部オリジナル `scp-NNN-jp`）
SERIES_PAGES: list[tuple[int, str]] = [
    (0, "https://scp-jp.wikidot.com/scp-series-jp"),
    (1, "https://scp-jp.wikidot.com/scp-series-jp-2"),
    (2, "https://scp-jp.wikidot.com/scp-series-jp-3"),
    (3, "https://scp-jp.wikidot.com/scp-series-jp-4"),
    (4, "https://scp-jp.wikidot.com/scp-series-jp-5"),
]

# 本家メインリスト和訳（`scp-NNN`）。JSON の `mainlistTranslationTitle` 用。
SERIES_PAGES_MAINLIST: list[tuple[int, str]] = [
    (0, "https://scp-jp.wikidot.com/scp-series"),
    (1, "https://scp-jp.wikidot.com/scp-series-2"),
    (2, "https://scp-jp.wikidot.com/scp-series-3"),
    (3, "https://scp-jp.wikidot.com/scp-series-4"),
    (4, "https://scp-jp.wikidot.com/scp-series-5"),
]

INTERNATIONAL_HUB_URL = "https://scp-jp.wikidot.com/scp-international"

SCP_HREF_RE = re.compile(r"^/scp-(\d+)-jp$")
# 本家メインリスト和訳一覧（`/scp-173` 形式。`-jp` や国際支部 `-xx` は除外）
SCP_MAINLIST_HREF_RE = re.compile(r"^/scp-(\d+)$")
OBJECT_CLASS_RE = re.compile(
    r"<strong>\s*(?:オブジェクトクラス|Object Class)\s*:\s*</strong>\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)
OBJECT_CLASS_RE_LOOSE = re.compile(
    r"(?:オブジェクトクラス|Object Class)\s*[:：]\s*([A-Za-z][A-Za-z\-]*)",
    re.IGNORECASE,
)

# page-tags に付くメタ的タグ（本番 UI の「語」タグとしては除外）
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
# 国際支部の和訳記事（2 文字コード）。`-jp` はメインシリーズ側で扱うため除外。
INTL_SCP_ARTICLE_PATH_RE = re.compile(r"^/scp-\d+-[a-z]{2}$")

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


@dataclass(frozen=True)
class SeriesRange:
    lo: int
    hi: int


def range_for_series(series: int) -> SeriesRange:
    # SCPJPSeries.scpNumberRange と同一
    ranges = {
        0: SeriesRange(1, 999),
        1: SeriesRange(1000, 1999),
        2: SeriesRange(2000, 2999),
        3: SeriesRange(3000, 3999),
        4: SeriesRange(4000, 4999),
    }
    return ranges[series]


def is_english_main_series_list(path: str) -> bool:
    """英語本部の SCP シリーズ一覧（/scp-series, /scp-series-2 …）。"""
    pl = path.lower()
    if pl == "/scp-series":
        return True
    return bool(re.match(r"^/scp-series-\d+$", pl))


def looks_like_intl_branch_list_page(path: str) -> bool:
    """scp-international から辿る「国際支部の SCP 和訳一覧」候補。"""
    pl = path.lower()
    if pl in {"/", "/scp-international"}:
        return False
    if "scp-series-jp" in pl:
        return False
    if is_english_main_series_list(path):
        return False
    return any(s in pl for s in INTL_LIST_SUBSTRINGS)


def parse_scp_number_from_href(href: str) -> int | None:
    m = SCP_HREF_RE.match(href.strip())
    if not m:
        return None
    return int(m.group(1), 10)


def parse_scp_number_from_mainlist_href(href: str) -> int | None:
    raw = (href or "").strip()
    if not raw or raw.startswith("#"):
        return None
    path = urlparse(urljoin("https://scp-jp.wikidot.com/", raw)).path
    m = SCP_MAINLIST_HREF_RE.match(path)
    if not m:
        return None
    return int(m.group(1), 10)


def extract_title_from_li(li) -> str | None:
    # strip=True は子ノード単位で空白を削るため、「</a> - タイトル」の先頭スペースが落ちて
    # 「SCP-xxx-JP- タイトル」になり区切りが壊れる。全体を結合してから strip する。
    full = li.get_text(separator="", strip=False).strip()
    if " - " not in full:
        return None
    _, title = full.split(" - ", 1)
    t = title.strip()
    return t if t else None


def fetch_html_with_retry(session: requests.Session, url: str, retries: int = 4) -> str:
    last_err: Exception | None = None
    for attempt in range(retries):
        time.sleep(REQUEST_DELAY_HUB_SEC)
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=90)
            if r.status_code == 503 and attempt < retries - 1:
                time.sleep(6 * (attempt + 1))
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
    assert last_err is not None
    raise last_err


def extract_intl_article_paths_from_html(html: str) -> set[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: set[str] = set()
    base = "https://scp-jp.wikidot.com/"
    for a in soup.find_all("a", href=True):
        raw = (a.get("href") or "").strip()
        if not raw or raw.startswith("#"):
            continue
        path = urlparse(urljoin(base, raw)).path
        m = INTL_SCP_ARTICLE_PATH_RE.match(path)
        if not m:
            continue
        if path.endswith("-jp"):
            continue
        out.add(path)
    return out


def fetch_international_hub_article_paths(session: requests.Session) -> list[str]:
    text = fetch_html_with_retry(session, INTERNATIONAL_HUB_URL)
    soup = BeautifulSoup(text, "html.parser")
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
    articles: set[str] = set()
    for i, u in enumerate(list_urls):
        if i >= MAX_INTL_LIST_PAGES:
            break
        try:
            html = fetch_html_with_retry(session, u)
        except Exception as e:
            print(f"WARN: skip intl list {u}: {e}", file=sys.stderr)
            continue
        articles.update(extract_intl_article_paths_from_html(html))
    return sorted(articles)


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
    session: requests.Session, article_path: str, *, delay_sec: float
) -> tuple[str | None, list[str]]:
    """個別記事からオブジェクトクラスとタグを取得。"""
    url = urljoin("https://scp-jp.wikidot.com/", article_path)
    time.sleep(delay_sec)
    r = session.get(url, headers=HTTP_HEADERS, timeout=90)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"
    html = r.text
    oc = extract_object_class_from_html(html)
    tags = extract_article_tags_from_html(html, oc)
    return oc, tags


def fetch_series_entries(series: int, url: str, session: requests.Session) -> list[dict[str, Any]]:
    time.sleep(REQUEST_DELAY_SEC)
    r = session.get(url, headers=HTTP_HEADERS, timeout=60)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"

    soup = BeautifulSoup(r.text, "html.parser")
    rng = range_for_series(series)
    out: list[dict[str, Any]] = []

    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "") or ""
        n = parse_scp_number_from_href(href)
        if n is None:
            continue
        if not (rng.lo <= n <= rng.hi):
            continue
        title = extract_title_from_li(li)
        if not title:
            continue
        out.append({"series": series, "scpNumber": n, "title": title})

    return out


def fetch_mainlist_translation_title_map(series: int, url: str, session: requests.Session) -> dict[int, str]:
    """`scp-series` 系ページから `/scp-数字` リンクの一覧行タイトルを取得。"""
    time.sleep(REQUEST_DELAY_SEC)
    r = session.get(url, headers=HTTP_HEADERS, timeout=60)
    r.raise_for_status()
    r.encoding = r.encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    rng = range_for_series(series)
    by_num: dict[int, str] = {}

    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "") or ""
        n = parse_scp_number_from_mainlist_href(href)
        if n is None:
            continue
        if not (rng.lo <= n <= rng.hi):
            continue
        title = extract_title_from_li(li)
        if not title:
            continue
        by_num[n] = title

    return by_num


def merge_mainlist_translation_titles(
    entries: list[dict[str, Any]], session: requests.Session
) -> None:
    """各エントリに `mainlistTranslationTitle` を付与（`scp-series` 一覧由来）。"""
    for series, url in SERIES_PAGES_MAINLIST:
        title_map = fetch_mainlist_translation_title_map(series, url, session)
        for e in entries:
            if int(e["series"]) != series:
                continue
            n = int(e["scpNumber"])
            if n in title_map:
                e["mainlistTranslationTitle"] = title_map[n]


def validate_payload(payload: dict[str, Any]) -> None:
    schema = payload.get("schemaVersion")
    if schema != 1:
        raise ValueError(f"schemaVersion must be 1, got {schema!r}")

    lv = payload.get("listVersion")
    if not isinstance(lv, int) or lv <= 0:
        raise ValueError(f"listVersion must be positive int, got {lv!r}")

    gen = payload.get("generatedAt")
    if not isinstance(gen, str) or not gen:
        raise ValueError("generatedAt must be non-empty string")

    entries = payload.get("entries")
    if not isinstance(entries, list) or len(entries) == 0:
        raise ValueError("entries must be non-empty list")

    hub = payload.get("hubLinkedPaths", [])
    if not isinstance(hub, list):
        raise ValueError("hubLinkedPaths must be a list")
    for i, p in enumerate(hub):
        if not isinstance(p, str) or not p.startswith("/scp-"):
            raise ValueError(f"hubLinkedPaths[{i}] must be a path string starting with /scp-")

    seen: set[tuple[int, int]] = set()
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            raise ValueError(f"entries[{i}] is not an object")
        s = e.get("series")
        n = e.get("scpNumber")
        t = e.get("title")
        if not isinstance(s, int) or not (0 <= s <= 4):
            raise ValueError(f"entries[{i}].series invalid: {s!r}")
        if not isinstance(n, int):
            raise ValueError(f"entries[{i}].scpNumber must be int, got {n!r}")
        if not isinstance(t, str) or not t.strip():
            raise ValueError(f"entries[{i}].title invalid")
        rng = range_for_series(s)
        if not (rng.lo <= n <= rng.hi):
            raise ValueError(f"entries[{i}] scpNumber {n} out of range for series {s}")
        key = (s, n)
        if key in seen:
            raise ValueError(f"duplicate entry series={s} scpNumber={n}")
        seen.add(key)

        mlt = e.get("mainlistTranslationTitle")
        if mlt is not None and (not isinstance(mlt, str) or not mlt.strip()):
            raise ValueError(f"entries[{i}].mainlistTranslationTitle must be non-empty string or omitted, got {mlt!r}")
        oc = e.get("objectClass")
        if oc is not None and not isinstance(oc, str):
            raise ValueError(f"entries[{i}].objectClass must be string or omitted, got {oc!r}")
        tg = e.get("tags")
        if tg is not None:
            if not isinstance(tg, list):
                raise ValueError(f"entries[{i}].tags must be a list")
            for j, tag in enumerate(tg):
                if not isinstance(tag, str) or not tag.strip():
                    raise ValueError(f"entries[{i}].tags[{j}] invalid")


def scrape_all(
    *,
    with_article_metadata: bool = False,
    metadata_delay_sec: float | None = None,
    metadata_max_articles: int | None = None,
) -> dict[str, Any]:
    session = requests.Session()
    all_entries: list[dict[str, Any]] = []

    for series, url in SERIES_PAGES:
        rows = fetch_series_entries(series, url, session)
        if len(rows) == 0:
            raise RuntimeError(f"No entries parsed for series={series} url={url}")
        all_entries.extend(rows)

    all_entries.sort(key=lambda e: (e["series"], e["scpNumber"]))

    merge_mainlist_translation_titles(all_entries, session)

    if with_article_metadata:
        delay = metadata_delay_sec if metadata_delay_sec is not None else REQUEST_DELAY_SEC
        n_done = 0
        for e in all_entries:
            if metadata_max_articles is not None and n_done >= metadata_max_articles:
                break
            n = int(e["scpNumber"])
            slug = f"/scp-{n:03d}-jp" if n < 1000 else f"/scp-{n}-jp"
            try:
                oc, tags = fetch_article_metadata(session, slug, delay_sec=delay)
            except Exception as ex:
                print(f"WARN: metadata {slug}: {ex}", file=sys.stderr)
                continue
            if oc:
                e["objectClass"] = oc
            if tags:
                e["tags"] = tags
            n_done += 1
            if n_done % 50 == 0:
                print(f"… metadata {n_done} articles", file=sys.stderr)

    hub_paths = fetch_international_hub_article_paths(session)

    now = datetime.now(timezone.utc)
    generated_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    list_version = int(now.timestamp())

    payload: dict[str, Any] = {
        "listVersion": list_version,
        "schemaVersion": 1,
        "generatedAt": generated_at,
        "entries": all_entries,
        "hubLinkedPaths": hub_paths,
    }
    validate_payload(payload)
    return payload


def main() -> int:
    import argparse

    p = argparse.ArgumentParser(description="Generate SCPListRemotePayload JSON (scp_list.json).")
    p.add_argument(
        "--out",
        default="scp_list.json",
        help="Output path (default: scp_list.json)",
    )
    p.add_argument(
        "--with-article-metadata",
        action="store_true",
        help=(
            "Fetch each -JP article page to fill objectClass and tags (slow; "
            f"uses ~{REQUEST_DELAY_SEC}s delay between requests by default)."
        ),
    )
    p.add_argument(
        "--metadata-delay-sec",
        type=float,
        default=None,
        help="Override delay between article metadata requests (seconds).",
    )
    p.add_argument(
        "--metadata-max-articles",
        type=int,
        default=None,
        help="Stop after this many article fetches (for testing).",
    )
    args = p.parse_args()
    out_path = args.out
    try:
        data = scrape_all(
            with_article_metadata=args.with_article_metadata,
            metadata_delay_sec=args.metadata_delay_sec,
            metadata_max_articles=args.metadata_max_articles,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    hub_n = len(data.get("hubLinkedPaths", []))
    print(f"Wrote {out_path} ({len(data['entries'])} entries, {hub_n} hubLinkedPaths).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
