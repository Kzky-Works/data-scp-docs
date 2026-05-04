#!/usr/bin/env python3
"""
JP 支部向けハイブリッド索引: シリーズ一覧（基礎層）＋ page-tags（属性層）＋
foundation-tales-jp（著者層）を統合し list/jp/*.json を生成する。

マニフェスト（schemaVersion 2）: `manifest_scp-*.json` / `manifest_tales.json` /
`manifest_gois.json` / `manifest_canons.json` / `manifest_jokes.json` に entries（u,i,t）と
スパース metadata（主キー i；jokes は報告書と同様に `c` を page-tags から付与し得る）を出力する。`manifest_tales.json` の entries には各 Tale 本文の `#page-info` 由来の **`lu`（最終更新 unix 秒）** を付与する。
**`metadata[].r`** は `foundation-tales-jp` 由来が **`jp`**、`foundation-tales`（scp-jp 上の本家翻訳ハブ）由来が **`en`**、両ハブに同一 `i` が載る場合は **`jp+en`**（アプリの JP/EN ピッカーはこれで判定）。listVersion は前回出力と差分が無い場合は据え置き。
GoI（`manifest_gois.json`）のみ schemaVersion 3: `goi-formats-jp` の h1/h2 構造に基づき `en` / `jp` / `other` の団体行（団体名 + ハブ `u` のみ。ハブのない団体は除外）を `goiRegions` に格納。フラット `entries` / `metadata` は空。詳細は同梱 `docs/GOI_MANIFEST_V3_ja.md`。

Canon（`manifest_canons.json`）: `canon-hub-jp`（`div.canon-title` 内）、`series-hub-jp`（`div.series-title` 内）、`canon-hub`（`div.canon-block` 内 h1/h2 リンク）からハブを収集し、
`canonRegions.jp` / `canonRegions.seriesJp` / `canonRegions.en` に分離。各行に `tag-list` のシリーズタグ（`ct`）、**索引ページ**上の要約（`ds`）: カノン JP は各 `div.canon-title` 直後の `div.canon-description`、連作 JP は `div.series-title` 直後の `div.series-description`、EN 索引は `div.canon-block` 内・見出し直後の `p`。個別ハブ本文は `div.canon-description` / `div.series-description`、無ければ `#page-content` 先頭の `blockquote` をフォールバック。
`#page-info` の最終更新 unix（`lu`）を付与。フラット `entries` + `metadata[].r`（`jp` / `series_jp` / `en`）も併記。

収集は Wikidot のみ（scp_list.json には依存しない）。

Git: `--git-commit` で `output-dir` 直下の `*.json` をステージし、差分があればコミット。
`--git-push` はコミットを含め（変更がなければスキップ）、`git push <remote>` を実行。
認証・upstream は利用者環境に依存（CI なら token、ローカルなら SSH 等）。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup

from wikidot_utils import wikidot_tag_list_total_pages

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MANIFEST_SCHEMA_VERSION = 2
GOI_MANIFEST_SCHEMA_VERSION = 3

HARVEST_STATE_FILENAME = "_harvest_state.json"

# `--mode` の許容値。詳細は `main()` の引数ヘルプ参照。
_HARVEST_MODE_CHOICES: tuple[str, ...] = ("daily", "weekly", "full")

HTTP_HEADERS = {
    "User-Agent": "ScpDocsHarvester/1.0 (+https://github.com/Kzky-Works/data-scp-docs)",
    "Accept-Language": "ja,en;q=0.8",
}

REQUEST_DELAY_SEC = 0.45

SERIES_JP: list[tuple[int, str]] = [
    (0, "/scp-series-jp"),
    (1, "/scp-series-jp-2"),
    (2, "/scp-series-jp-3"),
    (3, "/scp-series-jp-4"),
    (4, "/scp-series-jp-5"),
]
SERIES_MAIN: list[tuple[int, str]] = [
    (0, "/scp-series"),
    (1, "/scp-series-2"),
    (2, "/scp-series-3"),
    (3, "/scp-series-4"),
    (4, "/scp-series-5"),
    (5, "/scp-series-6"),
    (6, "/scp-series-7"),
    (7, "/scp-series-8"),
    (8, "/scp-series-9"),
    (9, "/scp-series-10"),
]

SCP_JP_HREF = re.compile(r"^/scp-(\d+)-jp$")
SCP_MAIN_HREF = re.compile(r"^/scp-(\d+)$")

# 属性層: オブジェクトクラス語（page-tags タグ名 = URL 末尾）
OBJECT_CLASS_TAGS = (
    "safe",
    "euclid",
    "keter",
    "thaumiel",
    "neutralized",
    "explained",
    "apollyon",
    "esoteric-class",
)

INTERNATIONAL_HUB = "/scp-international"
INTL_LIST_HINTS = (
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
MAX_INTL_LIST_PAGES = 96

# 国際版ハブから <a> が辿れない主一覧（例: PL / ES / ZH 系）を補完する。
INTL_SEED_PATHS: tuple[str, ...] = (
    "/lista-pl",
    "/serie-scp-es",
    "/scp-series-cn",
    "/scp-series-zh",
)


def is_intl_scp_article_path(pth: str) -> bool:
    """
    日本支部の国際訳文として一覧に載る /scp-… 記事路径。

    - 番号先: /scp-100-ru, /scp-100-zh-tr（Wikidot 既定）
    - 言語先: /scp-cn-601, /scp-pl-302, /scp-zh-001 等（中・波・日支部の慣習差）
    """
    p = (pth or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    p = p.rstrip("/")
    pl = p.lower()
    if pl.endswith("-jp") or pl.endswith("/jp"):
        return False
    if re.match(r"^/scp-\d+(-[a-z]{2})+$", p, re.I):
        return True
    m = re.match(r"^/scp-([a-z]{2,3})-(\d+)(-[a-z0-9]+)*$", p, re.I)
    if m:
        lang = m.group(1).lower()
        # 誤マッチ（例: /scp-scp-1）と本家日本オリジナル置き換えを除外
        if lang in ("jp", "scp"):
            return False
        return True
    return False

# カノンハブ索引: JP canon は div.canon-title + div.canon-description、連作 JP は
# div.series-title + div.series-description、EN (canon-hub) は div.canon-block 内 h1/h2 直後の p。
CANON_HUB_PAGES: tuple[tuple[str, str], ...] = (
    ("/canon-hub-jp", "jp"),
    ("/series-hub-jp", "series_jp"),
    ("/canon-hub", "en"),
)
EN_CANON_HUB_PATH: str = "/canon-hub"
SERIES_HUB_JP_PATH: str = "/series-hub-jp"
TAG_LIST_PATH = "/tag-list"
JOKE_INDEX_PATHS: tuple[str, ...] = ("/joke-scps", "/joke-scps-jp")
GOI_FORMATS_HUB = "/goi-formats-jp"
FOUNDATION_TALES_EN = "/foundation-tales"

PAGE_LINK_EXCLUDE_PREFIXES: tuple[str, ...] = (
    "/system:",
    "/nav:",
    "/search:",
    "/admin:",
    "/login",
    "/register",
    "/_:",
    "/local--",
    "/forum",
    "/blog:",
    "/activity",
)

JOKE_HUB_PATHS_LOWER = frozenset({"/joke-scps", "/joke-scps-jp"})


@dataclass
class BranchConfig:
    """支部（言語）単位の設定。他支部は別インスタンスを組み立てる。"""

    code: str = "jp"
    site_host: str = "https://scp-jp.wikidot.com"
    output_dir: str = field(default_factory=lambda: os.path.join(REPO_ROOT, "list", "jp"))
    foundation_tales_path: str = "/foundation-tales-jp"
    goi_tag: str = "goi-format"
    goi_tag_max_pages: int = 8

    def abs_url(self, path: str) -> str:
        p = path if path.startswith("/") else "/" + path
        return self.site_host.rstrip("/") + p


@dataclass
class ArticleRow:
    path: str  # /scp-173-jp
    u: str
    i: str
    t: str
    c: str | None = None
    o: str | None = None
    g: list[str] = field(default_factory=list)
    a: str | None = None  # 報告書では通常使わない（互換のためキーは出力時省略）


def sleep_delay() -> None:
    time.sleep(REQUEST_DELAY_SEC)


def fetch_html(session: requests.Session, url: str, *, retries: int = 6) -> str:
    last: Exception | None = None
    for attempt in range(retries):
        sleep_delay()
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=(15, 75))
            if r.status_code in (429, 502, 503) and attempt < retries - 1:
                time.sleep(min(90, 6 * (2**attempt)))
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except Exception as e:
            last = e
            if attempt < retries - 1:
                time.sleep(min(60, 5 * (attempt + 1)))
    assert last is not None
    raise last


def extract_title_from_li(li) -> str | None:
    full = li.get_text(separator="", strip=False).strip()
    if " - " not in full:
        return None
    _, title = full.split(" - ", 1)
    t = title.strip()
    return t if t else None


@dataclass(frozen=True)
class SeriesRange:
    lo: int
    hi: int


def range_for_series(series: int) -> SeriesRange:
    """シリーズⅠのみ 001–999。その他は Wikidot 慣例どおり各 1000 件ずつ。"""
    m = {
        0: SeriesRange(1, 999),
        1: SeriesRange(1000, 1999),
        2: SeriesRange(2000, 2999),
        3: SeriesRange(3000, 3999),
        4: SeriesRange(4000, 4999),
        5: SeriesRange(5000, 5999),
        6: SeriesRange(6000, 6999),
        7: SeriesRange(7000, 7999),
        8: SeriesRange(8000, 8999),
        9: SeriesRange(9000, 9999),
    }
    return m[series]


def scrape_series_jp(session: requests.Session, cfg: BranchConfig) -> dict[str, ArticleRow]:
    base = cfg.site_host.rstrip("/")
    out: dict[str, ArticleRow] = {}
    for series, path in SERIES_JP:
        url = base + path
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")
        rng = range_for_series(series)
        for li in soup.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            href = (a.get("href") or "").strip()
            pu = urlparse(urljoin(base + "/", href))
            m = SCP_JP_HREF.match(pu.path)
            if not m:
                continue
            n = int(m.group(1), 10)
            if not (rng.lo <= n <= rng.hi):
                continue
            title = extract_title_from_li(li)
            if not title:
                continue
            path_norm = pu.path
            i = path_norm.lstrip("/").lower()
            u = base + path_norm
            out[path_norm] = ArticleRow(path=path_norm, u=u, i=i, t=title)
    return out


def scrape_series_main(session: requests.Session, cfg: BranchConfig) -> dict[str, ArticleRow]:
    base = cfg.site_host.rstrip("/")
    out: dict[str, ArticleRow] = {}
    for series, path in SERIES_MAIN:
        url = base + path
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")
        rng = range_for_series(series)
        for li in soup.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            href = (a.get("href") or "").strip()
            pu = urlparse(urljoin(base + "/", href))
            m = SCP_MAIN_HREF.match(pu.path)
            if not m:
                continue
            n = int(m.group(1), 10)
            if not (rng.lo <= n <= rng.hi):
                continue
            title = extract_title_from_li(li)
            if not title:
                continue
            path_norm = pu.path
            i = path_norm.lstrip("/").lower()
            u = base + path_norm
            out[path_norm] = ArticleRow(path=path_norm, u=u, i=i, t=title)
    return out


def validate_manifest_entries_metadata(
    entries: list[dict[str, Any]], metadata: dict[str, Any], label: str
) -> None:
    """metadata のキーは必ず entries[].i に存在する（孤児禁止）。"""
    ids: set[str] = set()
    for e in entries:
        if not isinstance(e, dict):
            continue
        i = e.get("i")
        if isinstance(i, str) and i.strip():
            ids.add(i.strip())
    for k in metadata:
        if k not in ids:
            raise ValueError(f"manifest {label}: metadata key {k!r} has no matching entry.i")


def light_article_row_dict(row: ArticleRow) -> dict[str, Any]:
    return {"u": row.u, "i": row.i, "t": row.t}


def sparse_trifold_metadata_chunk(row: ArticleRow) -> dict[str, Any] | None:
    """entries 以外に載せる c / o / g のみ（空なら None）。o は t と異なる場合だけ。"""
    chunk: dict[str, Any] = {}
    if row.c and str(row.c).strip():
        chunk["c"] = str(row.c).strip()
    if row.o and str(row.o).strip():
        ost = str(row.o).strip()
        if ost != (row.t or "").strip():
            chunk["o"] = ost
    if row.g:
        chunk["g"] = [str(x).strip() for x in row.g if isinstance(x, str) and str(x).strip()]
    return chunk if chunk else None


def attach_jp_mainlist_title_from_main_series(
    jp_rows: dict[str, ArticleRow], main_rows: dict[str, ArticleRow]
) -> None:
    """支部行の o に本家 /scp-n 一覧タイトルを載せる（一覧 t と異なるときのみ）。"""
    for path, row in jp_rows.items():
        m = SCP_JP_HREF.match(path)
        if not m:
            continue
        n = int(m.group(1), 10)
        main_row = main_rows.get(f"/scp-{n}")
        if main_row is None:
            continue
        mt = (main_row.t or "").strip()
        jt = (row.t or "").strip()
        if mt and mt != jt:
            row.o = mt


def trifold_rows_to_manifest_parts(
    rows: dict[str, ArticleRow],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    entries_out: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}
    for _, row in sorted(rows.items()):
        entries_out.append(light_article_row_dict(row))
        chunk = sparse_trifold_metadata_chunk(row)
        if chunk:
            metadata[row.i] = chunk
    return entries_out, metadata


OC_TAG_TO_DISPLAY = {
    "safe": "Safe",
    "euclid": "Euclid",
    "keter": "Keter",
    "thaumiel": "Thaumiel",
    "neutralized": "Neutralized",
    "explained": "Explained",
    "apollyon": "Apollyon",
    "esoteric-class": "Esoteric",
}

# タグ一覧は件数で複数ページ化される。1 ページ目のみだと後方ページの記事に `c` が付かない（例: JP Keter の SCP-003-JP）。
MAX_OBJECT_CLASS_TAG_LIST_PAGES = 256


def crawl_object_class_tag_pages(
    session: requests.Session,
    cfg: BranchConfig,
    target_paths: set[str] | None = None,
) -> dict[str, str]:
    """属性層: system:page-tags/tag/<class> に掲載された記事パス → OC。

    target_paths を渡すと対象だけを採用し、全対象が解決した時点で巡回を止める。
    """
    base = cfg.site_host.rstrip("/")
    path_to_class: dict[str, str] = {}
    base_netloc = urlparse(base).netloc
    remaining = set(target_paths or [])

    def ingest_tag_page_html(html: str, oc_display: str) -> None:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            raw = (a.get("href") or "").strip()
            pu = urlparse(urljoin(base + "/", raw))
            if pu.netloc and pu.netloc != base_netloc:
                continue
            pth = pu.path or ""
            if not pth or pth.startswith("/system:"):
                continue
            if target_paths is not None and pth not in target_paths:
                continue
            if pth not in path_to_class:
                path_to_class[pth] = oc_display
                remaining.discard(pth)

    for tag in OBJECT_CLASS_TAGS:
        if target_paths is not None and not remaining:
            break
        first_url = f"{base}/system:page-tags/tag/{tag}"
        try:
            first_html = fetch_html(session, first_url, retries=4)
        except Exception as ex:
            print(f"WARN: OC tag page {tag}: {ex}", file=sys.stderr)
            continue
        total = wikidot_tag_list_total_pages(first_html)
        if total > MAX_OBJECT_CLASS_TAG_LIST_PAGES:
            print(
                f"WARN: OC tag {tag}: pager reports {total} pages; capping at {MAX_OBJECT_CLASS_TAG_LIST_PAGES}",
                file=sys.stderr,
            )
            total = MAX_OBJECT_CLASS_TAG_LIST_PAGES
        oc_display = OC_TAG_TO_DISPLAY.get(tag, tag.replace("-", " ").title())
        for page in range(1, total + 1):
            if page == 1:
                html = first_html
            else:
                page_url = f"{base}/system:page-tags/tag/{tag}/p/{page}"
                try:
                    html = fetch_html(session, page_url, retries=4)
                except Exception as ex:
                    print(f"WARN: OC tag page {tag} p/{page}: {ex}", file=sys.stderr)
                    break
            ingest_tag_page_html(html, oc_display)
            if target_paths is not None and not remaining:
                break
    return path_to_class


def filter_object_class_map(path_to_class: dict[str, str], paths: set[str]) -> dict[str, str]:
    """事前に1回だけクロールした OC マップを対象パス集合へ絞る。"""
    return {p: c for p, c in path_to_class.items() if p in paths}


def load_jp_tag_articles(output_dir: str) -> dict[str, list[str]]:
    """既存 `jp_tag.json` から slug -> raw tags を読む。無ければ空。"""
    path = os.path.join(output_dir, "jp_tag.json")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"WARN: jp_tag.json not found; manifest g enrichment skipped: {path}", file=sys.stderr)
        return {}
    except Exception as ex:
        print(f"WARN: failed to load jp_tag.json; manifest g enrichment skipped: {ex}", file=sys.stderr)
        return {}
    articles = data.get("articles")
    if not isinstance(articles, dict):
        print("WARN: jp_tag.json has no articles object; manifest g enrichment skipped", file=sys.stderr)
        return {}
    out: dict[str, list[str]] = {}
    for k, v in articles.items():
        if not isinstance(k, str) or not isinstance(v, list):
            continue
        tags = [str(x).strip() for x in v if isinstance(x, str) and str(x).strip()]
        if tags:
            out[k.strip().lower()] = tags
    return out


def object_class_from_tags(tags: list[str]) -> str | None:
    """`jp_tag.json` のタグ配列から現行優先順で OC 表示名を推定する。"""
    tag_set = {str(t).strip().lower() for t in tags if isinstance(t, str) and str(t).strip()}
    for tag in OBJECT_CLASS_TAGS:
        if tag in tag_set:
            return OC_TAG_TO_DISPLAY.get(tag, tag.replace("-", " ").title())
    return None


def object_class_map_from_jp_tag_articles(
    paths: set[str], tag_articles: dict[str, list[str]]
) -> tuple[dict[str, str], int]:
    """対象パスに対し、`jp_tag.json` から OC を復元する。戻り値2つ目はタグ行が存在した件数。"""
    out: dict[str, str] = {}
    tagged = 0
    for path in paths:
        key = path.lstrip("/").lower()
        tags = tag_articles.get(key)
        if tags is None:
            continue
        tagged += 1
        c = object_class_from_tags(tags)
        if c:
            out[path] = c
    return out, tagged


def paths_missing_from_jp_tag(paths: set[str], tag_articles: dict[str, list[str]]) -> set[str]:
    """`jp_tag.json` にタグ行が無い対象パスだけを返す。"""
    return {p for p in paths if p.lstrip("/").lower() not in tag_articles}


def apply_object_classes_to_article_rows(rows: dict[str, ArticleRow], path_to_class: dict[str, str]) -> None:
    for p, row in rows.items():
        if not row.c and p in path_to_class:
            row.c = path_to_class[p]


def load_existing_manifest_object_classes(path: str) -> dict[str, str]:
    """既存 manifest の metadata.c をゼロリクエスト fallback として読む。"""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as ex:
        # Corrupted manifest: log loudly so CI captures it instead of silently dropping the OC fallback.
        print(f"WARN: load_existing_manifest_object_classes: failed to read {path}: {ex}", file=sys.stderr)
        return {}
    md = data.get("metadata")
    if not isinstance(md, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in md.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        c = v.get("c")
        if isinstance(c, str) and c.strip():
            out[k.strip().lower()] = c.strip()
    return out


def object_class_map_from_existing_manifest(
    paths: set[str], existing_by_slug: dict[str, str]
) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in paths:
        c = existing_by_slug.get(path.lstrip("/").lower())
        if c:
            out[path] = c
    return out


def attach_tags_from_jp_tag_map(rows: dict[str, ArticleRow], tag_articles: dict[str, list[str]]) -> None:
    """ArticleRow.g に `jp_tag.json` の raw tags を付与する（既存順・class marker を保持）。"""
    for row in rows.values():
        tags = tag_articles.get(row.i.lower())
        if tags:
            row.g = list(tags)


def enrich_metadata_tags_from_jp_tag_map(
    entries: list[dict[str, Any]],
    metadata: dict[str, Any],
    tag_articles: dict[str, list[str]],
) -> None:
    """light entries + metadata に `metadata[i].g` をスパース付与する。"""
    for ent in entries:
        if not isinstance(ent, dict):
            continue
        ik = ent.get("i")
        if not isinstance(ik, str) or not ik.strip():
            continue
        tags = tag_articles.get(ik.strip().lower())
        if not tags:
            continue
        chunk = metadata.setdefault(ik.strip(), {})
        if isinstance(chunk, dict):
            chunk["g"] = list(tags)


def is_english_main_series_list(path: str) -> bool:
    pl = path.lower()
    if pl == "/scp-series":
        return True
    return bool(re.match(r"^/scp-series-\d+$", pl))


def looks_intl_list(path: str) -> bool:
    pl = path.lower()
    if pl in {"/", "/scp-international"}:
        return False
    if "scp-series-jp" in pl:
        return False
    if is_english_main_series_list(path):
        return False
    return any(s in pl for s in INTL_LIST_HINTS)


def discover_intl_list_urls(session: requests.Session, cfg: BranchConfig) -> list[str]:
    base = cfg.site_host.rstrip("/")
    hub = base + INTERNATIONAL_HUB
    html = fetch_html(session, hub)
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        raw = (a.get("href") or "").strip()
        absu = urljoin(hub, raw)
        pu = urlparse(absu)
        if pu.netloc != urlparse(base).netloc:
            continue
        path = pu.path or "/"
        if not looks_intl_list(path):
            continue
        u = f"{urlparse(base).scheme}://{pu.netloc}{path}"
        if u not in seen:
            seen.add(u)
            out.append(u)
    out.sort()
    # ハブにリンクが無い国別一覧（テキストのみ等）向け
    for rel in INTL_SEED_PATHS:
        u = f"{base}{rel}"
        if u not in seen:
            seen.add(u)
            out.append(u)
    out.sort()
    return out


def extract_intl_titles(html: str, base_host: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    titles: dict[str, str] = {}
    base = base_host.rstrip("/")
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        raw = (a.get("href") or "").strip()
        pu = urlparse(urljoin(base + "/", raw))
        pth = pu.path
        if not is_intl_scp_article_path(pth):
            continue
        t = extract_title_from_li(li) or a.get_text(separator=" ", strip=True)
        if not t:
            continue
        prev = titles.get(pth)
        if prev is None or len(t) > len(prev):
            titles[pth] = t
    return titles


def crawl_intl_titles(session: requests.Session, cfg: BranchConfig) -> dict[str, str]:
    urls = discover_intl_list_urls(session, cfg)[:MAX_INTL_LIST_PAGES]
    merged: dict[str, str] = {}
    for u in urls:
        try:
            html = fetch_html(session, u, retries=4)
        except Exception as ex:
            print(f"WARN: intl list {u}: {ex}", file=sys.stderr)
            continue
        part = extract_intl_titles(html, cfg.site_host)
        for p, t in part.items():
            prev = merged.get(p)
            if prev is None or len(t) > len(prev):
                merged[p] = t
    return merged


def build_int_rows_from_wikidot(session: requests.Session, cfg: BranchConfig) -> list[dict[str, Any]]:
    """国際訳文: /scp-international から辿る各一覧 + INTL_SEED_PATHS（ハブ未リンクの主一覧）。"""
    titles = crawl_intl_titles(session, cfg)
    base = cfg.site_host.rstrip("/")
    rows: list[dict[str, Any]] = []
    if not titles:
        print("WARN: no intl paths discovered; scp-int manifest may be empty", file=sys.stderr)
    for p in sorted(titles.keys()):
        i = p.lstrip("/").lower()
        u = base + p
        raw_t = titles.get(p, "")
        t = raw_t.strip() if isinstance(raw_t, str) and raw_t.strip() else _fallback_int_title(p)
        rows.append({"u": u, "i": i, "t": t})
    return rows


def _fallback_int_title(path: str) -> str:
    p = path.rstrip("/")
    m = re.match(r"^/scp-(\d+)-([a-z]{2}(?:-[a-z]{2})*)$", p, re.I)
    if m:
        return f"SCP-{m.group(1)}-{m.group(2).upper()}"
    m = re.match(r"^/scp-([a-z]{2,3})-(\d+)", p, re.I)
    if m:
        return f"SCP-{m.group(1).upper()}-{m.group(2)}"
    return p.lstrip("/").upper()


def next_list_version_and_generated_at(
    path: str, entries: list[dict[str, Any]], metadata: dict[str, Any]
) -> tuple[int, str]:
    """entries + metadata（正規化後）が前回と同一なら listVersion を据え置き、変化時のみ +1。"""
    dt = datetime.now(timezone.utc)
    gen = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    md_norm = {k: v for k, v in metadata.items() if isinstance(v, dict) and v}
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                old = json.load(f)
        except (OSError, ValueError) as ex:
            # Corrupted prior manifest: emit a warning and treat as empty so the diff path still bumps listVersion.
            print(f"WARN: next_list_version_and_generated_at: failed to read {path}: {ex}", file=sys.stderr)
            old = {}
        old_lv = int(old.get("listVersion") or 0)
        if old.get("entries") == entries and (old.get("metadata") or {}) == md_norm:
            return old_lv, gen
        return old_lv + 1, gen
    return int(dt.timestamp()), gen


def next_canon_list_version_and_generated_at(
    path: str,
    entries: list[dict[str, Any]],
    metadata: dict[str, Any],
    canon_regions: dict[str, list[dict[str, Any]]],
) -> tuple[int, str]:
    """entries + metadata + canonRegions が前回と同一なら listVersion 据え置き。"""
    dt = datetime.now(timezone.utc)
    gen = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    md_norm = {k: v for k, v in metadata.items() if isinstance(v, dict) and v}
    cr_norm = {
        "jp": canon_regions.get("jp") or [],
        "en": canon_regions.get("en") or [],
        "seriesJp": canon_regions.get("series_jp") or [],
    }
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                old = json.load(f)
        except (OSError, ValueError) as ex:
            print(f"WARN: next_canon_list_version_and_generated_at: failed to read {path}: {ex}", file=sys.stderr)
            old = {}
        old_lv = int(old.get("listVersion") or 0)
        old_cr = old.get("canonRegions") or {}
        if (
            old.get("entries") == entries
            and (old.get("metadata") or {}) == md_norm
            and (old_cr.get("jp") or []) == cr_norm["jp"]
            and (old_cr.get("en") or []) == cr_norm["en"]
            and (old_cr.get("seriesJp") or []) == cr_norm["seriesJp"]
        ):
            return old_lv, gen
        return old_lv + 1, gen
    return int(dt.timestamp()), gen


def write_manifest(path: str, entries: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    """schemaVersion 2: entries（軽）+ metadata（スパース、キーは i）。"""
    md = {k: v for k, v in metadata.items() if isinstance(v, dict) and v}
    validate_manifest_entries_metadata(entries, md, os.path.basename(path))
    lv, gen = next_list_version_and_generated_at(path, entries, metadata)
    payload: dict[str, Any] = {
        "listVersion": lv,
        "schemaVersion": MANIFEST_SCHEMA_VERSION,
        "generatedAt": gen,
        "entries": entries,
        "metadata": md,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def write_canon_manifest(
    path: str,
    entries: list[dict[str, Any]],
    metadata: dict[str, Any],
    canon_regions: dict[str, list[dict[str, Any]]],
) -> None:
    """manifest_canons.json: entries + metadata（r=jp|series_jp|en）+ canonRegions。"""
    md = {k: v for k, v in metadata.items() if isinstance(v, dict) and v}
    validate_manifest_entries_metadata(entries, md, os.path.basename(path))
    cr_norm: dict[str, list[dict[str, Any]]] = {
        "jp": canon_regions.get("jp") or [],
        "en": canon_regions.get("en") or [],
        "seriesJp": canon_regions.get("series_jp") or [],
    }
    lv, gen = next_canon_list_version_and_generated_at(
        path, entries, metadata, canon_regions
    )
    payload: dict[str, Any] = {
        "listVersion": lv,
        "schemaVersion": MANIFEST_SCHEMA_VERSION,
        "generatedAt": gen,
        "entries": entries,
        "metadata": md,
        "canonRegions": cr_norm,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _page_link_excluded(path: str) -> bool:
    pl = path.lower()
    if not pl.startswith("/"):
        return True
    for pfx in PAGE_LINK_EXCLUDE_PREFIXES:
        if pl.startswith(pfx):
            return True
    return False


def page_content_link_map_from_root(root, page_url: str, base: str) -> dict[str, str]:
    """#page-content 相当 root 内の同一サイト単一スラッグへのリンク → {正規パス: 表示テキスト}。"""
    out: dict[str, str] = {}
    if root is None:
        return out
    base_netloc = urlparse(base).netloc
    for a in root.find_all("a", href=True):
        raw = (a.get("href") or "").strip().split("#")[0]
        if not raw or raw.startswith("javascript:"):
            continue
        absu = urljoin(page_url, raw)
        pu = urlparse(absu)
        if pu.netloc and pu.netloc != base_netloc:
            continue
        p = pu.path or "/"
        if p == "/" or _page_link_excluded(p):
            continue
        segs = [x for x in p.strip("/").split("/") if x]
        if len(segs) != 1:
            continue
        slug = segs[0]
        if ":" in slug:
            continue
        title = a.get_text(" ", strip=True) or slug
        prev = out.get(p)
        if prev is None or len(title) > len(prev):
            out[p] = title
    return out


def extract_page_content_link_map(
    session: requests.Session, cfg: BranchConfig, page_path: str
) -> dict[str, str]:
    """#page-content 内の同一サイト単一スラッグへのリンク → {正規パス: 表示テキスト}。"""
    base = cfg.site_host.rstrip("/")
    url = cfg.abs_url(page_path)
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#page-content") or soup.body
    return page_content_link_map_from_root(root, url, base)


def is_joke_article_path(path: str) -> bool:
    pl = path.lower()
    if pl in JOKE_HUB_PATHS_LOWER:
        return False
    # 日本支部オリジナル（-jp-j）を本家系（-j）より先に判定
    if re.match(r"^/scp-.+-jp-j$", pl):
        return True
    if re.match(r"^/scp-.+-j$", pl):
        return True
    if pl.startswith("/joke-scp"):
        if re.match(r"^/joke-scp[\w-]*$", pl) and pl not in JOKE_HUB_PATHS_LOWER:
            return True
    return False


def _fallback_joke_title(path: str) -> str:
    """一覧からタイトルが取れないときの表示用（インテル系 `_fallback_int_title` に相当）。"""
    p = (path or "").rstrip("/").lstrip("/").lower()
    if not p:
        return ""
    if p.startswith("scp-"):
        return "SCP-" + p[4:].upper()
    return p.upper()


def _merge_joke_article_row(
    merged: dict[str, ArticleRow], pth: str, title: str, base: str
) -> None:
    """同一パスはより長いタイトルを採用（ハブ間・li/リンク突合のマージに使用）。"""
    t = (title or "").strip()
    if not t:
        t = _fallback_joke_title(pth)
    i = pth.lstrip("/").lower()
    u = base + pth
    prev = merged.get(pth)
    if prev is None or len(t) > len(prev.t):
        merged[pth] = ArticleRow(path=pth, u=u, i=i, t=t)


def scrape_joke_article_rows(session: requests.Session, cfg: BranchConfig) -> dict[str, ArticleRow]:
    """
    joke-scps / joke-scps-jp: 一覧は `<li>` で `SCP-… - 作品名` と出る箇子が多い。
    リンク本文だけ取る `extract_page_content_link_map` だと t が欠けるため、
    scp-series / intl 同様 `extract_title_from_li` を優先し、無ければアンカー本文を使う。
    同一取得 HTML 内で、li で取れなかったジョークパスをリンクマップで補完する。
    """
    base = cfg.site_host.rstrip("/")
    merged: dict[str, ArticleRow] = {}
    for hub in JOKE_INDEX_PATHS:
        url = cfg.abs_url(hub)
        html = fetch_html(session, url)
        soup = BeautifulSoup(html, "html.parser")
        root = soup.select_one("#page-content") or soup.body
        if root is None:
            continue
        from_li: set[str] = set()
        for li in root.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            href = (a.get("href") or "").strip()
            pu = urlparse(urljoin(base + "/", href))
            pth = pu.path or "/"
            if not is_joke_article_path(pth):
                continue
            t = extract_title_from_li(li) or a.get_text(" ", strip=True) or ""
            _merge_joke_article_row(merged, pth, t, base)
            from_li.add(pth)
        # リンク群だけに載るジョークを補完。`li` では `作品名` が ` - ` 以降に出るが
        # アンカー本文の方が文字数が長いため、上書き禁止（SCP-067-J 等）。
        link_map = page_content_link_map_from_root(root, url, base)
        for pth, t in link_map.items():
            if not is_joke_article_path(pth) or pth in from_li:
                continue
            _merge_joke_article_row(merged, pth, t, base)
    return merged


def _canon_valid_hub_path(
    href: str, page_url: str, site_netloc: str
) -> str | None:
    """canon ハブ 1 リンクの href から、同一サイトの単一スラッグパス（/foo）を取る。該当しなければ None。"""
    raw = (href or "").strip().split("#")[0]
    if not raw or raw.startswith("javascript:"):
        return None
    absu = urljoin(page_url, raw)
    pu = urlparse(absu)
    if pu.netloc and pu.netloc.lower() != site_netloc:
        return None
    p = pu.path or "/"
    if p == "/" or _page_link_excluded(p):
        return None
    segs = [x for x in p.strip("/").split("/") if x]
    if len(segs) != 1:
        return None
    if ":" in segs[0]:
        return None
    return p


def parse_canon_title_hubs_from_root(
    root, page_url: str, site_netloc: str
) -> list[tuple[str, str]]:
    """#page-content 相当 root 内の div.canon-title から、カノンハブへの単一スラッグリンクを文書順で列挙。"""
    seen_order: list[str] = []
    best_title: dict[str, str] = {}
    for block in root.select("div.canon-title"):
        for a in block.find_all("a", href=True):
            p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
            if p is None:
                continue
            segs = [x for x in p.strip("/").split("/") if x]
            slug = segs[0] if segs else ""
            title = a.get_text(" ", strip=True) or slug
            if p not in best_title:
                seen_order.append(p)
                best_title[p] = title
            elif len(title) > len(best_title[p]):
                best_title[p] = title
    return [(p, best_title[p]) for p in seen_order]


def _plain_text_from_canon_description_div(div) -> str:
    """div.canon-description 内の <p> を空白正規化して連結（記事直取得・索引共通）。"""
    if div is None:
        return ""
    parts = [p.get_text(" ", strip=True) for p in div.find_all("p")]
    desc = " ".join(x for x in parts if x)
    return re.sub(r"\s+", " ", desc).strip()


def parse_canon_hub_description_map_from_root(
    root, page_url: str, site_netloc: str
) -> dict[str, str]:
    """
    カノンハブ索引1ページ上の各 div.canon-title 直後の div.canon-description から
    { hub スラッグ小文字: 要約 } を作る。個別記事ページに canon-description が無い場合の正本。
    """
    out: dict[str, str] = {}
    for block in root.select("div.canon-title"):
        paths: list[str] = []
        seen: set[str] = set()
        for a in block.find_all("a", href=True):
            p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
            if p is None or p in seen:
                continue
            seen.add(p)
            paths.append(p)
        if not paths:
            continue
        desc_el = block.find_next_sibling()
        while desc_el is not None:
            if desc_el.name == "div":
                cl = desc_el.get("class") or []
                if "canon-description" in cl:
                    break
            desc_el = desc_el.find_next_sibling()
        else:
            desc_el = None
        desc = _plain_text_from_canon_description_div(desc_el)
        if not desc:
            continue
        for p in paths:
            ikey = p.lstrip("/").lower()
            out[ikey] = desc
    return out


def parse_series_title_hubs_from_root(
    root, page_url: str, site_netloc: str
) -> list[tuple[str, str]]:
    """`series-hub-jp` 内の div.series-title から連作ハブへの単一スラッグリンクを文書順で列挙。"""
    seen_order: list[str] = []
    best_title: dict[str, str] = {}
    for block in root.select("div.series-title"):
        for a in block.find_all("a", href=True):
            p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
            if p is None:
                continue
            segs = [x for x in p.strip("/").split("/") if x]
            slug = segs[0] if segs else ""
            title = a.get_text(" ", strip=True) or slug
            if p not in best_title:
                seen_order.append(p)
                best_title[p] = title
            elif len(title) > len(best_title[p]):
                best_title[p] = title
    return [(p, best_title[p]) for p in seen_order]


def parse_series_hub_description_map_from_root(
    root, page_url: str, site_netloc: str
) -> dict[str, str]:
    """
    `series-hub-jp` 上の各 div.series-title 直後の div.series-description から
    { hub スラッグ小文字: 要約 } を作る。
    """
    out: dict[str, str] = {}
    for block in root.select("div.series-title"):
        paths: list[str] = []
        seen: set[str] = set()
        for a in block.find_all("a", href=True):
            p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
            if p is None or p in seen:
                continue
            seen.add(p)
            paths.append(p)
        if not paths:
            continue
        desc_el = block.find_next_sibling()
        while desc_el is not None:
            if desc_el.name == "div":
                cl = desc_el.get("class") or []
                if "series-description" in cl:
                    break
            desc_el = desc_el.find_next_sibling()
        else:
            desc_el = None
        desc = _plain_text_from_canon_description_div(desc_el)
        if not desc:
            continue
        for p in paths:
            ikey = p.lstrip("/").lower()
            out[ikey] = desc
    return out


def parse_canon_en_hub_blocks_hubs_and_descriptions(
    root, page_url: str, site_netloc: str
) -> tuple[list[tuple[str, str]], dict[str, str]]:
    """
    カノンハブ（EN 索引: canon-hub）: div.canon-wrapper 内の各 div.canon-block から
    ハブ link（h1/h2/h3 内）と、見出し直後の要約 p（.snippet より前）を取る。

    本ページには div.canon-title / div.canon-description が無い。
    """
    blocks = root.select("div.canon-wrapper > div.canon-block")
    if not blocks:
        blocks = root.select("div.canon-block")
    seen_order: list[str] = []
    best_title: dict[str, str] = {}
    desc_map: dict[str, str] = {}
    for block in blocks:
        heading = None
        for tag in block.find_all(["h1", "h2", "h3"], recursive=False):
            for a in tag.find_all("a", href=True):
                p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
                if p is not None:
                    heading = tag
                    break
            if heading is not None:
                break
        if heading is None:
            continue
        path: str | None = None
        title = ""
        for a in heading.find_all("a", href=True):
            p = _canon_valid_hub_path(a.get("href") or "", page_url, site_netloc)
            if p is not None:
                path = p
                title = a.get_text(" ", strip=True) or p.lstrip("/")
                break
        if not path or not title:
            continue
        ikey = path.lstrip("/").lower()
        n = heading.find_next_sibling()
        desc = ""
        while n is not None:
            cl = n.get("class") or []
            if n.name == "div" and "snippet" in cl:
                break
            if n.name == "p":
                desc = re.sub(
                    r"\s+",
                    " ",
                    n.get_text(" ", strip=True),
                ).strip()
                break
            n = n.find_next_sibling()
        if path not in best_title:
            seen_order.append(path)
            best_title[path] = title
        elif len(title) > len(best_title[path]):
            best_title[path] = title
        if desc and ikey:
            desc_map[ikey] = desc
    return [(p, best_title[p]) for p in seen_order], desc_map


def extract_canon_title_hubs(
    session: requests.Session, cfg: BranchConfig, page_path: str
) -> list[tuple[str, str]]:
    """カノン索引1ページ内の各ハブ: JP は div.canon-title、EN 索引は div.canon-block。"""
    base = cfg.site_host.rstrip("/")
    page_url = cfg.abs_url(page_path)
    html = fetch_html(session, page_url)
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#page-content") or soup.body
    if root is None:
        return []
    site_netloc = urlparse(base).netloc.lower()
    if page_path == EN_CANON_HUB_PATH or page_path.rstrip("/") == EN_CANON_HUB_PATH:
        hubs, _ = parse_canon_en_hub_blocks_hubs_and_descriptions(
            root, page_url, site_netloc
        )
        return hubs
    return parse_canon_title_hubs_from_root(root, page_url, site_netloc)


def _ul_immediately_after_h3(h3) -> Any | None:
    """h3 の直後（任意の1つの p をスキップ）の ul。"""
    n = h3.find_next_sibling()
    if n is not None and n.name == "p":
        n = n.find_next_sibling()
    if n is not None and n.name == "ul":
        return n
    return None


def _hub_slugs_from_li(li) -> list[str]:
    """単一スラッグの内部リンク（タグページ以外）を文書順で列挙。"""
    out: list[str] = []
    for a in li.find_all("a", href=True):
        raw = (a.get("href") or "").strip().split("#")[0]
        if not raw or raw.startswith("javascript:"):
            continue
        if "/system:page-tags/tag/" in raw or "system:page-tags" in raw:
            continue
        pu = urlparse(urljoin("https://x/", raw))
        path = (pu.path or "").strip("/")
        if not path:
            continue
        parts = [x for x in path.split("/") if x]
        if len(parts) != 1:
            continue
        slug = parts[0]
        if ":" in slug:
            continue
        out.append(slug.lower())
    return out


def _parse_canon_tag_list_li(
    li, *, prefer_em_slug: bool
) -> tuple[str, str] | None:
    """
    tag-list の1行から (hub_slug_lower, tag_label)。
    カノン-EN は em (slug) を優先、カノン-JP は page-tags リンクの末尾を優先。
    """
    hubs = _hub_slugs_from_li(li)
    if not hubs:
        return None
    hub_slug = hubs[-1]
    tag_label: str | None = None
    if prefer_em_slug:
        em = li.find("em")
        if em is not None:
            txt = " ".join(em.get_text(" ", strip=True).split())
            m = re.match(r"^\(([^)]+)\)\s*$", txt)
            if m:
                tag_label = m.group(1).strip()
    if not tag_label:
        for a in li.find_all("a", href=True):
            raw = (a.get("href") or "").strip()
            if "/system:page-tags/tag/" not in raw:
                continue
            part = raw.split("/system:page-tags/tag/", 1)[-1]
            part = part.split("#")[0].split("?")[0]
            if part:
                tag_label = unquote(part)
                break
    if not tag_label:
        tag_label = hub_slug
    return hub_slug, tag_label


def scrape_canon_tag_hub_mapping(
    session: requests.Session, cfg: BranchConfig
) -> dict[str, str]:
    """tag-list の カノン-EN / カノン-JP / 連作-JP から {hub_slug_lower: series_tag_label}。"""
    url = cfg.abs_url(TAG_LIST_PATH)
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#page-content") or soup.body
    out: dict[str, str] = {}
    if root is None:
        return out
    for h3 in root.find_all("h3"):
        title = h3.get_text(strip=True)
        if title == "カノン-EN":
            ul = _ul_immediately_after_h3(h3)
            if ul is not None:
                for li in ul.find_all("li", recursive=False):
                    pair = _parse_canon_tag_list_li(li, prefer_em_slug=True)
                    if pair:
                        slug, tag = pair
                        out[slug] = tag
        elif title == "カノン-JP":
            ul = _ul_immediately_after_h3(h3)
            if ul is not None:
                for li in ul.find_all("li", recursive=False):
                    pair = _parse_canon_tag_list_li(li, prefer_em_slug=False)
                    if pair:
                        slug, tag = pair
                        out[slug] = tag
        elif title == "連作-JP":
            ul = _ul_immediately_after_h3(h3)
            if ul is not None:
                for li in ul.find_all("li", recursive=False):
                    pair = _parse_canon_tag_list_li(li, prefer_em_slug=False)
                    if pair:
                        slug, tag = pair
                        out[slug] = tag
    return out


def extract_page_info_unix_from_soup(soup: BeautifulSoup) -> int | None:
    """Wikidot `#page-info` 内 `span.odate.time_<unix>` から最終更新 unix（秒）。"""
    info = soup.select_one("#page-info")
    if info is None:
        return None
    span = info.find("span", class_="odate")
    if span is None:
        return None
    classes = span.get("class") or []
    for cl in classes:
        m = re.match(r"time_(\d+)$", str(cl))
        if m:
            return int(m.group(1))
    return None


def extract_page_info_unix(html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    return extract_page_info_unix_from_soup(soup)


def extract_canon_hub_card_fields(html: str) -> tuple[str, int | None, str | None]:
    """
    カノン／連作ハブ1ページから (ds プレーン, 最終更新 unix, 冒頭段落 desc)。
    ds: div.canon-description → div.series-description → #page-content 先頭 blockquote の順で要約。
    desc: #page-content 最初の <p>（heading 検索なし）。
    #page-info span.odate.time_* を参照。
    """
    soup = BeautifulSoup(html, "html.parser")
    div = soup.select_one("div.canon-description.unmargined")
    if div is None:
        div = soup.select_one("div.canon-description")
    desc = _plain_text_from_canon_description_div(div)
    if not desc:
        sdiv = soup.select_one("div.series-description.unmargined")
        if sdiv is None:
            sdiv = soup.select_one("div.series-description")
        desc = _plain_text_from_canon_description_div(sdiv)
    if not desc:
        root = soup.select_one("#page-content") or soup.body
        if root is not None:
            bq = root.find("blockquote")
            if bq is not None:
                desc = re.sub(r"\s+", " ", bq.get_text(" ", strip=True)).strip()
    ts = extract_page_info_unix_from_soup(soup)
    opening_desc = extract_opening_excerpt(soup)
    return desc, ts, opening_desc


def _log_canon_enrich_sample(
    region_lines: dict[str, list[dict[str, Any]]], *, limit: int = 3
) -> None:
    """stderr に ct/ds/lu の付与状況を短く出す（出力ファイルの取り違え防止用）。"""
    n = 0
    for region in ("jp", "en", "series_jp"):
        for line in region_lines.get(region) or []:
            if n >= limit:
                return
            ik = (line.get("i") or "")[:48]
            ct = line.get("ct")
            ds = line.get("ds") or ""
            lu = line.get("lu")
            print(
                f"INFO: canon enrich sample [{region}] i={ik!r} "
                f"ct={ct!r} ds_len={len(ds)} lu={lu!r}",
                file=sys.stderr,
            )
            n += 1


def enrich_canon_hub_lines_in_place(
    session: requests.Session,
    region_lines: dict[str, list[dict[str, Any]]],
    tag_map: dict[str, str],
    hub_desc_by_region: dict[str, dict[str, str]] | None = None,
) -> None:
    """各ハブ行に ct / ds / lu を付与。ds は索引ページ（hub_desc_by_region）を優先し、無い場合のみ同一 URL の本文を取得（同一 URL は1回だけ）。"""
    url_cache: dict[str, tuple[str, int | None, str | None]] = {}
    hub_desc_by_region = hub_desc_by_region or {}
    for region, lines in region_lines.items():
        region_ds = hub_desc_by_region.get(region) or {}
        for line in lines:
            ik = (line.get("i") or "").strip().lower()
            if ik:
                ct = tag_map.get(ik)
                if ct:
                    line["ct"] = ct
            u = (line.get("u") or "").strip()
            if not u:
                line["ds"] = ""
                continue
            ds_from_hub = region_ds.get(ik, "") if ik else ""
            if u not in url_cache:
                try:
                    html = fetch_html(session, u)
                    url_cache[u] = extract_canon_hub_card_fields(html)
                except HTTPError as e:
                    st = e.response.status_code if e.response is not None else 0
                    if st == 404:
                        print(
                            f"WARN: canon hub page 404 (index link stale?): {u}",
                            file=sys.stderr,
                        )
                        url_cache[u] = ("", None, None)
                    else:
                        raise
            ds_page, lu, opening_desc = url_cache[u]
            ds = ds_from_hub or ds_page
            line["ds"] = ds if ds else ""
            if lu is not None:
                line["lu"] = lu
            if opening_desc:
                line["desc"] = opening_desc


def scrape_canon_manifest_payload(
    session: requests.Session, cfg: BranchConfig
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """canon-hub-jp / series-hub-jp / canon-hub からハブ行とフラット entries + metadata.r。"""
    print("INFO: canons — tag-list (series tags)", file=sys.stderr)
    tag_map = scrape_canon_tag_hub_mapping(session, cfg)
    base = cfg.site_host.rstrip("/")
    site_netloc = urlparse(base).netloc.lower()
    region_lines: dict[str, list[dict[str, Any]]] = {
        "jp": [],
        "en": [],
        "series_jp": [],
    }
    hub_desc_by_region: dict[str, dict[str, str]] = {
        "jp": {},
        "en": {},
        "series_jp": {},
    }
    for hub_path, region in CANON_HUB_PAGES:
        page_url = cfg.abs_url(hub_path)
        html = fetch_html(session, page_url)
        soup = BeautifulSoup(html, "html.parser")
        root = soup.select_one("#page-content") or soup.body
        if root is None:
            hubs = []
        elif hub_path.rstrip("/") == EN_CANON_HUB_PATH.rstrip("/"):
            hubs, hub_desc_by_region[region] = (
                parse_canon_en_hub_blocks_hubs_and_descriptions(
                    root, page_url, site_netloc
                )
            )
        elif hub_path.rstrip("/") == SERIES_HUB_JP_PATH.rstrip("/"):
            hubs = parse_series_title_hubs_from_root(root, page_url, site_netloc)
            hub_desc_by_region[region] = parse_series_hub_description_map_from_root(
                root, page_url, site_netloc
            )
        else:
            hubs = parse_canon_title_hubs_from_root(root, page_url, site_netloc)
            hub_desc_by_region[region] = parse_canon_hub_description_map_from_root(
                root, page_url, site_netloc
            )
        for path, title in hubs:
            ikey = path.lstrip("/").lower()
            region_lines[region].append(
                {"u": base + path, "i": ikey, "t": title}
            )
    print("INFO: canons — hub-index ds + per-URL last-updated (article ds fallback)", file=sys.stderr)
    enrich_canon_hub_lines_in_place(
        session, region_lines, tag_map, hub_desc_by_region
    )
    _log_canon_enrich_sample(region_lines)
    by_i: dict[str, tuple[dict[str, Any], str]] = {}
    for region in ("jp", "en", "series_jp"):
        for line in region_lines[region]:
            ik = line.get("i")
            if not isinstance(ik, str) or not ik.strip():
                continue
            iks = ik.strip()
            if iks not in by_i:
                by_i[iks] = (line, region)
    light: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}
    for iks in sorted(by_i.keys()):
        line, region = by_i[iks]
        t_raw = line.get("t")
        t_str = (
            t_raw.strip()
            if isinstance(t_raw, str) and t_raw.strip()
            else iks
        )
        light.append({"u": line["u"], "i": iks, "t": t_str})
        metadata[iks] = {"r": region}
    return light, metadata, region_lines


def _goi_h1_region_label(h1_text: str) -> str | None:
    """要注意団体ブロック用 h1 から en / jp / other、該当しなければ None。"""
    t = " ".join(h1_text.split())
    if "要注意団体-JP" in t:
        return "jp"
    if "要注意団体-EN" in t:
        return "en"
    if t.startswith("要注意団体-") and "インフォメーション" not in t:
        return "other"
    return None


def _goi_find_anchor_name_before_h2(h2) -> str | None:
    prev = h2.find_previous_sibling()
    while prev is not None:
        if prev.name == "p":
            an = prev.find("a", attrs={"name": True})
            if an is not None:
                name = (an.get("name") or "").strip()
                if name:
                    return name
        prev = prev.find_previous_sibling()
    return None


def _goi_abs_path_for_href(
    href: str, page_url: str, site_host_base: str
) -> str | None:
    raw = (href or "").strip().split("#")[0]
    if not raw or raw.startswith("javascript:"):
        return None
    absu = urljoin(page_url, raw)
    pu = urlparse(absu)
    site_netloc = urlparse(site_host_base).netloc
    if pu.netloc and pu.netloc.lower() != site_netloc.lower():
        return None
    p = (pu.path or "/").rstrip("/") or "/"
    if p == "/" or _page_link_excluded(p):
        return None
    segs = [x for x in p.strip("/").split("/") if x]
    if len(segs) != 1:
        return None
    if ":" in segs[0]:
        return None
    return p if p.startswith("/") else "/" + p


def _goi_parse_h2_group(h2, page_url: str, site_host: str) -> dict[str, Any] | None:
    span = h2.find("span")
    if span is None:
        return None
    a = span.find("a", href=True)
    hub_path: str | None = None
    if a is not None:
        raw_h = (a.get("href") or "").strip()
        hub_path = _goi_abs_path_for_href(raw_h, page_url, site_host.rstrip("/"))
        name = a.get_text(" ", strip=True) or (hub_path or "").lstrip("/")
    else:
        name = span.get_text(" ", strip=True)
        if not name:
            return None
    if hub_path is None:
        return None
    anchor = _goi_find_anchor_name_before_h2(h2) or (
        re.sub(r"[^0-9a-zA-Z]+", "-", name).strip("-").lower() or "goi-group"
    )
    return {
        "i": anchor[:120],
        "t": name,
        "u": site_host.rstrip("/") + hub_path,
    }


def enrich_goi_regions_desc_in_place(
    session: requests.Session,
    goi_regions: dict[str, list[dict[str, Any]]],
) -> None:
    """goiRegions 各エントリのハブページ冒頭段落を `desc` として付与する。"""
    fetched = 0
    for region_list in goi_regions.values():
        for entry in region_list:
            u = (entry.get("u") or "").strip()
            if not u:
                continue
            try:
                html = fetch_html(session, u)
                soup = BeautifulSoup(html, "html.parser")
                desc = extract_opening_excerpt(soup)
                if desc:
                    entry["desc"] = desc
                fetched += 1
            except Exception as ex:
                print(f"WARN: GoI desc fetch failed: {u} ({ex})", file=sys.stderr)
    print(f"INFO: GoI desc enrichment — fetched={fetched}", file=sys.stderr)


def scrape_goi_formats_hub_structured(
    session: requests.Session, cfg: BranchConfig
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, Any]]:
    """goi-formats-jp を DOM 構造で解釈し、en/jp/other の団体行（各 u は団体ハブ）を返す。flat entries / metadata は空。"""
    base = cfg.site_host.rstrip("/")
    page_url = cfg.abs_url(GOI_FORMATS_HUB)
    html = fetch_html(session, page_url)
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#page-content .content-panel") or soup.select_one("#page-content")
    regions: dict[str, list[dict[str, Any]]] = {"en": [], "jp": [], "other": []}
    if root is None:
        return regions, [], {}

    current: str | None = None
    for h in root.find_all(["h1", "h2"]):
        if h.name == "h1":
            r = _goi_h1_region_label(h.get_text(" ", strip=True))
            if r is not None:
                current = r
            continue
        if h.name == "h2" and current is not None:
            g = _goi_parse_h2_group(h, page_url, base)
            if g is not None:
                regions[current].append(g)

    return regions, [], {}


def _next_goi_v3_list_version(
    path: str, new_payload: dict[str, Any]
) -> int:
    """entries + metadata + goiRegions が不変なら listVersion 据え置き。"""
    if not os.path.isfile(path):
        return int(datetime.now(timezone.utc).timestamp())

    try:
        with open(path, encoding="utf-8") as f:
            old = json.load(f)
    except (OSError, ValueError) as ex:
        # Corrupted prior GoI manifest: warn loudly and bump version so consumers refetch.
        print(f"WARN: _next_goi_v3_list_version: failed to read {path}: {ex}", file=sys.stderr)
        return int(datetime.now(timezone.utc).timestamp())
    if int(old.get("schemaVersion") or 0) < GOI_MANIFEST_SCHEMA_VERSION:
        return int(old.get("listVersion") or 0) + 1

    def norm(p: dict[str, Any]) -> dict[str, Any]:
        return {
            "entries": p.get("entries"),
            "metadata": p.get("metadata") or {},
            "goiRegions": p.get("goiRegions") or {},
        }

    if norm(old) == norm(new_payload):
        return int(old.get("listVersion") or 0)
    return int(old.get("listVersion") or 0) + 1


def write_goi_manifest_v3(path: str, payload: dict[str, Any]) -> None:
    light = payload.get("entries")
    if not isinstance(light, list):
        raise ValueError("goi v3: entries must be a list")
    md = payload.get("metadata") or {}
    if not isinstance(md, dict):
        raise ValueError("goi v3: metadata must be object")
    validate_manifest_entries_metadata(light, md, os.path.basename(path))
    tmp_payload = {**payload}
    tmp_payload["metadata"] = md
    tmp_payload["schemaVersion"] = GOI_MANIFEST_SCHEMA_VERSION
    lv = _next_goi_v3_list_version(path, tmp_payload)
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmp_payload["listVersion"] = lv
    tmp_payload["generatedAt"] = gen
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tpath = path + ".tmp"
    with open(tpath, "w", encoding="utf-8") as f:
        json.dump(tmp_payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tpath, path)


def load_existing_tales_metadata(path: str) -> tuple[dict[str, int], dict[str, str], dict[str, str]]:
    """既存 `manifest_tales.json` から `i -> lu` / `i -> img` / `i -> desc` を読み出す。

    daily / weekly モードで「前回値の引き継ぎ」と「差分検出（lu が変わった記事だけ本文を再 fetch）」に使う。
    ファイルが無い／壊れているときは全部空の辞書。
    """
    lu_by_i: dict[str, int] = {}
    img_by_i: dict[str, str] = {}
    desc_by_i: dict[str, str] = {}
    if not os.path.isfile(path):
        return lu_by_i, img_by_i, desc_by_i
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return lu_by_i, img_by_i, desc_by_i
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if isinstance(entries, list):
        for ent in entries:
            if not isinstance(ent, dict):
                continue
            ik = ent.get("i")
            if not isinstance(ik, str) or not ik.strip():
                continue
            key = ik.strip()
            lu = ent.get("lu")
            if isinstance(lu, int) and lu > 0:
                lu_by_i[key] = lu
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if isinstance(metadata, dict):
        for ik, chunk in metadata.items():
            if not isinstance(ik, str) or not isinstance(chunk, dict):
                continue
            key = ik.strip()
            if not key:
                continue
            img = chunk.get("img")
            if isinstance(img, str) and img.strip():
                img_by_i[key] = img.strip()
            desc = chunk.get("desc")
            if isinstance(desc, str) and desc.strip():
                desc_by_i[key] = desc.strip()
    return lu_by_i, img_by_i, desc_by_i


def load_existing_manifest_img_desc(path: str) -> tuple[dict[str, str], dict[str, str]]:
    """既存 manifest の `metadata` セクションから `i -> img` / `i -> desc` を読み出す。

    trifold SCP 報告書・jokes など `lu` を持たないマニフェスト用の軽量版。
    ファイルが無い／壊れているときは `({}, {})` を返す。
    """
    img_by_i: dict[str, str] = {}
    desc_by_i: dict[str, str] = {}
    if not os.path.isfile(path):
        return img_by_i, desc_by_i
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, ValueError):
        return img_by_i, desc_by_i
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if not isinstance(metadata, dict):
        return img_by_i, desc_by_i
    for ik, chunk in metadata.items():
        if not isinstance(ik, str) or not isinstance(chunk, dict):
            continue
        key = ik.strip()
        if not key:
            continue
        img = chunk.get("img")
        if isinstance(img, str) and img.strip():
            img_by_i[key] = img.strip()
        desc = chunk.get("desc")
        if isinstance(desc, str) and desc.strip():
            desc_by_i[key] = desc.strip()
    return img_by_i, desc_by_i


def enrich_entries_img_desc_in_place(
    session: requests.Session,
    entries: list[dict[str, Any]],
    *,
    mode: str = "full",
    prior_img: dict[str, str] | None = None,
    prior_desc: dict[str, str] | None = None,
    deadline: float | None = None,
) -> None:
    """entries に `img`/`desc` を付与する（SCP 報告書・jokes 共用）。

    モード:
    - `daily`: 本文 fetch しない。prior_img / prior_desc を entries に復元するだけ。
    - `weekly`: prior_img に `i` が既にある記事はスキップ（差分取得）。ない記事のみ本文フェッチ。
    - `full`: 全件フェッチ。

    付与した img / desc は entries の各 dict に直接セットする。
    呼び出し元は `merge_img_desc_from_entries_to_metadata` で metadata へ移すこと。
    `deadline` (time.time() 換算の unix 秒) を指定すると、超過時にループを中断して部分結果を返す。
    """
    prior_img = prior_img or {}
    prior_desc = prior_desc or {}

    if mode == "daily":
        for ent in entries:
            ik = (ent.get("i") or "").strip()
            if not ik:
                continue
            if ik in prior_img:
                ent.setdefault("img", prior_img[ik])
            if ik in prior_desc:
                ent.setdefault("desc", prior_desc[ik])
        return

    fetched = 0
    skipped = 0
    for ent in entries:
        u = (ent.get("u") or "").strip()
        ik = (ent.get("i") or "").strip()
        if not u or not ik:
            continue
        if mode == "weekly" and ik in prior_img:
            ent.setdefault("img", prior_img[ik])
            if ik in prior_desc:
                ent.setdefault("desc", prior_desc[ik])
            skipped += 1
            continue
        if deadline is not None and time.time() >= deadline:
            print(
                f"INFO: img/desc enrichment — time budget reached, stopping early "
                f"(fetched={fetched} preserved={skipped})",
                file=sys.stderr,
            )
            break
        try:
            html = fetch_html(session, u)
            soup = BeautifulSoup(html, "html.parser")
            img = extract_first_content_image_url(soup, u)
            if img:
                ent["img"] = img
            desc = extract_description_excerpt(soup)
            if desc:
                ent["desc"] = desc
            fetched += 1
        except HTTPError as e:
            st = e.response.status_code if e.response is not None else 0
            print(f"WARN: article page fetch failed ({st}): {u}", file=sys.stderr)
        except Exception as ex:
            print(f"WARN: article page fetch failed: {u} ({ex})", file=sys.stderr)

    print(
        f"INFO: img/desc enrichment — mode={mode} fetched={fetched} preserved={skipped}",
        file=sys.stderr,
    )


def merge_img_desc_from_entries_to_metadata(
    entries: list[dict[str, Any]], metadata: dict[str, Any]
) -> None:
    """entries の `img`/`desc` を metadata dict に移し、entries からは除去する。

    `enrich_entries_img_desc_in_place` の後に呼ぶ。
    entries は `{"u":..., "i":..., "t":...}` 形式の manifest entries list。
    """
    for ent in entries:
        ik = (ent.get("i") or "").strip()
        if not ik:
            continue
        for field in ("img", "desc"):
            val = ent.pop(field, None)
            if val:
                if ik not in metadata:
                    metadata[ik] = {}
                metadata[ik][field] = val


def load_harvest_state(output_dir: str) -> dict[str, Any]:
    """`_harvest_state.json` を読む。無ければ空辞書を返す。

    weekly モードのセクション開始位置ローテーション用に `section_offset` を保持する。
    """
    path = os.path.join(output_dir, HARVEST_STATE_FILENAME)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (OSError, ValueError):
        pass
    return {}


def save_harvest_state(output_dir: str, state: dict[str, Any]) -> None:
    """`_harvest_state.json` を原子的に保存。"""
    path = os.path.join(output_dir, HARVEST_STATE_FILENAME)
    tpath = path + ".tmp"
    with open(tpath, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tpath, path)


def extract_first_content_image_url(soup: BeautifulSoup, base_url: str) -> str | None:
    """`#page-content` 内の最初の `<img>` を絶対 URL で返す。サイドメニュー・ナビ画像は除外。"""
    content = soup.select_one("#page-content")
    if content is None:
        return None
    for img in content.find_all("img"):
        # 親要素にナビ／メタ系のクラスが付いていれば除外
        skip = False
        node = img
        for _ in range(6):
            cls = node.get("class") or [] if hasattr(node, "get") else []
            if any(c in {"nav", "footnote", "rate-points", "page-rate-widget-box"} for c in cls):
                skip = True
                break
            parent = getattr(node, "parent", None)
            if parent is None:
                break
            node = parent
        if skip:
            continue
        src = img.get("src") or img.get("data-src")
        if not isinstance(src, str) or not src.strip():
            continue
        s = src.strip()
        if s.startswith("data:"):
            continue
        return urljoin(base_url, s)
    return None


_DESCRIPTION_HEADER_RE = re.compile(r"(説明|description)", re.I)
_DESC_MAX_CHARS = 260


def extract_description_excerpt(soup: BeautifulSoup) -> str | None:
    """`#page-content` の「説明 / Description」セクション直後のテキスト先頭 260 字。

    見出しが見つからないときは `#page-content` 直下の最初のまとまった段落を返す。
    """
    content = soup.select_one("#page-content")
    if content is None:
        return None

    def collect_after(node, max_chars: int) -> str:
        out: list[str] = []
        cur = 0
        sib = node.find_next_sibling()
        while sib is not None and cur < max_chars:
            if sib.name in {"h1", "h2", "h3"}:
                break
            text = sib.get_text(separator=" ", strip=True)
            if text:
                out.append(text)
                cur += len(text)
            sib = sib.find_next_sibling()
        joined = " ".join(out).strip()
        if not joined:
            return ""
        if len(joined) > max_chars:
            joined = joined[:max_chars].rstrip() + "…"
        return joined

    for h in content.find_all(["h1", "h2", "h3"]):
        title = h.get_text(separator=" ", strip=True)
        if title and _DESCRIPTION_HEADER_RE.search(title):
            t = collect_after(h, _DESC_MAX_CHARS)
            if t:
                return t

    # フォールバック: 最初の `<p>`
    p = content.find("p")
    if p is not None:
        text = p.get_text(separator=" ", strip=True)
        if text:
            if len(text) > _DESC_MAX_CHARS:
                text = text[:_DESC_MAX_CHARS].rstrip() + "…"
            return text
    return None


def extract_opening_excerpt(soup: BeautifulSoup) -> str | None:
    """`#page-content` の最初の `<p>` テキストを最大 _DESC_MAX_CHARS 字で返す。
    Tales / Canons / GoIs 用（heading 検索なし）。
    """
    content = soup.select_one("#page-content")
    if content is None:
        return None
    p = content.find("p")
    if p is None:
        return None
    text = p.get_text(separator=" ", strip=True)
    if not text:
        return None
    if len(text) > _DESC_MAX_CHARS:
        text = text[:_DESC_MAX_CHARS].rstrip() + "…"
    return text


def enrich_tale_entries_last_updated_in_place(
    session: requests.Session,
    entries: list[dict[str, Any]],
    *,
    mode: str = "full",
    prior_lu: dict[str, int] | None = None,
    prior_desc: dict[str, str] | None = None,
    deadline: float | None = None,
) -> None:
    """各 Tale の本文 URL を取得し `#page-info` から `lu`、`#page-content` 冒頭段落から `desc` を付与。
    Tales は img を収集しない（画像がほぼ存在しないため）。

    モード:
    - `daily`: 本文 fetch しない。前回値（`prior_lu`/`prior_desc`）を復元するだけ。
    - `weekly`: `lu` と `desc` が両方判明している記事はスキップ（差分取得）。
    - `full`: 全記事を取得し直す。
    `deadline` (time.time() 換算の unix 秒) を指定すると、超過時にループを中断して部分結果を返す。
    """
    prior_lu = prior_lu or {}
    prior_desc = prior_desc or {}

    if mode == "daily":
        for ent in entries:
            ik = (ent.get("i") or "").strip() if isinstance(ent.get("i"), str) else ""
            if not ik:
                continue
            if ik in prior_lu:
                ent["lu"] = prior_lu[ik]
            if ik in prior_desc:
                ent.setdefault("desc", prior_desc[ik])
        return

    fetched: int = 0
    skipped: int = 0
    for ent in entries:
        u = (ent.get("u") or "").strip() if isinstance(ent.get("u"), str) else ""
        ik = (ent.get("i") or "").strip() if isinstance(ent.get("i"), str) else ""
        if not u or not ik:
            continue
        if mode == "weekly":
            if ik in prior_lu and not ent.get("lu"):
                ent["lu"] = prior_lu[ik]
            if ik in prior_desc:
                ent.setdefault("desc", prior_desc[ik])
            if ent.get("lu") and ent.get("desc"):
                skipped += 1
                continue
        if deadline is not None and time.time() >= deadline:
            print(
                f"INFO: tales enrichment — time budget reached, stopping early "
                f"(fetched={fetched} preserved={skipped})",
                file=sys.stderr,
            )
            break
        try:
            html = fetch_html(session, u)
            soup = BeautifulSoup(html, "html.parser")
            ts = extract_page_info_unix_from_soup(soup)
            if ts is not None and not ent.get("lu"):
                ent["lu"] = ts
            desc = extract_opening_excerpt(soup)
            if desc and not ent.get("desc"):
                ent["desc"] = desc
            fetched += 1
        except HTTPError as e:
            st = e.response.status_code if e.response is not None else 0
            print(f"WARN: tale page fetch failed ({st}): {u}", file=sys.stderr)
        except Exception as ex:
            print(f"WARN: tale page fetch failed: {u} ({ex})", file=sys.stderr)

    print(f"INFO: tales enrichment — mode={mode} fetched={fetched} preserved={skipped}", file=sys.stderr)


def tales_raw_to_manifest_parts(raw: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    light: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}
    for e in raw:
        if not isinstance(e, dict):
            continue
        i = e.get("i")
        if not isinstance(i, str) or not i.strip():
            continue
        u = e.get("u")
        t = e.get("t")
        if not isinstance(u, str) or not u.strip():
            continue
        row_u = u.strip()
        row_i = i.strip()
        title = t if isinstance(t, str) and t.strip() else row_i
        row: dict[str, Any] = {"u": row_u, "i": row_i, "t": title}
        lu_raw = e.get("lu")
        if isinstance(lu_raw, int) and lu_raw > 0:
            row["lu"] = lu_raw
        light.append(row)
        md_chunk: dict[str, Any] = {}
        a = e.get("a")
        if isinstance(a, str) and a.strip():
            md_chunk["a"] = a.strip()
        r_raw = e.get("r")
        if isinstance(r_raw, str) and r_raw.strip():
            md_chunk["r"] = r_raw.strip()
        # サムネ URL と説明抜粋（260 字）はアプリの「続きから読む」プレビュー用。アプリ側 WebView 抽出を不要にする。
        img_raw = e.get("img")
        if isinstance(img_raw, str) and img_raw.strip():
            md_chunk["img"] = img_raw.strip()
        desc_raw = e.get("desc")
        if isinstance(desc_raw, str) and desc_raw.strip():
            md_chunk["desc"] = desc_raw.strip()
        if md_chunk:
            metadata[row_i] = md_chunk
    light.sort(key=lambda r: str((r.get("i") or "")).lower())
    return light, metadata


def simple_multiform_raw_to_manifest_parts(
    raw: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """entries が u,i,t のみの raw 行 → light entries + 空 metadata。"""
    light: list[dict[str, Any]] = []
    for e in raw:
        if not isinstance(e, dict):
            continue
        i = e.get("i")
        u = e.get("u")
        if not isinstance(i, str) or not i.strip() or not isinstance(u, str) or not u.strip():
            continue
        t = e.get("t")
        ikey = i.strip()
        light.append(
            {
                "u": u.strip(),
                "i": ikey,
                "t": (t if isinstance(t, str) and t.strip() else ikey),
            }
        )
    return light, {}


# --- 著者層: foundation-tales-jp（Swift パーサと同等の簡易ロジック） ---

AUTHOR_TABLE_OPEN = '<table style="width: 100%;margin-top:1.2em">'
WIKI_TABLE_OPEN = '<table class="wiki-content-table">'


def parse_foundation_tales_jp(html: str, cfg: BranchConfig) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    base = cfg.site_host.rstrip("/")
    scan = 0
    while True:
        idx = html.find(AUTHOR_TABLE_OPEN, scan)
        if idx < 0:
            break
        inner_b = idx + len(AUTHOR_TABLE_OPEN)
        end = html.find("</table>", inner_b)
        if end < 0:
            break
        author_inner = html[inner_b:end]
        author_name = _extract_author_name(author_inner)
        after_author = end + len("</table>")
        next_author = html.find(AUTHOR_TABLE_OPEN, after_author)
        tail_end = next_author if next_author >= 0 else len(html)
        tail = html[after_author:tail_end]
        wiki_open = tail.find(WIKI_TABLE_OPEN)
        tales_block = ""
        if wiki_open >= 0:
            wc = wiki_open + len(WIKI_TABLE_OPEN)
            wiki_close = tail.find("</table>", wc)
            if wiki_close >= 0:
                tales_block = tail[wc:wiki_close]
        for m in re.finditer(
            r'<td><a href="([^"]+)">([^<]*)</a></td>',
            tales_block,
            flags=re.IGNORECASE,
        ):
            href = m.group(1).strip()
            title = (
                m.group(2)
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .strip()
            )
            if not href:
                continue
            pu = urlparse(urljoin(base + "/", href))
            if pu.netloc and pu.netloc != urlparse(base).netloc:
                continue
            path = pu.path or "/"
            i = path.lstrip("/").lower()
            u = base + (path if path.startswith("/") else "/" + path)
            t = title if title else i
            ent: dict[str, Any] = {"u": u, "i": i, "t": t}
            if author_name:
                ent["a"] = author_name
            entries.append(ent)
        # 次の著者ブロック先頭へ（同一位置の再検索ループを防ぐ）
        scan = next_author if next_author >= 0 else len(html)
    return entries


def _extract_author_name(fragment: str) -> str | None:
    em = re.search(
        r'<span class="error-inline"[^>]*>.*?<em>([^<]+)</em>',
        fragment,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if em:
        s = em.group(1).strip()
        if s:
            return s
    last: str | None = None
    for m in re.finditer(
        r'<a href="https?://www\.wikidot\.com/user:info/[^"]+"[^>]*>([^<]*)</a>',
        fragment,
        flags=re.IGNORECASE,
    ):
        t = m.group(1).strip()
        if t:
            last = t
    return last


def parse_goi_tag_pages(session: requests.Session, cfg: BranchConfig) -> list[dict[str, Any]]:
    """goi-format タグ一覧からエントリを収集（ページ数上限あり）。"""
    base = cfg.site_host.rstrip("/")
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for page in range(1, cfg.goi_tag_max_pages + 1):
        url = f"{base}/system:page-tags/tag/{cfg.goi_tag}"
        if page > 1:
            url += f"/p/{page}"
        try:
            html = fetch_html(session, url, retries=4)
        except Exception as ex:
            print(f"WARN: goi tag page {page}: {ex}", file=sys.stderr)
            break
        soup = BeautifulSoup(html, "html.parser")
        page_had_new = False
        for a in soup.select(".list-pages-box a[href], .pages-list-item a[href], #page-content a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("/"):
                continue
            if "/system:" in href:
                continue
            title = a.get_text(strip=True)
            pu = urlparse(urljoin(base + "/", href))
            if pu.netloc and pu.netloc != urlparse(base).netloc:
                continue
            path = pu.path or "/"
            if not path.startswith("/scp-"):
                continue
            i = path.lstrip("/").lower()
            if i in seen:
                continue
            seen.add(i)
            u = base + path
            ent: dict[str, Any] = {"u": u, "i": i, "t": title or i}
            out.append(ent)
            page_had_new = True
        if not page_had_new and page > 1:
            break
    return out


class JapaneseBranchHarvester:
    """JP 支部の収集 orchestrator。"""

    def __init__(
        self,
        cfg: BranchConfig | None = None,
        mode: str = "full",
        deadline: float | None = None,
        section_offset: int = 0,
    ):
        self.cfg = cfg or BranchConfig()
        self.session = requests.Session()
        # daily / weekly / full のクロール階層。詳細は `_HARVEST_MODE_CHOICES`。
        self.mode = mode if mode in _HARVEST_MODE_CHOICES else "full"
        # time.time() 換算の締め切り。None = 無制限。
        self.deadline = deadline
        # weekly モードでセクション開始位置を毎回ずらすためのオフセット（state ファイル由来）。
        self.section_offset = max(0, int(section_offset))

    def run(self) -> None:
        cfg = self.cfg
        os.makedirs(cfg.output_dir, exist_ok=True)
        out_abs = os.path.abspath(cfg.output_dir)
        print(f"INFO: output-dir (absolute): {out_abs}", file=sys.stderr)
        print(
            f"INFO: manifest_canons.json will be: {os.path.join(out_abs, 'manifest_canons.json')}",
            file=sys.stderr,
        )
        man_jp = os.path.join(cfg.output_dir, "manifest_scp-jp.json")
        man_main = os.path.join(cfg.output_dir, "manifest_scp-main.json")
        man_int = os.path.join(cfg.output_dir, "manifest_scp-int.json")
        man_tales = os.path.join(cfg.output_dir, "manifest_tales.json")
        man_gois = os.path.join(cfg.output_dir, "manifest_gois.json")
        man_canons = os.path.join(cfg.output_dir, "manifest_canons.json")
        man_jokes = os.path.join(cfg.output_dir, "manifest_jokes.json")

        print("INFO: base layer — JP series", file=sys.stderr)
        jp_rows = scrape_series_jp(self.session, cfg)

        print("INFO: base layer — mainlist", file=sys.stderr)
        main_rows = scrape_series_main(self.session, cfg)
        attach_jp_mainlist_title_from_main_series(jp_rows, main_rows)

        jp_paths = set(jp_rows.keys())
        main_paths = set(main_rows.keys())

        print("INFO: attribute layer — manifest raw tags from jp_tag.json", file=sys.stderr)
        jp_tag_articles = load_jp_tag_articles(cfg.output_dir)
        attach_tags_from_jp_tag_map(jp_rows, jp_tag_articles)
        attach_tags_from_jp_tag_map(main_rows, jp_tag_articles)

        print("INFO: attribute layer — object class from jp_tag.json", file=sys.stderr)
        oc_jp, tagged_jp = object_class_map_from_jp_tag_articles(jp_paths, jp_tag_articles)
        oc_main, tagged_main = object_class_map_from_jp_tag_articles(main_paths, jp_tag_articles)
        apply_object_classes_to_article_rows(jp_rows, oc_jp)
        apply_object_classes_to_article_rows(main_rows, oc_main)
        existing_oc_jp = object_class_map_from_existing_manifest(
            jp_paths, load_existing_manifest_object_classes(man_jp)
        )
        existing_oc_main = object_class_map_from_existing_manifest(
            main_paths, load_existing_manifest_object_classes(man_main)
        )
        apply_object_classes_to_article_rows(jp_rows, existing_oc_jp)
        apply_object_classes_to_article_rows(main_rows, existing_oc_main)

        print("INFO: intl lists (hub crawl)", file=sys.stderr)
        int_entries = build_int_rows_from_wikidot(self.session, cfg)
        int_paths: set[str] = set()
        for ent in int_entries:
            raw_u = ent.get("u")
            if isinstance(raw_u, str) and raw_u.strip():
                pth = urlparse(raw_u.strip()).path or ""
                if pth:
                    int_paths.add(pth)
        print("INFO: attribute layer — intl object class from jp_tag.json", file=sys.stderr)
        oc_int, tagged_int = object_class_map_from_jp_tag_articles(int_paths, jp_tag_articles)
        existing_oc_int = object_class_map_from_existing_manifest(
            int_paths, load_existing_manifest_object_classes(man_int)
        )
        # Entries without metadata["c"] here rely on oc_int hitting `pth`; expand tag-page crawl or light `g` if OC is missing in app.
        md_int: dict[str, Any] = {}
        for ent in int_entries:
            ik = ent.get("i")
            if not isinstance(ik, str) or not ik.strip():
                continue
            pth = ""
            raw_u = ent.get("u")
            if isinstance(raw_u, str) and raw_u.strip():
                pth = urlparse(raw_u.strip()).path or ""
            c_val = (oc_int.get(pth) or existing_oc_int.get(pth)) if pth else None
            if c_val and str(c_val).strip():
                md_int[ik.strip()] = {"c": str(c_val).strip()}

        print("INFO: tales — foundation-tales-jp + foundation-tales", file=sys.stderr)
        tales_html = fetch_html(self.session, cfg.abs_url(cfg.foundation_tales_path))
        jp_tales = parse_foundation_tales_jp(tales_html, cfg)
        for ent in jp_tales:
            ent["r"] = "jp"
        tale_by_i: dict[str, dict[str, Any]] = {}
        for ent in jp_tales:
            ik = ent.get("i")
            if isinstance(ik, str) and ik.strip():
                tale_by_i[ik.strip()] = ent
        try:
            tales_en_html = fetch_html(self.session, cfg.abs_url(FOUNDATION_TALES_EN), retries=4)
            en_tales = parse_foundation_tales_jp(tales_en_html, cfg)
            for ent in en_tales:
                ent["r"] = "en"
                ik = ent.get("i")
                if not isinstance(ik, str) or not ik.strip():
                    continue
                key = ik.strip()
                if key in tale_by_i:
                    prev = tale_by_i[key]
                    if prev.get("r") == "jp" and ent.get("r") == "en":
                        prev["r"] = "jp+en"
                else:
                    tale_by_i[key] = ent
        except Exception as ex:
            print(f"WARN: foundation-tales (EN hub): {ex}", file=sys.stderr)
        tale_entries = list(tale_by_i.values())

        print("INFO: gois — goi-formats-jp (structured, schema v3)", file=sys.stderr)
        goi_regions, goi_flat, goi_meta = scrape_goi_formats_hub_structured(
            self.session, cfg
        )
        print("INFO: gois — per-hub desc enrichment", file=sys.stderr)
        enrich_goi_regions_desc_in_place(self.session, goi_regions)
        goi_payload: dict[str, Any] = {
            "entries": goi_flat,
            "metadata": {
                k: v for k, v in goi_meta.items() if isinstance(v, dict) and v
            },
            "goiRegions": {
                "en": goi_regions.get("en") or [],
                "jp": goi_regions.get("jp") or [],
                "other": goi_regions.get("other") or [],
            },
        }

        print(
            "INFO: canons — canon-hub-jp + series-hub-jp + canon-hub (canon-block)",
            file=sys.stderr,
        )
        canon_light, canon_meta, canon_regions = scrape_canon_manifest_payload(
            self.session, cfg
        )

        print("INFO: jokes — joke-scps + joke-scps-jp (li-based titles like series)", file=sys.stderr)
        joke_rows = scrape_joke_article_rows(self.session, cfg)
        attach_tags_from_jp_tag_map(joke_rows, jp_tag_articles)
        print("INFO: attribute layer — joke object class from jp_tag.json", file=sys.stderr)
        joke_paths = set(joke_rows.keys())
        oc_joke, tagged_joke = object_class_map_from_jp_tag_articles(joke_paths, jp_tag_articles)
        apply_object_classes_to_article_rows(joke_rows, oc_joke)
        existing_oc_joke = object_class_map_from_existing_manifest(
            joke_paths, load_existing_manifest_object_classes(man_jokes)
        )
        apply_object_classes_to_article_rows(joke_rows, existing_oc_joke)

        missing_from_jp_tag = (
            paths_missing_from_jp_tag(jp_paths, jp_tag_articles)
            | paths_missing_from_jp_tag(main_paths, jp_tag_articles)
            | paths_missing_from_jp_tag(int_paths, jp_tag_articles)
            | paths_missing_from_jp_tag(joke_paths, jp_tag_articles)
        )
        fallback_targets = missing_from_jp_tag if not jp_tag_articles else set()
        total_targets = len(jp_paths) + len(main_paths) + len(int_paths) + len(joke_paths)
        total_tagged = tagged_jp + tagged_main + tagged_int + tagged_joke
        total_from_jp_tag = len(oc_jp) + len(oc_main) + len(oc_int) + len(oc_joke)
        total_from_existing = len(existing_oc_jp) + len(existing_oc_main) + len(existing_oc_int) + len(existing_oc_joke)
        print(
            "INFO: object class jp_tag summary — "
            f"targets={total_targets} tagged={total_tagged} "
            f"resolved={total_from_jp_tag} existing_manifest_resolved={total_from_existing} "
            f"missing_from_jp_tag={len(missing_from_jp_tag)} fallback_candidates={len(fallback_targets)}",
            file=sys.stderr,
        )
        if fallback_targets:
            print(
                "INFO: object class fallback — crawling page-tags only for "
                f"{len(fallback_targets)} paths missing from jp_tag.json",
                file=sys.stderr,
            )
            fallback_oc = crawl_object_class_tag_pages(self.session, cfg, fallback_targets)
            apply_object_classes_to_article_rows(jp_rows, filter_object_class_map(fallback_oc, jp_paths))
            apply_object_classes_to_article_rows(main_rows, filter_object_class_map(fallback_oc, main_paths))
            apply_object_classes_to_article_rows(joke_rows, filter_object_class_map(fallback_oc, joke_paths))
            for ent in int_entries:
                ik = ent.get("i")
                if not isinstance(ik, str) or not ik.strip():
                    continue
                raw_u = ent.get("u")
                pth = urlparse(raw_u.strip()).path if isinstance(raw_u, str) and raw_u.strip() else ""
                if pth in fallback_oc:
                    chunk = md_int.setdefault(ik.strip(), {})
                    if isinstance(chunk, dict) and not chunk.get("c"):
                        chunk["c"] = fallback_oc[pth]
            print(
                "INFO: object class fallback summary — "
                f"resolved={len(fallback_oc)} unresolved={len(fallback_targets) - len(fallback_oc)}",
                file=sys.stderr,
            )
        else:
            print("INFO: object class fallback — skipped (jp_tag.json loaded)", file=sys.stderr)

        # 5 セクション分の trifold/raw 分解と md prep。これを先に済ませることで、
        # 下のローテーションに渡す closure が共通の前提状態の上で動ける。
        ej, mj = trifold_rows_to_manifest_parts(jp_rows)
        em, mm = trifold_rows_to_manifest_parts(main_rows)
        enrich_metadata_tags_from_jp_tag_map(int_entries, md_int, jp_tag_articles)
        jl, jm = trifold_rows_to_manifest_parts(joke_rows)

        def _enrich_write_scp_jp() -> None:
            prior_img, prior_desc = load_existing_manifest_img_desc(man_jp)
            enrich_entries_img_desc_in_place(
                self.session, ej, mode=self.mode, prior_img=prior_img, prior_desc=prior_desc,
                deadline=self.deadline,
            )
            merge_img_desc_from_entries_to_metadata(ej, mj)
            write_manifest(man_jp, ej, mj)

        def _enrich_write_scp_main() -> None:
            prior_img, prior_desc = load_existing_manifest_img_desc(man_main)
            enrich_entries_img_desc_in_place(
                self.session, em, mode=self.mode, prior_img=prior_img, prior_desc=prior_desc,
                deadline=self.deadline,
            )
            merge_img_desc_from_entries_to_metadata(em, mm)
            write_manifest(man_main, em, mm)

        def _enrich_write_scp_int() -> None:
            prior_img, prior_desc = load_existing_manifest_img_desc(man_int)
            enrich_entries_img_desc_in_place(
                self.session, int_entries, mode=self.mode, prior_img=prior_img, prior_desc=prior_desc,
                deadline=self.deadline,
            )
            merge_img_desc_from_entries_to_metadata(int_entries, md_int)
            write_manifest(man_int, int_entries, md_int)

        def _enrich_write_tales() -> None:
            print(f"INFO: tales — per-article enrichment (mode={self.mode})", file=sys.stderr)
            prior_lu, _prior_img_unused, prior_desc = load_existing_tales_metadata(man_tales)
            enrich_tale_entries_last_updated_in_place(
                self.session, tale_entries, mode=self.mode,
                prior_lu=prior_lu, prior_desc=prior_desc, deadline=self.deadline,
            )
            tl, tm = tales_raw_to_manifest_parts(tale_entries)
            n_lu = sum(1 for x in tl if isinstance(x.get("lu"), int) and int(x["lu"]) > 0)
            print(
                f"INFO: manifest_tales — entries={len(tl)} with_lu={n_lu} (listVersion via write_manifest)",
                file=sys.stderr,
            )
            write_manifest(man_tales, tl, tm)

        def _enrich_write_jokes() -> None:
            prior_img, prior_desc = load_existing_manifest_img_desc(man_jokes)
            enrich_entries_img_desc_in_place(
                self.session, jl, mode=self.mode, prior_img=prior_img, prior_desc=prior_desc,
                deadline=self.deadline,
            )
            merge_img_desc_from_entries_to_metadata(jl, jm)
            write_manifest(man_jokes, jl, jm)

        # weekly モード時は section_offset で開始位置を 1 ずつ rotate（5 回で全セクションが「最初」になる）。
        # daily / full は固定順（既存挙動維持）。
        sections: list[tuple[str, "callable"]] = [
            ("scp-jp", _enrich_write_scp_jp),
            ("scp-main", _enrich_write_scp_main),
            ("scp-int", _enrich_write_scp_int),
            ("tales", _enrich_write_tales),
            ("jokes", _enrich_write_jokes),
        ]
        if self.mode == "weekly":
            offset = int(self.section_offset) % len(sections)
            if offset:
                sections = sections[offset:] + sections[:offset]
            print(
                f"INFO: section rotation — offset={offset} order={[s[0] for s in sections]}",
                file=sys.stderr,
            )
        for name, action in sections:
            action()

        write_goi_manifest_v3(man_gois, goi_payload)
        write_canon_manifest(man_canons, canon_light, canon_meta, canon_regions)

        print(
            f"OK: wrote {man_jp}, {man_main}, {man_int}, {man_tales}, {man_gois}, {man_canons}, {man_jokes}",
            file=sys.stderr,
        )

        # 全 manifest の sha256/byteSize/listVersion/entryCount を 1 ファイルにまとめる。
        # アプリ側は起動時にこの数 KB だけを GET し、変わった kind だけ本体を取りに行く。
        write_catalog_index(
            cfg.output_dir,
            files=[
                ("manifest_scp-jp", man_jp),
                ("manifest_scp-main", man_main),
                ("manifest_scp-int", man_int),
                ("manifest_tales", man_tales),
                ("manifest_canons", man_canons),
                ("manifest_gois", man_gois),
                ("manifest_jokes", man_jokes),
            ],
        )


CATALOG_INDEX_SCHEMA_VERSION = 1


def write_catalog_index(output_dir: str, *, files: list[tuple[str, str]]) -> None:
    """`catalog_index.json` を書き出す。

    アプリ起動時のチェックを軽量化するための頂点インデックス。
    各 manifest の `listVersion` / `schemaVersion` / `generatedAt` / `contentHash`(sha256) /
    `byteSize` / `entryCount` を 1 つにまとめる。
    """
    import hashlib

    rec_files: list[dict[str, Any]] = []
    for kind, fp in files:
        if not os.path.isfile(fp):
            continue
        try:
            with open(fp, "rb") as f:
                blob = f.read()
        except OSError:
            continue
        try:
            payload = json.loads(blob)
        except ValueError:
            continue
        if not isinstance(payload, dict):
            continue
        list_version = payload.get("listVersion")
        schema_version = payload.get("schemaVersion")
        generated_at = payload.get("generatedAt")
        entries = payload.get("entries")
        entry_count = len(entries) if isinstance(entries, list) else None
        rec_files.append({
            "kind": kind,
            "url": os.path.basename(fp),
            "listVersion": list_version if isinstance(list_version, int) else None,
            "schemaVersion": schema_version if isinstance(schema_version, int) else None,
            "generatedAt": generated_at if isinstance(generated_at, str) else None,
            "contentHash": "sha256-" + hashlib.sha256(blob).hexdigest(),
            "byteSize": len(blob),
            "entryCount": entry_count,
        })
    out_path = os.path.join(output_dir, "catalog_index.json")
    payload = {
        "schemaVersion": CATALOG_INDEX_SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": rec_files,
    }
    tpath = out_path + ".tmp"
    with open(tpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tpath, out_path)
    print(f"OK: wrote {out_path} (files={len(rec_files)})", file=sys.stderr)


def _git_toplevel(start_dir: str) -> str | None:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=start_dir,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if r.returncode == 0 and r.stdout.strip():
            return os.path.abspath(r.stdout.strip())
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def _resolve_git_repo_root(output_dir: str, override: str) -> str:
    if override.strip():
        root = os.path.abspath(override.strip())
        if not os.path.isdir(os.path.join(root, ".git")):
            raise ValueError(f"--git-repo is not a git root: {root!r}")
        return root
    for start in (output_dir, REPO_ROOT):
        if not start or not os.path.isdir(start):
            continue
        top = _git_toplevel(start)
        if top:
            return top
    raise ValueError(
        "Could not find a git repository. Run from a clone, set --git-repo, "
        "or use --output-dir inside the data-scp-docs (or mirror) repo."
    )


def git_stage_commit_push_json(
    output_dir: str,
    *,
    do_commit: bool,
    do_push: bool,
    message: str,
    remote: str,
    repo_root_override: str,
) -> None:
    """output_dir 直下の *.json を stage → 差分があれば commit → do_push なら push。"""
    if not do_commit and not do_push:
        return
    out_abs = os.path.abspath(output_dir)
    if not os.path.isdir(out_abs):
        raise ValueError(f"output-dir does not exist: {out_abs!r}")
    repo_root = _resolve_git_repo_root(out_abs, repo_root_override)
    json_names = sorted(
        n
        for n in os.listdir(out_abs)
        if n.endswith(".json") and os.path.isfile(os.path.join(out_abs, n))
    )
    if not json_names:
        print("GIT: no *.json under output-dir; nothing to stage", file=sys.stderr)
        return
    rel_paths = [os.path.relpath(os.path.join(out_abs, n), repo_root) for n in json_names]
    for rp in rel_paths:
        if rp.startswith(".."):
            raise ValueError(
                f"output-dir must be inside git repo root {repo_root!r} (got {rp!r})"
            )
    subprocess.run(
        ["git", "add", "--"] + rel_paths,
        cwd=repo_root,
        check=True,
        timeout=120,
    )
    st = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=repo_root,
        check=False,
        timeout=60,
    )
    if st.returncode == 0:
        print("GIT: no staged changes; skip commit", file=sys.stderr)
    else:
        subprocess.run(
            ["git", "commit", "-m", message],
            cwd=repo_root,
            check=True,
            timeout=120,
        )
        print("GIT: committed", file=sys.stderr)
    if do_push:
        subprocess.run(
            ["git", "push", remote],
            cwd=repo_root,
            check=True,
            timeout=600,
        )
        print(f"GIT: pushed to {remote}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description="Hybrid harvester for list/jp/*.json")
    p.add_argument(
        "--output-dir",
        type=str,
        default="",
        help=(
            "list/jp 相当の書き出し先。未指定時は REPO_ROOT/list/jp "
            "（REPO_ROOT=本スクリプトの dirname/.. = 本リポジトリのルート）"
        ),
    )
    p.add_argument(
        "--git-commit",
        action="store_true",
        help="ハーベスト後、output-dir 内の *.json を git add / commit（変更がある場合のみコミット）",
    )
    p.add_argument(
        "--git-push",
        action="store_true",
        help="--git-commit 後に git push（--git-commit 未指定時はコミット処理も行う）",
    )
    p.add_argument(
        "--git-message",
        type=str,
        default="data: refresh list/jp manifests (harvester)",
        help="git commit メッセージ",
    )
    p.add_argument(
        "--git-remote",
        type=str,
        default="origin",
        help="git push 先 remote 名",
    )
    p.add_argument(
        "--git-repo",
        type=str,
        default="",
        help="リポジトリルート（未指定時は output-dir またはミラー REPO_ROOT から自動検出）",
    )
    p.add_argument(
        "--mode",
        type=str,
        choices=list(_HARVEST_MODE_CHOICES),
        default="full",
        help=(
            "クロール階層。`daily` は一覧ハブのみ取得し Tale 本文（lu/img/desc）は前回値を引き継ぐ；"
            "`weekly` は前回 lu が無い記事だけ本文を取得して img/desc も抽出；"
            "`full` は全記事の本文を取得し直す（旧来挙動・既定）。"
        ),
    )
    p.add_argument(
        "--time-budget-minutes",
        type=float,
        default=0.0,
        help=(
            "実行時間の上限（分）。0 = 無制限。CI タイムアウト前に graceful stop するために使う。"
            "時間切れ時点までに取得した img/desc を partial commit して、次回実行で続きを再開できる。"
        ),
    )
    p.add_argument(
        "--reset-state",
        action="store_true",
        help=(
            "weekly モードのセクション開始位置オフセットを 0 に戻す（schema 変更等で全件再エンリッチが必要になったとき用）。"
        ),
    )
    args = p.parse_args()
    cfg = BranchConfig()
    if args.output_dir:
        cfg.output_dir = os.path.abspath(args.output_dir)
    deadline = time.time() + args.time_budget_minutes * 60 if args.time_budget_minutes > 0 else None

    # weekly モード時のみ section_offset を state から読み、run 後に +1 して保存。
    # daily / full は state を一切いじらない（既存挙動）。
    state: dict[str, Any] = {}
    section_offset = 0
    if args.mode == "weekly":
        os.makedirs(cfg.output_dir, exist_ok=True)
        state = load_harvest_state(cfg.output_dir)
        if args.reset_state:
            state["section_offset"] = 0
        raw_off = state.get("section_offset", 0)
        section_offset = int(raw_off) if isinstance(raw_off, int) else 0

    try:
        try:
            JapaneseBranchHarvester(
                cfg, mode=args.mode, deadline=deadline, section_offset=section_offset
            ).run()
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1
    finally:
        # 例外で落ちても次回は別オフセットから始めたいので finally で保存。
        # 同じセクションで詰まり続けると永遠に他セクションへ到達できないため。
        if args.mode == "weekly":
            state["section_offset"] = (section_offset + 1) % 5
            save_harvest_state(cfg.output_dir, state)
            print(
                f"INFO: harvest state — next section_offset={state['section_offset']}",
                file=sys.stderr,
            )
    want_commit = bool(args.git_commit or args.git_push)
    try:
        git_stage_commit_push_json(
            cfg.output_dir,
            do_commit=want_commit,
            do_push=bool(args.git_push),
            message=args.git_message.strip() or "data: refresh list/jp manifests (harvester)",
            remote=args.git_remote.strip() or "origin",
            repo_root_override=args.git_repo,
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: git failed (exit {e.returncode})", file=sys.stderr)
        return e.returncode if e.returncode else 1
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
