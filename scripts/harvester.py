#!/usr/bin/env python3
"""
JP 支部向けハイブリッド索引: シリーズ一覧（基礎層）＋ page-tags（属性層）＋
foundation-tales-jp（著者層）を統合し list/jp/*.json を生成する。

マニフェスト（schemaVersion 2）: `manifest_scp-*.json` / `manifest_tales.json` /
`manifest_gois.json` に entries（u,i,t）とスパース metadata（主キー i）を出力する。

収集は Wikidot のみ（scp_list.json には依存しない）。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

MANIFEST_SCHEMA_VERSION = 2

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
INTL_PATH_RE = re.compile(r"^/scp-\d+-[a-z]{2}$")
MAX_INTL_LIST_PAGES = 80


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
    m = {
        0: SeriesRange(1, 999),
        1: SeriesRange(1000, 1999),
        2: SeriesRange(2000, 2999),
        3: SeriesRange(3000, 3999),
        4: SeriesRange(4000, 4999),
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


def map_object_class_from_tag_pages(session: requests.Session, cfg: BranchConfig, paths: set[str]) -> dict[str, str]:
    """属性層: system:page-tags/tag/<class> に掲載された記事パス → OC（先に列挙したタグを優先）。"""
    base = cfg.site_host.rstrip("/")
    path_to_class: dict[str, str] = {}
    for tag in OBJECT_CLASS_TAGS:
        url = f"{base}/system:page-tags/tag/{tag}"
        try:
            html = fetch_html(session, url, retries=4)
        except Exception as ex:
            print(f"WARN: OC tag page {tag}: {ex}", file=sys.stderr)
            continue
        soup = BeautifulSoup(html, "html.parser")
        oc_display = OC_TAG_TO_DISPLAY.get(tag, tag.replace("-", " ").title())
        for a in soup.find_all("a", href=True):
            raw = (a.get("href") or "").strip()
            pu = urlparse(urljoin(base + "/", raw))
            if pu.netloc and pu.netloc != urlparse(base).netloc:
                continue
            pth = pu.path or ""
            if pth not in paths:
                continue
            if pth not in path_to_class:
                path_to_class[pth] = oc_display
    return path_to_class


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
        if not INTL_PATH_RE.match(pth) or pth.endswith("-jp"):
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
    """国際支部リンクは /scp-international から辿った各一覧ページのスクレイプ結果のみ（scp_list 不要）。"""
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
    m = re.match(r"^/scp-(\d+)-([a-z]{2})$", path)
    if not m:
        return path.lstrip("/").upper()
    return f"SCP-{m.group(1)}-{m.group(2).upper()}"


def now_payload_meta() -> tuple[int, str]:
    dt = datetime.now(timezone.utc)
    return int(dt.timestamp()), dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_manifest(path: str, entries: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    """schemaVersion 2: entries（軽）+ metadata（スパース、キーは i）。"""
    lv, gen = now_payload_meta()
    md = {k: v for k, v in metadata.items() if isinstance(v, dict) and v}
    validate_manifest_entries_metadata(entries, md, os.path.basename(path))
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
        light.append({"u": u.strip(), "i": i.strip(), "t": (t if isinstance(t, str) and t.strip() else i.strip())})
        a = e.get("a")
        if isinstance(a, str) and a.strip():
            metadata[i.strip()] = {"a": a.strip()}
    return light, metadata


def goi_raw_to_manifest_parts(raw: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    light: list[dict[str, Any]] = []
    for e in raw:
        if not isinstance(e, dict):
            continue
        i = e.get("i")
        u = e.get("u")
        if not isinstance(i, str) or not i.strip() or not isinstance(u, str) or not u.strip():
            continue
        t = e.get("t")
        light.append(
            {
                "u": u.strip(),
                "i": i.strip(),
                "t": (t if isinstance(t, str) and t.strip() else i.strip()),
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

    def __init__(self, cfg: BranchConfig | None = None):
        self.cfg = cfg or BranchConfig()
        self.session = requests.Session()

    def run(self) -> None:
        cfg = self.cfg
        os.makedirs(cfg.output_dir, exist_ok=True)

        print("INFO: base layer — JP series", file=sys.stderr)
        jp_rows = scrape_series_jp(self.session, cfg)

        print("INFO: base layer — mainlist", file=sys.stderr)
        main_rows = scrape_series_main(self.session, cfg)
        attach_jp_mainlist_title_from_main_series(jp_rows, main_rows)

        jp_paths = set(jp_rows.keys())
        main_paths = set(main_rows.keys())

        print("INFO: attribute layer — object class tags", file=sys.stderr)
        oc_jp = map_object_class_from_tag_pages(self.session, cfg, jp_paths)
        oc_main = map_object_class_from_tag_pages(self.session, cfg, main_paths)
        for p, row in jp_rows.items():
            if not row.c and p in oc_jp:
                row.c = oc_jp[p]
        for p, row in main_rows.items():
            if not row.c and p in oc_main:
                row.c = oc_main[p]

        print("INFO: intl lists (hub crawl)", file=sys.stderr)
        int_entries = build_int_rows_from_wikidot(self.session, cfg)

        print("INFO: tales — foundation-tales-jp", file=sys.stderr)
        tales_html = fetch_html(self.session, cfg.abs_url(cfg.foundation_tales_path))
        tale_entries = parse_foundation_tales_jp(tales_html, cfg)

        print("INFO: gois — tag goi-format", file=sys.stderr)
        goi_entries = parse_goi_tag_pages(self.session, cfg)

        man_jp = os.path.join(cfg.output_dir, "manifest_scp-jp.json")
        man_main = os.path.join(cfg.output_dir, "manifest_scp-main.json")
        man_int = os.path.join(cfg.output_dir, "manifest_scp-int.json")
        man_tales = os.path.join(cfg.output_dir, "manifest_tales.json")
        man_gois = os.path.join(cfg.output_dir, "manifest_gois.json")

        ej, mj = trifold_rows_to_manifest_parts(jp_rows)
        write_manifest(man_jp, ej, mj)
        em, mm = trifold_rows_to_manifest_parts(main_rows)
        write_manifest(man_main, em, mm)
        write_manifest(man_int, int_entries, {})

        tl, tm = tales_raw_to_manifest_parts(tale_entries)
        write_manifest(man_tales, tl, tm)
        gl, gm = goi_raw_to_manifest_parts(goi_entries)
        write_manifest(man_gois, gl, gm)

        print(
            f"OK: wrote {man_jp}, {man_main}, {man_int}, {man_tales}, {man_gois}",
            file=sys.stderr,
        )


def main() -> int:
    p = argparse.ArgumentParser(description="Hybrid harvester for list/jp/*.json")
    args = p.parse_args()
    try:
        JapaneseBranchHarvester().run()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
