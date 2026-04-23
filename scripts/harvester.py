#!/usr/bin/env python3
"""
JP 支部向けハイブリッド索引: シリーズ一覧（基礎層）＋ page-tags（属性層）＋
foundation-tales-jp（著者層）を統合し list/jp/*.json を生成する。

将来の支部追加: BranchConfig の site_host / output_dir / URL 定義を差し替える。
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
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

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


def load_scp_list_merge(path: str) -> dict[tuple[int, int], dict[str, Any]]:
    """docs/scp_list.json があれば (series, n) → メタ。"""
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    entries = data.get("entries")
    if not isinstance(entries, list):
        return {}
    m: dict[tuple[int, int], dict[str, Any]] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        try:
            s = int(e["series"])
            n = int(e["scpNumber"])
        except (KeyError, TypeError, ValueError):
            continue
        m[(s, n)] = e
    return m


def series_num_from_jp_path(path: str) -> tuple[int, int] | None:
    m = SCP_JP_HREF.match(path)
    if not m:
        return None
    n = int(m.group(1), 10)
    for s in range(5):
        r = range_for_series(s)
        if r.lo <= n <= r.hi:
            return s, n
    return None


def series_num_from_main_path(path: str) -> tuple[int, int] | None:
    m = SCP_MAIN_HREF.match(path)
    if not m:
        return None
    n = int(m.group(1), 10)
    for s in range(5):
        r = range_for_series(s)
        if r.lo <= n <= r.hi:
            return s, n
    return None


def apply_scp_list_merge(rows: dict[str, ArticleRow], merge: dict[tuple[int, int], dict[str, Any]], *, jp: bool) -> None:
    for path, row in rows.items():
        key = series_num_from_jp_path(path) if jp else series_num_from_main_path(path)
        if key is None:
            continue
        e = merge.get(key)
        if not e:
            continue
        if jp:
            mt = e.get("mainlistTranslationTitle")
            if isinstance(mt, str) and mt.strip():
                row.o = mt.strip()
        else:
            tit = e.get("title")
            if isinstance(tit, str) and tit.strip():
                row.o = tit.strip()
        oc = e.get("objectClass")
        if isinstance(oc, str) and oc.strip():
            row.c = oc.strip()
        tg = e.get("tags")
        if isinstance(tg, list) and tg:
            row.g = [str(x).strip() for x in tg if isinstance(x, str) and str(x).strip()]


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


def load_hub_paths_from_scp_list(path: str) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    hub = data.get("hubLinkedPaths")
    if not isinstance(hub, list):
        return []
    out: list[str] = []
    for p in hub:
        if isinstance(p, str) and INTL_PATH_RE.match(p) and not p.endswith("-jp"):
            out.append(p)
    out.sort()
    return out


def build_int_rows(session: requests.Session, cfg: BranchConfig, scp_list_path: str) -> list[dict[str, Any]]:
    hub_paths = load_hub_paths_from_scp_list(scp_list_path)
    titles = crawl_intl_titles(session, cfg)
    base = cfg.site_host.rstrip("/")
    rows: list[dict[str, Any]] = []
    if not hub_paths:
        print("WARN: hubLinkedPaths empty; scp-int.json will be empty", file=sys.stderr)
        return rows
    for p in hub_paths:
        i = p.lstrip("/").lower()
        u = base + p
        t = titles.get(p) or _fallback_int_title(p)
        e: dict[str, Any] = {"u": u, "i": i, "t": t}
        rows.append(e)
    return rows


def _fallback_int_title(path: str) -> str:
    m = re.match(r"^/scp-(\d+)-([a-z]{2})$", path)
    if not m:
        return path.lstrip("/").upper()
    return f"SCP-{m.group(1)}-{m.group(2).upper()}"


def article_entry_dict(row: ArticleRow) -> dict[str, Any]:
    d: dict[str, Any] = {"u": row.u, "i": row.i, "t": row.t}
    if row.c:
        d["c"] = row.c
    if row.o:
        d["o"] = row.o
    if row.g:
        d["g"] = list(row.g)
    if row.a:
        d["a"] = row.a
    return d


def now_payload_meta() -> tuple[int, str]:
    dt = datetime.now(timezone.utc)
    return int(dt.timestamp()), dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_article_list(path: str, entries: list[dict[str, Any]]) -> None:
    lv, gen = now_payload_meta()
    payload = {
        "listVersion": lv,
        "schemaVersion": 1,
        "generatedAt": gen,
        "entries": entries,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def write_general_list(path: str, entries: list[dict[str, Any]]) -> None:
    lv, gen = now_payload_meta()
    payload = {
        "listVersion": lv,
        "schemaVersion": 1,
        "generatedAt": gen,
        "entries": entries,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


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

    def run(self, *, scp_list_path: str) -> None:
        cfg = self.cfg
        os.makedirs(cfg.output_dir, exist_ok=True)

        merge = load_scp_list_merge(scp_list_path)

        print("INFO: base layer — JP series", file=sys.stderr)
        jp_rows = scrape_series_jp(self.session, cfg)
        apply_scp_list_merge(jp_rows, merge, jp=True)

        print("INFO: base layer — mainlist", file=sys.stderr)
        main_rows = scrape_series_main(self.session, cfg)
        apply_scp_list_merge(main_rows, merge, jp=False)

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

        print("INFO: intl hub", file=sys.stderr)
        int_entries = build_int_rows(self.session, cfg, scp_list_path)

        print("INFO: tales — foundation-tales-jp", file=sys.stderr)
        tales_html = fetch_html(self.session, cfg.abs_url(cfg.foundation_tales_path))
        tale_entries = parse_foundation_tales_jp(tales_html, cfg)

        print("INFO: gois — tag goi-format", file=sys.stderr)
        goi_entries = parse_goi_tag_pages(self.session, cfg)

        out_jp = os.path.join(cfg.output_dir, "scp-jp.json")
        out_main = os.path.join(cfg.output_dir, "scp.json")
        out_int = os.path.join(cfg.output_dir, "scp-int.json")
        out_tales = os.path.join(cfg.output_dir, "tales.json")
        out_gois = os.path.join(cfg.output_dir, "gois.json")

        write_article_list(out_jp, [article_entry_dict(r) for _, r in sorted(jp_rows.items())])
        write_article_list(out_main, [article_entry_dict(r) for _, r in sorted(main_rows.items())])
        write_article_list(out_int, int_entries)
        write_general_list(out_tales, tale_entries)
        write_general_list(out_gois, goi_entries)

        print(f"OK: wrote {out_jp}, {out_main}, {out_int}, {out_tales}, {out_gois}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description="Hybrid harvester for list/jp/*.json")
    p.add_argument(
        "--scp-list",
        default=os.path.join(REPO_ROOT, "docs", "scp_list.json"),
        help="Optional scp_list.json for merge + hubLinkedPaths",
    )
    args = p.parse_args()
    try:
        JapaneseBranchHarvester().run(scp_list_path=args.scp_list)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
