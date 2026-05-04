"""Wikidot サイトマップ (`/sitemap.xml`) loader / parser。

Wikidot は sitemap-index 形式でサイトマップを公開している:
- `/sitemap.xml` — sitemap-index。`<sitemap><loc>` で `sitemap_<kind>_<n>.xml` を列挙
- `sitemap_page_*.xml` — 通常ページ（記事・ハブ・著者ページ等）
- `sitemap_thread_*.xml` / `sitemap_category_*.xml` — フォーラム関連（既定では除外）

各 sub-sitemap には `<url><loc>` + `<lastmod>` (ISO 8601) が含まれており、これを
`{path: lastmod_unix}` の dict に変換して返すことで、harvester の本文フェッチを
「前回 lu と一致 → スキップ」する lastmod-skip 判定に使う。
"""
from __future__ import annotations

import re
import sys
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

if TYPE_CHECKING:
    import requests


_SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
# `sitemap_<kind>_<n>.xml` の <kind> 部分を抽出する。
_SUB_SITEMAP_RE = re.compile(r"/sitemap_([a-z]+)_\d+\.xml$", re.I)


def _parse_iso8601_to_unix(s: str) -> int | None:
    """ISO 8601 文字列を unix epoch 秒に変換。失敗時は None。"""
    s = (s or "").strip()
    if not s:
        return None
    # Python 3.10 以前は "Z" 末尾に未対応のため "+00:00" に正規化。
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return int(dt.timestamp())


def _path_from_loc(loc: str, base_netloc: str) -> str | None:
    """`<loc>` の URL からホスト一致を確認した上で path 部分を返す。"""
    raw = (loc or "").strip()
    if not raw:
        return None
    pu = urlparse(raw)
    if pu.netloc and pu.netloc.lower() != base_netloc.lower():
        return None
    return pu.path or None


def _fetch_xml_root(session: "requests.Session", url: str) -> ET.Element | None:
    """サイトマップ XML を fetch して root Element を返す。失敗時は None。"""
    try:
        # サイトマップは harvester の sleep_delay と独立。size が大きい (~1MB) ので timeout 余裕めに。
        resp = session.get(url, timeout=(15, 90))
        resp.raise_for_status()
    except Exception as ex:
        print(f"WARN: sitemap fetch failed: {url} ({ex})", file=sys.stderr)
        return None
    try:
        return ET.fromstring(resp.content)
    except ET.ParseError as ex:
        print(f"WARN: sitemap XML parse failed: {url} ({ex})", file=sys.stderr)
        return None


def _iter_sub_sitemap_urls(index_root: ET.Element) -> list[tuple[str, str]]:
    """sitemap-index の `<sitemap><loc>` を `(kind, url)` のリストで返す。"""
    out: list[tuple[str, str]] = []
    for sm in index_root.findall("sm:sitemap", _SITEMAP_NS):
        loc = sm.find("sm:loc", _SITEMAP_NS)
        if loc is None or not (loc.text or "").strip():
            continue
        url = loc.text.strip()
        m = _SUB_SITEMAP_RE.search(url)
        kind = m.group(1).lower() if m else "unknown"
        out.append((kind, url))
    return out


def load_wikidot_sitemap_lastmod(
    session: "requests.Session",
    base_host: str,
    *,
    include_kinds: tuple[str, ...] = ("page",),
) -> dict[str, int]:
    """`base_host/sitemap.xml` から `{path: lastmod_unix}` を返す。

    - `sitemap.xml` は sitemap-index 形式。`<sitemap><loc>` を辿り `sitemap_<kind>_*.xml` を取得。
    - `include_kinds` に含まれる種別のみ取得（既定は `("page",)`、forum/category 等は除外）。
    - `<url><loc>` のホスト一致を確認、`<lastmod>` を unix epoch にパース。
    - パースに失敗した URL は静かに無視。失敗時のフォールバック用に空 dict を返す。
    """
    base = base_host.rstrip("/")
    base_netloc = urlparse(base).netloc
    index_url = base + "/sitemap.xml"
    root = _fetch_xml_root(session, index_url)
    if root is None:
        return {}

    sub_urls = _iter_sub_sitemap_urls(root)
    if not sub_urls:
        print(f"WARN: sitemap index empty: {index_url}", file=sys.stderr)
        return {}

    include = {k.lower() for k in include_kinds}
    out: dict[str, int] = {}
    for kind, url in sub_urls:
        if kind not in include:
            continue
        sub_root = _fetch_xml_root(session, url)
        if sub_root is None:
            continue
        for u in sub_root.findall("sm:url", _SITEMAP_NS):
            loc_node = u.find("sm:loc", _SITEMAP_NS)
            lm_node = u.find("sm:lastmod", _SITEMAP_NS)
            if loc_node is None or lm_node is None:
                continue
            path = _path_from_loc(loc_node.text or "", base_netloc)
            if not path:
                continue
            ts = _parse_iso8601_to_unix(lm_node.text or "")
            if ts is None:
                continue
            # 同一 path が複数 sitemap に出現した場合は最大値（最新）を採用。
            prev = out.get(path)
            if prev is None or ts > prev:
                out[path] = ts
    return out


if __name__ == "__main__":
    # 簡易プローブ用。
    import argparse

    import requests

    p = argparse.ArgumentParser(description="Probe Wikidot sitemap and print summary.")
    p.add_argument("--base", default="http://scp-jp.wikidot.com")
    p.add_argument("--limit", type=int, default=5, help="先頭何件をプレビュー表示するか")
    args = p.parse_args()
    sess = requests.Session()
    sess.headers["User-Agent"] = "ScpDocsHarvester-SitemapProbe/1.0"
    lm = load_wikidot_sitemap_lastmod(sess, args.base)
    print(f"loaded {len(lm)} entries from {args.base}/sitemap.xml")
    for i, (k, v) in enumerate(sorted(lm.items())[: args.limit]):
        print(f"  {k} -> {v}")
