"""Wikidot system:page-tags の HTML 取得・一覧抽出（標準ライブラリのみ）。"""

from __future__ import annotations

import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


SITE_BASE = "https://scp-jp.wikidot.com"
PAGE_TAGS_INDEX = f"{SITE_BASE}/system:page-tags"


@dataclass(frozen=True)
class TagRef:
    path: str
    display_name: str


def fetch_html(url: str, user_agent: str, timeout: float = 60.0) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent, "Accept-Language": "ja,en;q=0.9"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def fetch_html_retry(url: str, user_agent: str, retries: int, sleep: float) -> str:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            return fetch_html(url, user_agent)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as e:
            last_err = e
            wait = sleep * (2**attempt) + random.uniform(0, 0.25)
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def extract_tag_refs_from_index(html: str) -> list[TagRef]:
    refs: dict[str, str] = {}
    pattern = re.compile(
        r'<a\s+class="tag"\s+href="(/system:page-tags/tag/[^"]+)"[^>]*>([\s\S]*?)</a>',
        re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        path = m.group(1).strip()
        inner = re.sub(r"<[^>]+>", "", m.group(2))
        inner = inner.replace("&nbsp;", " ").strip()
        inner = " ".join(inner.split())
        tail = path.rsplit("/", 1)[-1]
        decoded = urllib.parse.unquote(tail)
        label = inner if inner else decoded
        if path not in refs:
            refs[path] = label
    return [TagRef(path=k, display_name=v) for k, v in sorted(refs.items())]


def extract_main_tag_listing_segment(html: str) -> str:
    marker = '<div class="tmp-pagesbytag">'
    start = html.find(marker)
    if start < 0:
        return ""
    cloud = '<div class="pages-tag-cloud-box"'
    end = html.find(cloud, start)
    if end < 0:
        return html[start:]
    return html[start:end]


def extract_article_slugs_from_block(block: str) -> list[str]:
    slugs: list[str] = []
    for href_m in re.finditer(r'<p>\s*<a\s+href="/([^"]+)"', block, re.IGNORECASE):
        path = href_m.group(1).strip()
        if path.startswith("_") or "javascript:" in path.lower():
            continue
        seg = path.split("/")[0]
        slugs.append(seg)
    return slugs


def parse_total_pages(html: str) -> int:
    m = re.search(r"ページ\s+(\d+)\s+/\s+(\d+)", html)
    if m:
        return max(1, int(m.group(2)))
    return 1


def collect_slugs_for_tag(tag: TagRef, user_agent: str, sleep: float, retries: int) -> list[str]:
    out: list[str] = []
    first_url = SITE_BASE + tag.path
    first_html = fetch_html_retry(first_url, user_agent, retries, sleep)
    total = parse_total_pages(first_html)
    pages_html = [first_html]
    for n in range(2, total + 1):
        time.sleep(sleep)
        pages_html.append(
            fetch_html_retry(f"{SITE_BASE}{tag.path}/p/{n}", user_agent, retries, sleep)
        )

    for page_html in pages_html:
        segment = extract_main_tag_listing_segment(page_html)
        out.extend(extract_article_slugs_from_block(segment))
    return out
