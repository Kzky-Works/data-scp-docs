#!/usr/bin/env python3
"""
SCP-JP Wikidot: タグ名の収集と、各タグ付き記事スラッグの逆引き用マップを生成する。

前提:
  - `https://scp-jp.wikidot.com/system:page-tags/tag/jp/p/1` … `p/59` の各ページ HTML から
    `href="/system:page-tags/tag/...` をすべて列挙し、重複排除して「jp 名前空間付近のタグ一覧」を得る。
    （メインの `tmp-pagesbytag` 内 `list-pages-item` は当該 URL では「jp」タグ付きページ一覧のため、
    タグ名の網羅にはページ全体のリンク走査が必要な場合がある。）
  - 各タグ `T` について `https://scp-jp.wikidot.com/system:page-tags/tag/{T}` を取得し、
    `id="main-content"` 以降の `tmp-pagesbytag` ブロック内の
    `<div class="list-pages-item"><p><a href="/...">` から記事パスを抽出する。
    ページネーションがあるタグは `/p/2` … を辿る。

出力 JSON（stdout）スキーマ例::
    {
      "source": "scp-jp.wikidot.com/system:page-tags",
      "tagPageRange": [1, 59],
      "tags": ["safe", "人型", ...],
      "articles": {
        "scp-173-jp": ["safe", "人型", ...],
        "scp-001": ["euclid", ...]
      }
    }

CI では、生成先の慣例として **`list/jp/jp_tag.json`** に書き出す（`-o list/jp/jp_tag.json`）。

`docs/catalog/scp_jp.json` 等へマージする際は、スラッグ正規化（-jp 付き）を
既存パイプラインに合わせて調整すること。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict

BASE = "http://scp-jp.wikidot.com"


def fetch(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "data-scp-docs-tag-harvester/1.0 (+https://github.com/Kzky-Works/data-scp-docs)"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def extract_tag_hrefs(html: str) -> set[str]:
    out: set[str] = set()
    for m in re.finditer(r'href="/system:page-tags/tag/([^"#?]+)"', html):
        slug = urllib.parse.unquote(m.group(1))
        if slug.lower().startswith("jp/p/"):
            continue
        if slug.lower() in {"jp"}:
            continue
        out.add(slug)
    return out


def main_content_tmp_pagesbytag_block(html: str) -> str | None:
    idx = html.find('id="main-content"')
    if idx == -1:
        body = html
    else:
        body = html[idx:]
    m = re.search(
        r'<div class="tmp-pagesbytag">(.*?)</div>\s*</div>\s*<div class="pages-tag-cloud-box"',
        body,
        re.S,
    )
    return m.group(1) if m else None


def list_article_paths_from_tag_page(html: str) -> list[str]:
    block = main_content_tmp_pagesbytag_block(html)
    if not block:
        return []
    paths: list[str] = []
    for href in re.findall(
        r'<div class="list-pages-item">\s*<p><a href="(/[^"]+)">', block
    ):
        if href.startswith("/system:page-tags/"):
            continue
        paths.append(href)
    return paths


def paginated_tag_urls(tag_slug: str) -> list[str]:
    """Return [url_p1, url_p2, ...] for a tag listing."""
    enc = urllib.parse.quote(tag_slug, safe="")
    first = f"{BASE}/system:page-tags/tag/{enc}"
    urls = [first]
    # Detect further pages from first fetch
    try:
        html = fetch(first)
    except Exception:
        return urls
    for m in re.finditer(
        r'href="(/system:page-tags/tag/%s/p/(\d+))"' % re.escape(enc),
        html,
    ):
        p = int(m.group(2))
        if p >= 2:
            urls.append(BASE + m.group(1))
    # Dedupe preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def harvest(args: argparse.Namespace) -> dict:
    all_tags: set[str] = set()
    for p in range(args.jp_tag_hub_page_min, args.jp_tag_hub_page_max + 1):
        url = f"{BASE}/system:page-tags/tag/jp/p/{p}"
        try:
            html = fetch(url)
        except Exception as e:
            print(f"warn: failed {url}: {e}", file=sys.stderr)
            continue
        all_tags |= extract_tag_hrefs(html)
        time.sleep(args.sleep_sec)

    article_tags: dict[str, set[str]] = defaultdict(set)
    tag_list = sorted(all_tags)
    if args.max_tags:
        tag_list = tag_list[: args.max_tags]

    for i, tag in enumerate(tag_list):
        try:
            pages = paginated_tag_urls(tag)
            for page_url in pages:
                html = fetch(page_url)
                for path in list_article_paths_from_tag_page(html):
                    slug = path.strip("/").split("/")[-1]
                    if not slug:
                        continue
                    article_tags[slug].add(tag)
                time.sleep(args.sleep_sec)
        except Exception as e:
            print(f"warn: tag {tag!r}: {e}", file=sys.stderr)
        if (i + 1) % 50 == 0:
            print(f"... processed {i + 1}/{len(tag_list)} tags", file=sys.stderr)

    return {
        "source": "scp-jp.wikidot.com/system:page-tags",
        "tagPageRange": [args.jp_tag_hub_page_min, args.jp_tag_hub_page_max],
        "tags": tag_list,
        "articles": {k: sorted(v) for k, v in sorted(article_tags.items())},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jp-tag-hub-page-min", type=int, default=1)
    ap.add_argument("--jp-tag-hub-page-max", type=int, default=59)
    ap.add_argument("--sleep-sec", type=float, default=0.35, help=" politeness delay between HTTP requests ")
    ap.add_argument("--max-tags", type=int, default=0, help="if >0, only first N tags after sort (debug)")
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default="",
        help="write JSON file instead of stdout (e.g. list/jp/jp_tag.json)",
    )
    args = ap.parse_args()
    data = harvest(args)
    text = json.dumps(data, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
