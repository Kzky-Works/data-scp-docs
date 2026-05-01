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
    ページネーションはページャの「1 / N」から N を取り、`/p/2` … `/p/N` を**順にすべて**取得する
    （1ページ目に中間ページへの `href` が無いと記事が漏れるため、リンク列挙だけでは不十分）。

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

正本の CI（data-scp-docs）とローカルでは、生成先の慣例として **`list/jp/jp_tag.json`** に書き出す（`-o list/jp/jp_tag.json`）。

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wikidot_utils import wikidot_tag_list_total_pages

BASE = "http://scp-jp.wikidot.com"
JP_TAG_SCHEMA_VERSION = 1


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


def iter_paginated_tag_pages(tag_slug: str, *, max_pages: int) -> list[tuple[str, str]]:
    """タグ一覧の URL + HTML をページ 1 … N まで返す。1ページ目は再取得しない。"""
    enc = urllib.parse.quote(tag_slug, safe="")
    first = f"{BASE}/system:page-tags/tag/{enc}"
    try:
        first_html = fetch(first)
    except Exception as e:
        print(f"warn: tag {tag_slug!r} page 1: {e}", file=sys.stderr)
        return []
    total = wikidot_tag_list_total_pages(first_html)
    if total > max_pages:
        print(
            f"warn: tag {tag_slug!r}: pager reports {total} pages; capping at {max_pages}",
            file=sys.stderr,
        )
        total = max_pages
    out = [(first, first_html)]
    for page in range(2, total + 1):
        url = f"{BASE}/system:page-tags/tag/{enc}/p/{page}"
        try:
            out.append((url, fetch(url)))
        except Exception as e:
            print(f"warn: tag {tag_slug!r} page {page}: {e}", file=sys.stderr)
            break
    return out


def next_list_version_and_generated_at(output_path: str, data: dict[str, Any]) -> tuple[int, str]:
    """Keep listVersion stable when source/tag/articles payload is unchanged."""
    now = datetime.now(timezone.utc)
    gen = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    if not output_path:
        return int(now.timestamp()), gen
    p = Path(output_path)
    if not p.is_file():
        return int(now.timestamp()), gen
    try:
        old = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        old = {}
    old_lv = int(old.get("listVersion") or 0)
    comparable_keys = ("source", "tagPageRange", "tags", "articles")
    if all(old.get(k) == data.get(k) for k in comparable_keys):
        return old_lv, gen
    return old_lv + 1, gen


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
            pages = iter_paginated_tag_pages(tag, max_pages=args.max_tag_list_pages)
            for _page_url, html in pages:
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

    data: dict[str, Any] = {
        "source": "scp-jp.wikidot.com/system:page-tags",
        "tagPageRange": [args.jp_tag_hub_page_min, args.jp_tag_hub_page_max],
        "tags": tag_list,
        "articles": {k: sorted(v) for k, v in sorted(article_tags.items())},
    }
    lv, gen = next_list_version_and_generated_at(args.output, data)
    return {
        "listVersion": lv,
        "schemaVersion": JP_TAG_SCHEMA_VERSION,
        "generatedAt": gen,
        **data,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jp-tag-hub-page-min", type=int, default=1)
    ap.add_argument("--jp-tag-hub-page-max", type=int, default=59)
    ap.add_argument("--sleep-sec", type=float, default=0.35, help=" politeness delay between HTTP requests ")
    ap.add_argument(
        "--max-tag-list-pages",
        type=int,
        default=512,
        help="per-tag cap for tag listing pagination (pager 1/N の N がこれを超えたら打ち切り)",
    )
    ap.add_argument("--max-tags", type=int, default=0, help="if >0, only first N tags after sort (debug)")
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default="",
        help="write JSON file instead of stdout (e.g. list/jp/jp_tag.json)",
    )
    args = ap.parse_args()
    if args.max_tag_list_pages < 1:
        print("error: --max-tag-list-pages must be >= 1", file=sys.stderr)
        sys.exit(2)
    data = harvest(args)
    text = json.dumps(data, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
