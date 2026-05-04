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
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from wikidot_utils import parse_odate_unix, wikidot_tag_list_total_pages

BASE = "http://scp-jp.wikidot.com"
JP_TAG_SCHEMA_VERSION = 1
PROGRESS_SCHEMA_VERSION = 1
RATE_LIMIT_STATUSES = (429, 503)
TAG_TAXONOMY_SOURCE = "scp-jp.wikidot.com/tag-list"
TAG_TAXONOMY_PATH = "/tag-list"
TAG_TAXONOMY_CATEGORIES = (
    "メジャー",
    "オブジェクトクラス",
    "アトリビュート",
    "GoIフォーマット",
    "コンテンツマーカー",
    "支部",
    "ジャンル",
    "ウィキ運営用",
)
TAG_TAXONOMY_KNOWN_TAGS = ("scp", "safe", "euclid")


class RateLimitedError(Exception):
    """Raised when the server rate-limits us; the harvester treats this as a stop signal."""

    def __init__(self, tag: str, page: int, status: int):
        super().__init__(f"rate limited (HTTP {status}) at tag {tag!r} page {page}")
        self.tag = tag
        self.page = page
        self.status = status


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


def iter_paginated_tag_pages(
    tag_slug: str,
    *,
    max_pages: int,
    stop_on_rate_limit: bool = True,
) -> list[tuple[str, str]]:
    """タグ一覧の URL + HTML をページ 1 … N まで返す。1ページ目は再取得しない。

    `stop_on_rate_limit=True` のとき HTTP 503/429 を `RateLimitedError` として送出し、
    呼び出し側が即時にループを停止できるようにする。
    """
    enc = urllib.parse.quote(tag_slug, safe="")
    first = f"{BASE}/system:page-tags/tag/{enc}"
    try:
        first_html = fetch(first)
    except urllib.error.HTTPError as e:
        if stop_on_rate_limit and e.code in RATE_LIMIT_STATUSES:
            raise RateLimitedError(tag_slug, 1, e.code) from e
        print(f"warn: tag {tag_slug!r} page 1: {e}", file=sys.stderr)
        return []
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
        except urllib.error.HTTPError as e:
            if stop_on_rate_limit and e.code in RATE_LIMIT_STATUSES:
                raise RateLimitedError(tag_slug, page, e.code) from e
            print(f"warn: tag {tag_slug!r} page {page}: {e}", file=sys.stderr)
            break
        except Exception as e:
            print(f"warn: tag {tag_slug!r} page {page}: {e}", file=sys.stderr)
            break
    return out


def load_previous_output(path: str) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"warn: failed to read previous output {path}: {e}", file=sys.stderr)
        return {}


def taxonomy_fields_from_previous(previous: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    taxonomy = previous.get("tagTaxonomy")
    if isinstance(taxonomy, list) and taxonomy:
        fields["tagTaxonomy"] = taxonomy
    source = previous.get("tagTaxonomySource")
    if isinstance(source, str) and source.strip():
        fields["tagTaxonomySource"] = source.strip()
    updated = previous.get("tagTaxonomyUpdatedAt")
    if isinstance(updated, int):
        fields["tagTaxonomyUpdatedAt"] = updated
    return fields


def collapse_ws(text: str) -> str:
    return " ".join(text.split())


def tag_slug_from_page_tags_href(href: str) -> str | None:
    marker = "/system:page-tags/tag/"
    if marker not in href:
        return None
    part = href.split(marker, 1)[1].split("#", 1)[0].split("?", 1)[0]
    if not part:
        return None
    return urllib.parse.unquote(part).strip()


def iter_tabset_panels(tabset) -> list[tuple[str, Any]]:
    nav = tabset.find("ul", class_="yui-nav", recursive=False)
    content = tabset.find("div", class_="yui-content", recursive=False)
    if nav is None or content is None:
        return []
    labels: list[str] = []
    for li in nav.find_all("li", recursive=False):
        em = li.find("em")
        labels.append(collapse_ws(em.get_text(" ", strip=True) if em else li.get_text(" ", strip=True)))
    panels = [
        div
        for div in content.find_all("div", recursive=False)
        if str(div.get("id") or "").startswith("wiki-tab-")
    ]
    return [(label, panel) for label, panel in zip(labels, panels) if label]


def find_main_tag_taxonomy_tabset(root) -> Any | None:
    wanted = set(TAG_TAXONOMY_CATEGORIES)
    best = None
    best_score = 0
    for tabset in root.find_all("div", class_="yui-navset"):
        labels = {label for label, _panel in iter_tabset_panels(tabset)}
        score = len(labels & wanted)
        if score > best_score:
            best = tabset
            best_score = score
    return best if best_score >= 4 else None


def tags_from_li(li, allowed_tags: set[str], seen: set[str]) -> list[str]:
    out: list[str] = []
    for a in li.find_all("a", href=True):
        tag = tag_slug_from_page_tags_href(str(a.get("href") or ""))
        if not tag or tag not in allowed_tags or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out


def collect_tag_taxonomy_groups(
    container,
    *,
    default_subcategory: str,
    allowed_tags: set[str],
    seen: set[str],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = {}
    current_subcategory = default_subcategory

    def add_tags(subcategory: str, tags: list[str]) -> None:
        if not tags:
            return
        grouped.setdefault(subcategory, []).extend(tags)

    def walk(node, subcategory: str) -> str:
        current = subcategory
        for child in node.find_all(recursive=False):
            name = getattr(child, "name", None)
            if name in {"h2", "h3", "h4"}:
                text = collapse_ws(child.get_text(" ", strip=True))
                if text:
                    current = text
                continue
            if name == "div" and "yui-navset" in (child.get("class") or []):
                for label, panel in iter_tabset_panels(child):
                    if label == "最小化":
                        continue
                    walk(panel, label)
                continue
            if name == "li":
                add_tags(current, tags_from_li(child, allowed_tags, seen))
            current = walk(child, current)
        return current

    walk(container, current_subcategory)
    return [
        {"subcategory": subcategory, "tags": tags}
        for subcategory, tags in grouped.items()
        if tags
    ]


def validate_tag_taxonomy(taxonomy: list[dict[str, Any]], allowed_tags: set[str]) -> bool:
    if not taxonomy:
        return False
    flat = {
        tag
        for group in taxonomy
        for tag in group.get("tags", [])
        if isinstance(tag, str)
    }
    if not flat:
        return False
    categories = {
        group.get("category")
        for group in taxonomy
        if isinstance(group.get("category"), str)
    }
    if len(allowed_tags) >= 100 and len(categories) < 4:
        return False
    for known in TAG_TAXONOMY_KNOWN_TAGS:
        if known in allowed_tags and known not in flat:
            return False
    if len(allowed_tags) >= 100 and len(flat) < 25:
        return False
    return True


def parse_tag_taxonomy_html(html: str, allowed_tags: set[str]) -> tuple[list[dict[str, Any]], int | None]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#page-content") or soup
    updated_at = parse_odate_unix(soup.select_one("#page-info span.odate"))
    tabset = find_main_tag_taxonomy_tabset(root)
    if tabset is None:
        return [], updated_at

    taxonomy: list[dict[str, Any]] = []
    seen: set[str] = set()
    wanted = set(TAG_TAXONOMY_CATEGORIES)
    for category, panel in iter_tabset_panels(tabset):
        if category not in wanted:
            continue
        for group in collect_tag_taxonomy_groups(
            panel,
            default_subcategory=category,
            allowed_tags=allowed_tags,
            seen=seen,
        ):
            taxonomy.append(
                {
                    "category": category,
                    "subcategory": group["subcategory"],
                    "tags": group["tags"],
                }
            )
    return taxonomy, updated_at


def build_tag_taxonomy_fields(args: argparse.Namespace, allowed_tags: set[str]) -> dict[str, Any]:
    previous = load_previous_output(args.output)
    previous_fields = taxonomy_fields_from_previous(previous)
    if getattr(args, "skip_tag_taxonomy", False):
        return previous_fields

    try:
        if args.tag_list_html_file:
            html = Path(args.tag_list_html_file).read_text(encoding="utf-8")
        else:
            html = fetch(BASE + TAG_TAXONOMY_PATH)
        taxonomy, updated_at = parse_tag_taxonomy_html(html, allowed_tags)
        if not validate_tag_taxonomy(taxonomy, allowed_tags):
            print("warn: tag taxonomy extraction failed validation; keeping previous taxonomy", file=sys.stderr)
            return previous_fields
        fields: dict[str, Any] = {
            "tagTaxonomySource": TAG_TAXONOMY_SOURCE,
            "tagTaxonomy": taxonomy,
        }
        if updated_at is not None:
            fields["tagTaxonomyUpdatedAt"] = updated_at
        print(
            f"OK: extracted tag taxonomy ({len(taxonomy)} groups, "
            f"{sum(len(g['tags']) for g in taxonomy)} tags)",
            file=sys.stderr,
        )
        return fields
    except Exception as e:
        print(f"warn: failed to extract tag taxonomy: {e}; keeping previous taxonomy", file=sys.stderr)
        return previous_fields


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
    comparable_keys = (
        "source",
        "tagPageRange",
        "tags",
        "articles",
        "tagTaxonomySource",
        "tagTaxonomyUpdatedAt",
        "tagTaxonomy",
    )
    if all(old.get(k) == data.get(k) for k in comparable_keys):
        return old_lv, gen
    return old_lv + 1, gen


def discover_jp_tags(args: argparse.Namespace) -> list[str]:
    """`/system:page-tags/tag/jp/p/1..N` を巡回してタグスラッグ一覧をソートして返す。"""
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
    return sorted(all_tags)


def load_progress(path: str) -> dict | None:
    if not path:
        return None
    p = Path(path)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"warn: failed to read progress file {path}: {e}", file=sys.stderr)
        return None


def save_progress(path: str, state: dict) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def harvest(args: argparse.Namespace) -> tuple[dict, dict]:
    """Run one batch of harvesting and return (output_data, progress_state)."""
    state = None if args.reset_cycle else load_progress(args.state_file)

    range_pair = [args.jp_tag_hub_page_min, args.jp_tag_hub_page_max]
    needs_new_cycle = (
        state is None
        or not isinstance(state.get("cycleTags"), list)
        or not state["cycleTags"]
        or int(state.get("nextIndex") or 0) >= len(state["cycleTags"])
        or state.get("tagPageRange") != range_pair
    )

    tag_to_articles: dict[str, list[str]] = {}

    if needs_new_cycle:
        print("starting new cycle: discovering tags from hub pages...", file=sys.stderr)
        cycle_tags = discover_jp_tags(args)
        if not cycle_tags:
            print("error: no tags discovered from hub pages", file=sys.stderr)
            sys.exit(2)
        if args.max_tags:
            cycle_tags = cycle_tags[: args.max_tags]
        next_index = 0
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cycle_id = now_iso
        cycle_started_at = now_iso
        # Carry forward only the tags that still exist in the new cycle (clean stale entries).
        if state and isinstance(state.get("tagToArticles"), dict):
            cycle_set = set(cycle_tags)
            tag_to_articles = {
                t: list(v)
                for t, v in state["tagToArticles"].items()
                if t in cycle_set and isinstance(v, list)
            }
        print(
            f"new cycle: {len(cycle_tags)} tags, batch_size={args.batch_size}",
            file=sys.stderr,
        )
    else:
        assert state is not None
        cycle_tags = list(state["cycleTags"])
        next_index = int(state["nextIndex"])
        cycle_id = str(state.get("cycleId") or "")
        cycle_started_at = str(state.get("cycleStartedAt") or cycle_id)
        prev_map = state.get("tagToArticles") or {}
        if isinstance(prev_map, dict):
            tag_to_articles = {
                t: list(v) for t, v in prev_map.items() if isinstance(v, list)
            }
        print(
            f"resuming cycle {cycle_id}: {next_index}/{len(cycle_tags)} done",
            file=sys.stderr,
        )

    start = next_index
    end = min(start + args.batch_size, len(cycle_tags))
    slice_tags = cycle_tags[start:end]

    stop_reason = "batch_done"
    failed_tag: str | None = None
    processed = 0

    for i, tag in enumerate(slice_tags):
        try:
            pages = iter_paginated_tag_pages(
                tag,
                max_pages=args.max_tag_list_pages,
                stop_on_rate_limit=not args.no_stop_on_503,
            )
        except RateLimitedError as e:
            print(
                f"stop: rate limited (HTTP {e.status}) at tag {e.tag!r} page {e.page}",
                file=sys.stderr,
            )
            stop_reason = "rate_limited"
            failed_tag = e.tag
            break
        articles_for_tag: set[str] = set()
        for _page_url, html in pages:
            for path in list_article_paths_from_tag_page(html):
                slug = path.strip("/").split("/")[-1]
                if slug:
                    articles_for_tag.add(slug)
            time.sleep(args.sleep_sec)
        tag_to_articles[tag] = sorted(articles_for_tag)
        next_index = start + i + 1
        processed += 1
        if processed % 50 == 0:
            print(
                f"... processed {processed}/{len(slice_tags)} this run "
                f"(cycle: {next_index}/{len(cycle_tags)})",
                file=sys.stderr,
            )

    if stop_reason != "rate_limited":
        next_index = end

    if stop_reason != "rate_limited" and next_index >= len(cycle_tags):
        stop_reason = "cycle_done"

    # Build the article→tags inverted index from the canonical tagToArticles map.
    article_tags_inv: dict[str, set[str]] = defaultdict(set)
    for tag, slugs in tag_to_articles.items():
        for slug in slugs:
            article_tags_inv[slug].add(tag)

    data: dict[str, Any] = {
        "source": "scp-jp.wikidot.com/system:page-tags",
        "tagPageRange": range_pair,
        "tags": list(cycle_tags),
        "articles": {k: sorted(v) for k, v in sorted(article_tags_inv.items())},
    }
    if stop_reason == "cycle_done":
        data.update(build_tag_taxonomy_fields(args, set(cycle_tags)))
    else:
        # Partial runs keep the prior taxonomy so the old listVersion never points at a new taxonomy.
        data.update(taxonomy_fields_from_previous(load_previous_output(args.output)))

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # listVersion: bump only on cycle_done. Otherwise keep prior value (or seed by timestamp).
    old_lv = 0
    if args.output:
        op = Path(args.output)
        if op.is_file():
            try:
                old_lv = int(json.loads(op.read_text(encoding="utf-8")).get("listVersion") or 0)
            except Exception:
                old_lv = 0
    if stop_reason == "cycle_done":
        lv, _ = next_list_version_and_generated_at(args.output, data)
    else:
        lv = old_lv if old_lv else int(datetime.now(timezone.utc).timestamp())

    output_data = {
        "listVersion": lv,
        "schemaVersion": JP_TAG_SCHEMA_VERSION,
        "generatedAt": now_iso,
        **data,
    }

    progress_state = {
        "schemaVersion": PROGRESS_SCHEMA_VERSION,
        "cycleId": cycle_id,
        "cycleStartedAt": cycle_started_at,
        "tagPageRange": range_pair,
        "cycleTags": cycle_tags,
        "nextIndex": next_index,
        "lastRunAt": now_iso,
        "lastRunStop": stop_reason,
        "lastRunFailedTag": failed_tag,
        "lastRunProcessed": processed,
        "tagToArticles": {t: list(v) for t, v in sorted(tag_to_articles.items())},
    }

    print(f"summary: processed {processed} tags this run", file=sys.stderr)
    print(f"cycle: {next_index}/{len(cycle_tags)}", file=sys.stderr)
    if stop_reason == "rate_limited":
        print(f"stopped: rate_limited at tag {failed_tag!r}", file=sys.stderr)
        print(
            f"resume: rerun the same command (next_index={next_index})",
            file=sys.stderr,
        )
    elif stop_reason == "batch_done":
        print(
            f"stopped: batch_done; next run resumes at index {next_index}",
            file=sys.stderr,
        )
    else:
        print(f"stopped: cycle_done (listVersion={lv})", file=sys.stderr)

    return output_data, progress_state


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jp-tag-hub-page-min", type=int, default=1)
    ap.add_argument("--jp-tag-hub-page-max", type=int, default=59)
    ap.add_argument(
        "--sleep-sec",
        type=float,
        default=0.5,
        help="politeness delay between HTTP requests (default 0.5)",
    )
    ap.add_argument(
        "--max-tag-list-pages",
        type=int,
        default=512,
        help="per-tag cap for tag listing pagination (pager 1/N の N がこれを超えたら打ち切り)",
    )
    ap.add_argument("--max-tags", type=int, default=0, help="if >0, only first N tags after sort (debug)")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="max number of tags to process in this run (default 250). "
        "Combine with daily cron to spread one cycle over ~1 week.",
    )
    ap.add_argument(
        "--state-file",
        type=str,
        default="list/jp/_jp_tag_progress.json",
        help="path to persistent progress state for cycle/resume",
    )
    ap.add_argument(
        "--reset-cycle",
        action="store_true",
        help="ignore existing progress and start a new cycle from scratch",
    )
    ap.add_argument(
        "--no-stop-on-503",
        action="store_true",
        help="legacy behavior: keep going on 503/429 instead of stopping early",
    )
    ap.add_argument(
        "--skip-tag-taxonomy",
        action="store_true",
        help="do not fetch /tag-list; carry forward existing tagTaxonomy fields if present",
    )
    ap.add_argument(
        "--tag-list-html-file",
        type=str,
        default="",
        help="read tag-list HTML from this fixture instead of fetching /tag-list (tests/debug)",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=str,
        default="",
        help="write JSON file instead of stdout (e.g. list/jp/jp_tag.json)",
    )
    ap.add_argument(
        "--output-compact",
        type=str,
        default="",
        help=(
            "write a compact bidirectional dictionary alongside (e.g. list/jp/jp_tag_compact.json). "
            "アプリ側の jp_tag 走査を不要にするため、`tagsToArticles` と `articleToTags` を整形済みで出力する。"
        ),
    )
    args = ap.parse_args()
    if args.max_tag_list_pages < 1:
        print("error: --max-tag-list-pages must be >= 1", file=sys.stderr)
        sys.exit(2)
    if args.batch_size < 1:
        print("error: --batch-size must be >= 1", file=sys.stderr)
        sys.exit(2)
    data, progress = harvest(args)
    text = json.dumps(data, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        sys.stdout.write(text)
    save_progress(args.state_file, progress)
    if args.output_compact:
        compact = build_compact_payload(data)
        with open(args.output_compact, "w", encoding="utf-8") as f:
            json.dump(compact, f, ensure_ascii=False, separators=(",", ":"))
            f.write("\n")
        print(
            "OK: wrote compact tag map "
            f"({len(compact['tagsToArticles'])} tags, {len(compact['articleToTags'])} articles)"
            f" -> {args.output_compact}",
            file=sys.stderr,
        )


def build_compact_payload(data: dict) -> dict[str, Any]:
    """`jp_tag.json` の `articles` 辞書を、アプリ用の整形済み双方向辞書に変換する。"""
    article_to_tags = data.get("articles") or {}
    if not isinstance(article_to_tags, dict):
        article_to_tags = {}
    tags_to_articles: defaultdict[str, list[str]] = defaultdict(list)
    for slug, tags in article_to_tags.items():
        if not isinstance(slug, str) or not isinstance(tags, list):
            continue
        for tag in tags:
            if isinstance(tag, str) and tag.strip():
                tags_to_articles[tag.strip()].append(slug)
    for tag, slugs in tags_to_articles.items():
        slugs.sort()
    return {
        "schemaVersion": 1,
        "listVersion": data.get("listVersion"),
        "generatedAt": data.get("generatedAt"),
        "tagsToArticles": dict(sorted(tags_to_articles.items())),
        "articleToTags": {k: list(v) for k, v in sorted(article_to_tags.items())},
    }


if __name__ == "__main__":
    main()
