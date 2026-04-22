#!/usr/bin/env python3
"""
scp-jp.wikidot.com の system:page-tags を巡回し、タグ一覧ページから SCP 記事 URL を収集して
mergeKey（series_scpNumber）ごとのタグ集合を構築し、scp_list.json とマージした JSON を出力する。

前提: ScpDocs の SCPListRemotePayload / SCPJPSeries の番号レンジ（001〜4999）に合わせ、
      /scp-NNN / scp-NNN-jp / scp-NNN-j のスラッグのみを対象とする。

本スクリプトと GitHub Actions は data-scp-docs リポジトリが正（scp_list.json 関連の集約先）。
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


SITE_BASE = "https://scp-jp.wikidot.com"
PAGE_TAGS_INDEX = f"{SITE_BASE}/system:page-tags"
DEFAULT_UA = "ScpDocsTagCrawler/1.0 (+https://github.com/Kzky-Works/data-scp-docs)"


@dataclass(frozen=True)
class TagRef:
    """タグ一覧 URL パスと表示名（クラウドのリンクテキスト優先）。"""

    path: str  # e.g. /system:page-tags/tag/en
    display_name: str


def merge_key_for_scp_slug(slug: str) -> str | None:
    """
    Wikidot のページスラッグから JapanSCPListMetadataStore と同じ mergeKey を返す。
    対象外なら None。
    """
    lower = slug.strip().lower()
    if not lower.startswith("scp-"):
        return None
    rest = lower[4:]
    if rest.endswith("-jp"):
        rest = rest[:-3]
    elif rest.endswith("-j"):
        rest = rest[:-2]
    digits = "".join(c for c in rest if c.isdigit())
    if not digits:
        return None
    n = int(digits)
    if not (1 <= n <= 4999):
        return None
    if n <= 999:
        series = 0
    elif n <= 1999:
        series = 1
    elif n <= 2999:
        series = 2
    elif n <= 3999:
        series = 3
    else:
        series = 4
    return f"{series}_{n}"


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
    """メインページのタグクラウドから (path, display_name) を抽出。"""
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
    """
    タグ詳細ページ: tmp-pagesbytag 開始〜 tag-cloud 直前まで（ネストした </div> も含め正しく切る）。
    """
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
    """1 タグについてページネーションを辿り、一覧に出たページスラッグを返す。"""
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


def merge_tags_into_payload(
    payload: dict[str, Any],
    merge_key_tags: dict[str, set[str]],
    sync_time_iso: str,
) -> dict[str, Any]:
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("payload.entries が配列ではありません")

    new_entries: list[dict[str, Any]] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        mk = None
        series = e.get("series")
        sn = e.get("scpNumber")
        if isinstance(series, int) and isinstance(sn, int):
            mk = f"{series}_{sn}"
        ne = dict(e)
        existing = ne.get("tags")
        tag_set: set[str] = set()
        if isinstance(existing, list):
            for t in existing:
                if isinstance(t, str) and t.strip():
                    tag_set.add(t.strip())
        if mk and mk in merge_key_tags:
            tag_set |= merge_key_tags[mk]
        if tag_set:
            ne["tags"] = sorted(tag_set)
        else:
            ne.pop("tags", None)
        if mk and mk in merge_key_tags:
            ne["articleMetadataSyncedAt"] = sync_time_iso
        new_entries.append(ne)

    out = dict(payload)
    out["entries"] = new_entries
    return out


def load_json_file(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_json_url(url: str, user_agent: str) -> dict[str, Any]:
    raw = fetch_html_retry(url, user_agent, retries=4, sleep=0.5)
    return json.loads(raw)


def main() -> int:
    ap = argparse.ArgumentParser(description="SCP-JP Wikidot page-tags → scp_list.json タグマージ")
    ap.add_argument(
        "--base-json-url",
        default=os_env("SCP_LIST_JSON_URL", "https://kzky-works.github.io/data-scp-docs/scp_list.json"),
        help="マージ先の scp_list.json URL（--base-json-path 未指定時）",
    )
    ap.add_argument("--base-json-path", default="", help="ローカル JSON を優先するときのパス（指定時は URL を使わない）")
    ap.add_argument("--out", default="", help="出力 JSON パス（--dry-run-tags 時は任意）")
    ap.add_argument("--sleep", type=float, default=float(os_env("WIKIDOT_CRAWL_SLEEP_SECS", "0.35")), help="リクエスト間スリープ秒")
    ap.add_argument("--user-agent", default=os_env("WIKIDOT_CRAWL_USER_AGENT", DEFAULT_UA))
    ap.add_argument("--max-tags", type=int, default=0, help="デバッグ用: 処理するタグ数の上限（0 で無制限）")
    ap.add_argument("--shuffle-tags", action="store_true", help="タグ処理順をシャッフル（長期運用で偏り低減）")
    ap.add_argument("--dry-run-tags", action="store_true", help="タグ一覧取得のみで終了（クロールしない）")
    args = ap.parse_args()

    if not args.dry_run_tags and not args.out:
        ap.error("--out が必要です（または --dry-run-tags）")

    print(f"[info] base: {args.base_json_path or args.base_json_url}", file=sys.stderr)

    index_html = fetch_html_retry(PAGE_TAGS_INDEX, args.user_agent, retries=4, sleep=args.sleep)
    time.sleep(args.sleep)
    tag_refs = extract_tag_refs_from_index(index_html)
    print(f"[info] tags in cloud: {len(tag_refs)}", file=sys.stderr)

    if args.dry_run_tags:
        print(json.dumps([{"path": t.path, "display_name": t.display_name} for t in tag_refs], ensure_ascii=False, indent=2))
        return 0

    if args.shuffle_tags:
        random.shuffle(tag_refs)
    if args.max_tags > 0:
        tag_refs = tag_refs[: args.max_tags]

    merge_key_tags: dict[str, set[str]] = {}

    for i, tag in enumerate(tag_refs):
        label = tag.display_name
        try:
            slugs = collect_slugs_for_tag(tag, args.user_agent, args.sleep, retries=4)
        except Exception as e:
            print(f"[warn] tag skip {tag.path}: {e}", file=sys.stderr)
            continue
        finally:
            time.sleep(args.sleep)

        matched = 0
        for slug in slugs:
            mk = merge_key_for_scp_slug(slug)
            if mk is None:
                continue
            merge_key_tags.setdefault(mk, set()).add(label)
            matched += 1

        if (i + 1) % 25 == 0 or i == 0:
            print(
                f"[info] progress {i + 1}/{len(tag_refs)} tag={label!r} pages~ slugs={len(slugs)} scp_hits={matched}",
                file=sys.stderr,
            )

    if args.base_json_path:
        payload = load_json_file(args.base_json_path)
    else:
        payload = load_json_url(args.base_json_url, args.user_agent)

    sync_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    lv = payload.get("listVersion", 0)
    try:
        next_lv = int(lv) + 1
    except (TypeError, ValueError):
        next_lv = 1
    payload["listVersion"] = next_lv
    payload["generatedAt"] = sync_ts

    merged = merge_tags_into_payload(payload, merge_key_tags, sync_ts)

    out_path = args.out
    assert out_path
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(
        f"[done] wrote {out_path} listVersion={next_lv} mergeKeys_with_tags={len(merge_key_tags)}",
        file=sys.stderr,
    )
    return 0


def os_env(key: str, default: str) -> str:
    import os

    v = os.environ.get(key)
    return v if v is not None and v != "" else default


if __name__ == "__main__":
    raise SystemExit(main())
