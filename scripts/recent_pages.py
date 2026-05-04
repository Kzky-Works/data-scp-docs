"""SCP-JP の Wikidot 集約ページ (`/most-recently-created-jp` / `/most-recently-created-translated` /
`/most-recently-edited`) からのインクリメンタルハーベスト用パーサ。

これらは ListPages モジュール由来の単純な HTML テーブルで、`/p/N` 形式の HTML ページネーションを持つ。
``system:recent-changes`` は AJAX モジュールでしか追加ページを取れず重いため、`most-recently-edited`
（編集日時降順の同じ列構造）で代替する。

各ジェネレータは降順（新しい順）に列挙するので、呼び出し側は ``since_unix`` を超えた行が出てきたら
打ち切れる。Wikidot 側の合計ページ数は数百件規模になるが、毎日 / 毎時走らせる前提なら現実には
1〜数ページ読めば十分。ハードキャップ ``hard_cap_pages`` を超えた場合は ``incomplete=True`` を返し、
呼び出し側はそのカーソル前進を保留して次回の monthly full に頼る、もしくはレガシー daily へフォールバック
する想定。
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from wikidot_utils import parse_odate_unix, wikidot_tag_list_total_pages

if TYPE_CHECKING:
    import requests


RECENT_CREATED_JP_PATH = "/most-recently-created-jp"
RECENT_CREATED_TRANSLATED_PATH = "/most-recently-created-translated"
RECENT_EDITED_PATH = "/most-recently-edited"

# `harvester.fetch_html` が送るリトライと組み合わせて、1 ジェネレータあたり最大何ページ読むか。
# 数日分の差分を取るなら 1〜3 ページで十分。CI 障害復帰や初回シードの暴走防止のためのガード。
DEFAULT_HARD_CAP_PAGES = 50


@dataclass
class RecentEntry:
    """ListPages テーブル 1 行ぶんの抽出結果。

    ``ts_unix`` は most-recently-created-* では作成日時、most-recently-edited では最終更新日時。
    ``slug`` は URL パス（先頭 ``/`` 付き、例: ``/scp-3938-jp``）。
    """

    slug: str
    title: str
    ts_unix: int
    author: str | None = None


@dataclass
class IterResult:
    """ジェネレータの戻り値。

    - ``entries``: ``ts_unix > since_unix`` を満たす行を新しい順に格納。
    - ``incomplete``: ハードキャップに到達した時 True。呼び出し側はカーソル更新を見送る。
    """

    entries: list[RecentEntry]
    incomplete: bool = False


def _abs_listing_url(site_host: str, path: str, page: int) -> str:
    base = site_host.rstrip("/")
    if page <= 1:
        return base + path
    return base + path + f"/p/{page}"


def _row_to_entry(tr, base_host: str) -> RecentEntry | None:
    """`tr.listpages-table-body` 1 行を ``RecentEntry`` に変換。失敗時は None。"""
    a = tr.find("a", href=True)
    if a is None:
        return None
    href = (a.get("href") or "").strip()
    if not href:
        return None
    pu = urlparse(urljoin(base_host.rstrip("/") + "/", href))
    slug = pu.path
    if not slug or not slug.startswith("/"):
        return None
    title = (a.get_text(strip=True) or "").strip()
    if not title:
        return None
    odate = tr.find("span", class_="odate")
    ts = parse_odate_unix(odate)
    if ts is None or ts <= 0:
        return None
    author = None
    printuser = tr.find("span", class_="printuser")
    if printuser is not None:
        # 最後の <a> のテキストが表示名（最初は user-info アイコン）。
        anchors = [x for x in printuser.find_all("a") if (x.get_text(strip=True) or "").strip()]
        if anchors:
            author = (anchors[-1].get_text(strip=True) or "").strip() or None
    return RecentEntry(slug=slug, title=title, ts_unix=int(ts), author=author)


def _iter_listpages_path(
    session: "requests.Session",
    site_host: str,
    list_path: str,
    *,
    since_unix: int,
    fetch_html,
    hard_cap_pages: int = DEFAULT_HARD_CAP_PAGES,
) -> IterResult:
    """共通実装: 指定 ListPages パスを `/p/N` で 1..N と読み、``since_unix`` を超えた時点で停止。

    ``fetch_html`` は呼び出し側 (``harvester.fetch_html``) を渡す。throttle・リトライの一元化のため。
    """
    out: list[RecentEntry] = []
    seen_slugs: set[str] = set()
    total_pages: int | None = None
    page = 1
    while page <= hard_cap_pages:
        url = _abs_listing_url(site_host, list_path, page)
        try:
            html = fetch_html(session, url)
        except Exception as ex:  # noqa: BLE001 - log + abort cursor advance
            print(
                f"WARN: recent-pages fetch failed at {url}: {ex}. "
                "Marking iter incomplete; cursor will not advance.",
                file=sys.stderr,
            )
            return IterResult(entries=out, incomplete=True)
        soup = BeautifulSoup(html, "html.parser")
        if total_pages is None:
            total_pages = wikidot_tag_list_total_pages(html)
        rows = soup.select("tr.listpages-table-body")
        if not rows:
            break
        crossed = False
        for tr in rows:
            ent = _row_to_entry(tr, site_host)
            if ent is None:
                continue
            if ent.slug in seen_slugs:
                continue
            if ent.ts_unix <= since_unix:
                # 新しい順なのでここで全体を打ち切れる。
                crossed = True
                break
            seen_slugs.add(ent.slug)
            out.append(ent)
        if crossed:
            break
        if total_pages is not None and page >= total_pages:
            break
        page += 1
    incomplete = page > hard_cap_pages and not (
        total_pages is not None and total_pages <= hard_cap_pages
    )
    if incomplete:
        print(
            f"WARN: recent-pages hard-cap hit at {list_path} "
            f"(read {hard_cap_pages} pages, total={total_pages}). "
            "Cursor will not advance for this generator.",
            file=sys.stderr,
        )
    return IterResult(entries=out, incomplete=incomplete)


def iter_recently_created_jp(
    session: "requests.Session",
    site_host: str,
    *,
    since_unix: int,
    fetch_html,
    hard_cap_pages: int = DEFAULT_HARD_CAP_PAGES,
) -> IterResult:
    """新規作成された JP オリジナル記事の差分。"""
    return _iter_listpages_path(
        session,
        site_host,
        RECENT_CREATED_JP_PATH,
        since_unix=since_unix,
        fetch_html=fetch_html,
        hard_cap_pages=hard_cap_pages,
    )


def iter_recently_translated(
    session: "requests.Session",
    site_host: str,
    *,
    since_unix: int,
    fetch_html,
    hard_cap_pages: int = DEFAULT_HARD_CAP_PAGES,
) -> IterResult:
    """新規追加された翻訳記事の差分。"""
    return _iter_listpages_path(
        session,
        site_host,
        RECENT_CREATED_TRANSLATED_PATH,
        since_unix=since_unix,
        fetch_html=fetch_html,
        hard_cap_pages=hard_cap_pages,
    )


def iter_recently_edited(
    session: "requests.Session",
    site_host: str,
    *,
    since_unix: int,
    fetch_html,
    hard_cap_pages: int = DEFAULT_HARD_CAP_PAGES,
) -> IterResult:
    """直近編集された記事の差分（システムページ・author/fragment も混じるので呼び出し側で要フィルタ）。"""
    return _iter_listpages_path(
        session,
        site_host,
        RECENT_EDITED_PATH,
        since_unix=since_unix,
        fetch_html=fetch_html,
        hard_cap_pages=hard_cap_pages,
    )
