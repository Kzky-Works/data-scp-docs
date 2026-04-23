"""3 系統アプリフィード生成スクリプト共通（validate / HTTP / 本家メイン一覧行の取得）。"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

REQUEST_DELAY_SEC = 2.5

HTTP_HEADERS = {
    "User-Agent": "ScpDocsAppFeedBot/1.0 (+https://github.com/Kzky-Works/data-scp-docs; contact: repo owner)",
    "Accept-Language": "ja,en;q=0.8",
}

SERIES_PAGES_MAINLIST: list[tuple[int, str]] = [
    (0, "https://scp-jp.wikidot.com/scp-series"),
    (1, "https://scp-jp.wikidot.com/scp-series-2"),
    (2, "https://scp-jp.wikidot.com/scp-series-3"),
    (3, "https://scp-jp.wikidot.com/scp-series-4"),
    (4, "https://scp-jp.wikidot.com/scp-series-5"),
]

SCP_MAINLIST_HREF_RE = re.compile(r"^/scp-(\d+)$")


@dataclass(frozen=True)
class SeriesRange:
    lo: int
    hi: int


def range_for_series(series: int) -> SeriesRange:
    ranges = {
        0: SeriesRange(1, 999),
        1: SeriesRange(1000, 1999),
        2: SeriesRange(2000, 2999),
        3: SeriesRange(3000, 3999),
        4: SeriesRange(4000, 4999),
    }
    return ranges[series]


def extract_title_from_li(li) -> str | None:
    full = li.get_text(separator="", strip=False).strip()
    if " - " not in full:
        return None
    _, title = full.split(" - ", 1)
    t = title.strip()
    return t if t else None


def fetch_html_with_retry(
    session: requests.Session,
    url: str,
    *,
    retries: int = 8,
    delay_sec: float | None = None,
    transient_status: tuple[int, ...] = (502, 503, 429),
) -> str:
    delay = REQUEST_DELAY_SEC if delay_sec is None else max(0.0, float(delay_sec))
    last_err: Exception | None = None
    for attempt in range(retries):
        time.sleep(delay)
        try:
            r = session.get(url, headers=HTTP_HEADERS, timeout=(15, 75))
            if r.status_code in transient_status and attempt < retries - 1:
                wait = min(180, 12 * (2**attempt))
                time.sleep(wait)
                continue
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(120, 8 * (attempt + 1)))
    assert last_err is not None
    raise last_err


def parse_scp_number_mainlist_href(href: str) -> int | None:
    raw = (href or "").strip()
    if not raw or raw.startswith("#"):
        return None
    path = urlparse(urljoin("https://scp-jp.wikidot.com/", raw)).path
    m = SCP_MAINLIST_HREF_RE.match(path)
    if not m:
        return None
    return int(m.group(1), 10)


def fetch_mainlist_rows(session: requests.Session) -> list[dict[str, Any]]:
    """本家メイン和訳一覧（scp-series …）の行。series / scpNumber / title。"""
    rows: list[dict[str, Any]] = []
    for series, list_url in SERIES_PAGES_MAINLIST:
        html = fetch_html_with_retry(session, list_url)
        soup = BeautifulSoup(html, "html.parser")
        rng = range_for_series(series)
        for li in soup.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "") or ""
            n = parse_scp_number_mainlist_href(href)
            if n is None or not (rng.lo <= n <= rng.hi):
                continue
            title = extract_title_from_li(li)
            if not title:
                continue
            rows.append({"series": series, "scpNumber": n, "title": title})
    rows.sort(key=lambda r: (int(r["series"]), int(r["scpNumber"])))
    return rows


def mainlist_article_path(scp_number: int) -> str:
    n = int(scp_number)
    return f"/scp-{n:03d}" if n < 1000 else f"/scp-{n}"


def mainlist_article_url(scp_number: int) -> str:
    return "https://scp-jp.wikidot.com" + mainlist_article_path(scp_number)


def mainlist_stable_id(scp_number: int) -> str:
    return mainlist_article_path(scp_number).lstrip("/")


def load_scp_list_entry_blobs(path: str) -> dict[tuple[int, int], dict[str, Any]]:
    """docs/scp_list.json の (series, scpNumber) → title / mainlistTranslationTitle / objectClass / tags。"""
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        print(f"WARN: could not load {path}: {e}", file=sys.stderr)
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return {}
    out: dict[tuple[int, int], dict[str, Any]] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        try:
            s = int(e["series"])
            n = int(e["scpNumber"])
        except (KeyError, TypeError, ValueError):
            continue
        blob: dict[str, Any] = {}
        for key in ("title", "mainlistTranslationTitle", "objectClass"):
            v = e.get(key)
            if isinstance(v, str) and v.strip():
                blob[key] = v.strip()
        tg = e.get("tags")
        if isinstance(tg, list) and tg and all(isinstance(t, str) and str(t).strip() for t in tg):
            blob["tags"] = [str(t).strip() for t in tg]
        if blob:
            out[(s, n)] = blob
    return out


def validate_app_feed_payload(payload: dict[str, Any]) -> None:
    if payload.get("schemaVersion") != 1:
        raise ValueError(f"schemaVersion must be 1, got {payload.get('schemaVersion')!r}")
    lv = payload.get("listVersion")
    if not isinstance(lv, int) or lv <= 0:
        raise ValueError(f"listVersion must be positive int, got {lv!r}")
    gen = payload.get("generatedAt")
    if not isinstance(gen, str) or not gen.strip():
        raise ValueError("generatedAt must be non-empty ISO8601 string")
    entries = payload.get("entries")
    if not isinstance(entries, list) or len(entries) == 0:
        raise ValueError("entries must be non-empty list")
    seen_i: set[str] = set()
    for idx, e in enumerate(entries):
        if not isinstance(e, dict):
            raise ValueError(f"entries[{idx}] is not an object")
        for req in ("u", "i", "t"):
            v = e.get(req)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"entries[{idx}].{req} invalid")
        if not str(e["u"]).lower().startswith("https://"):
            raise ValueError(f"entries[{idx}].u must be https URL")
        idi = e["i"].strip()
        if idi in seen_i:
            raise ValueError(f"duplicate i: {idi!r}")
        seen_i.add(idi)
        c = e.get("c")
        if c is not None and (not isinstance(c, str) or not c.strip()):
            raise ValueError(f"entries[{idx}].c must be non-empty string or omitted")
        o = e.get("o")
        if o is not None and (not isinstance(o, str) or not o.strip()):
            raise ValueError(f"entries[{idx}].o must be non-empty string or omitted")
        g = e.get("g")
        if g is not None:
            if not isinstance(g, list):
                raise ValueError(f"entries[{idx}].g must be list or omitted")
            for j, tag in enumerate(g):
                if not isinstance(tag, str) or not tag.strip():
                    raise ValueError(f"entries[{idx}].g[{j}] invalid")


def atomic_write_json(out_path: str, payload: dict[str, Any], *, verbose: bool) -> None:
    validate_app_feed_payload(payload)
    tmp = f"{out_path}.tmp.{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, out_path)
    except BaseException:
        if os.path.isfile(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise
    if verbose:
        print(
            f"Wrote {out_path} ({len(payload['entries'])} entries, listVersion={payload['listVersion']}).",
            file=sys.stderr,
        )
