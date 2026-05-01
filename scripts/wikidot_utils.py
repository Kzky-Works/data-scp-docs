"""Small shared helpers for Wikidot harvest scripts."""
from __future__ import annotations

import re


def wikidot_tag_list_total_pages(html: str) -> int:
    """Return total pages from a Wikidot page-tags pager; fall back to 1."""
    m = re.search(r'class="pager-no"[^>]*>\s*ページ\s+\d+\s*/\s*(\d+)\s*<', html, re.I)
    if m:
        return max(1, int(m.group(1)))
    m = re.search(r'class="pager-no"[^>]*>\s*page\s+\d+\s+of\s+(\d+)\s*<', html, re.I)
    if m:
        return max(1, int(m.group(1)))
    return 1
