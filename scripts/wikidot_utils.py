"""Small shared helpers for Wikidot harvest scripts."""
from __future__ import annotations

import re

_ODATE_TIME_CLASS_RE = re.compile(r"^time_(\d+)$")


def wikidot_tag_list_total_pages(html: str) -> int:
    """Return total pages from a Wikidot page-tags pager; fall back to 1."""
    m = re.search(r'class="pager-no"[^>]*>\s*ページ\s+\d+\s*/\s*(\d+)\s*<', html, re.I)
    if m:
        return max(1, int(m.group(1)))
    m = re.search(r'class="pager-no"[^>]*>\s*page\s+\d+\s+of\s+(\d+)\s*<', html, re.I)
    if m:
        return max(1, int(m.group(1)))
    return 1


def parse_odate_unix(node) -> int | None:
    """Extract the unix-second timestamp from a Wikidot odate element.

    Wikidot embeds creation/edit time as a CSS class on `<span class="odate time_NNNNNNNN ...">`.
    Returns None if the node has no `time_<digits>` token.
    """
    if node is None:
        return None
    classes = node.get("class") if hasattr(node, "get") else None
    if not classes:
        return None
    if isinstance(classes, str):
        classes = classes.split()
    for cl in classes:
        m = _ODATE_TIME_CLASS_RE.match(str(cl))
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None
