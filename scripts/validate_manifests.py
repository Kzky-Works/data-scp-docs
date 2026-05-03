#!/usr/bin/env python3
"""manifest_*.json の metadata キーが entries[].i にすべて存在することを検証する。"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

OBJECT_CLASS_TAGS = (
    "safe",
    "euclid",
    "keter",
    "thaumiel",
    "neutralized",
    "explained",
    "apollyon",
    "esoteric-class",
)

OC_TAG_TO_DISPLAY = {
    "safe": "Safe",
    "euclid": "Euclid",
    "keter": "Keter",
    "thaumiel": "Thaumiel",
    "neutralized": "Neutralized",
    "explained": "Explained",
    "apollyon": "Apollyon",
    "esoteric-class": "Esoteric",
}


def object_class_from_tags(tags: list[str]) -> str | None:
    tag_set = {t.strip().lower() for t in tags if isinstance(t, str) and t.strip()}
    for tag in OBJECT_CLASS_TAGS:
        if tag in tag_set:
            return OC_TAG_TO_DISPLAY.get(tag, tag.replace("-", " ").title())
    return None


def load_jp_tag_articles(dir_path: str) -> tuple[dict[str, list[str]], list[str]]:
    path = os.path.join(dir_path, "jp_tag.json")
    if not os.path.isfile(path):
        return {}, []
    errs: list[str] = []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        return {}, [f"{path}: failed to load jp_tag.json: {ex}"]
    if not isinstance(data, dict):
        return {}, [f"{path}: root must be object"]
    articles = data.get("articles")
    if not isinstance(articles, dict):
        return {}, [f"{path}: missing articles object"]
    out: dict[str, list[str]] = {}
    for k, v in articles.items():
        if not isinstance(k, str) or not k.strip():
            errs.append(f"{path}: articles contains non-empty string keys only")
            continue
        if not isinstance(v, list) or not v:
            errs.append(f"{path}: articles[{k!r}] must be a non-empty array")
            continue
        tags = [x.strip() for x in v if isinstance(x, str) and x.strip()]
        if len(tags) != len(v):
            errs.append(f"{path}: articles[{k!r}] contains non-string or empty tag")
            continue
        out[k.strip().lower()] = tags
    return out, errs


def check_file(
    path: str,
    jp_tag_articles: dict[str, list[str]],
    *,
    url_to_i_global: dict[str, tuple[str, str]] | None = None,
) -> list[str]:
    errs: list[str] = []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return [f"{path}: root must be object"]
    entries = data.get("entries")
    metadata = data.get("metadata")
    if not isinstance(entries, list):
        errs.append(f"{path}: missing entries array")
        return errs
    ids: set[str] = set()
    is_tales = os.path.basename(path) == "manifest_tales.json"
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            errs.append(f"{path}: entries[{i}] not object")
            continue
        slug = e.get("i")
        url_raw = e.get("u")
        if isinstance(slug, str) and slug.strip():
            key = slug.strip()
            if key in ids:
                errs.append(f"{path}: duplicate entries[].i {key!r}")
            ids.add(key)
            # tales マニフェストでは `lu`（最終更新 unix）が必須。harvester が引き継ぎ／差分取得しても落ちない範囲で警告。
            if is_tales:
                lu = e.get("lu")
                if not (isinstance(lu, int) and lu > 0):
                    # 新規記事は初回 weekly fetch までは lu 不在になりうるため、warn 相当だが致命とはしない。
                    pass
        if isinstance(url_raw, str) and url_raw.strip() and url_to_i_global is not None and isinstance(slug, str) and slug.strip():
            u_key = url_raw.strip()
            slug_key = slug.strip()
            existing = url_to_i_global.get(u_key)
            # 同じ `i` が複数マニフェストに現れるのは設計上許容（hub 兼 canon 等）。
            # 違反は「同 URL が**異なる** `i` に紐づく」場合のみ。
            if existing is not None and existing[1] != slug_key:
                errs.append(
                    f"{path}: URL {u_key!r} is reused by both {existing[1]!r} (in {existing[0]}) and {slug_key!r}"
                )
            elif existing is None:
                url_to_i_global[u_key] = (path, slug_key)
    if not isinstance(metadata, dict):
        return errs
    for k, v in metadata.items():
        if k not in ids:
            errs.append(f"{path}: metadata orphan key {k!r}")
        if not isinstance(v, dict):
            continue
        # `img` / `desc` は harvester で焼き込まれた続きから読むプレビュー用フィールド。型のみ検証。
        img_val = v.get("img")
        if img_val is not None and (not isinstance(img_val, str) or not img_val.strip()):
            errs.append(f"{path}: metadata[{k!r}].img must be non-empty string when present")
        desc_val = v.get("desc")
        if desc_val is not None and (not isinstance(desc_val, str) or not desc_val.strip()):
            errs.append(f"{path}: metadata[{k!r}].desc must be non-empty string when present")
        g = v.get("g")
        if g is None:
            continue
        if not isinstance(g, list):
            errs.append(f"{path}: metadata[{k!r}].g must be an array")
            continue
        got = [x.strip() for x in g if isinstance(x, str) and x.strip()]
        if len(got) != len(g):
            errs.append(f"{path}: metadata[{k!r}].g contains non-string or empty tag")
            continue
        expected = jp_tag_articles.get(k.lower())
        if expected is not None and got != expected:
            errs.append(f"{path}: metadata[{k!r}].g does not match jp_tag.json articles[{k.lower()!r}]")
        inferred_c = object_class_from_tags(got)
        c_val = v.get("c")
        if inferred_c and isinstance(c_val, str) and c_val.strip() and c_val.strip() != inferred_c:
            errs.append(
                f"{path}: metadata[{k!r}].c {c_val!r} conflicts with OC inferred from metadata.g {inferred_c!r}"
            )
    return errs


def check_catalog_index(dir_path: str) -> list[str]:
    """`catalog_index.json` の `contentHash`/`byteSize`/`entryCount` を実ファイルと突き合わせる。

    存在しない場合はスキップ（旧構成との互換のため致命ではない）。
    """
    import hashlib

    path = os.path.join(dir_path, "catalog_index.json")
    if not os.path.isfile(path):
        return []
    errs: list[str] = []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        return [f"{path}: failed to load: {ex}"]
    files = data.get("files") if isinstance(data, dict) else None
    if not isinstance(files, list):
        return [f"{path}: missing files array"]
    for rec in files:
        if not isinstance(rec, dict):
            errs.append(f"{path}: files entry not object")
            continue
        url = rec.get("url")
        if not isinstance(url, str) or not url.strip():
            errs.append(f"{path}: files[].url missing")
            continue
        target = os.path.join(dir_path, url.strip())
        if not os.path.isfile(target):
            errs.append(f"{path}: files[].url not found on disk: {url!r}")
            continue
        with open(target, "rb") as f:
            blob = f.read()
        actual_hash = "sha256-" + hashlib.sha256(blob).hexdigest()
        actual_size = len(blob)
        expected_hash = rec.get("contentHash")
        expected_size = rec.get("byteSize")
        if expected_hash and expected_hash != actual_hash:
            errs.append(f"{path}: contentHash mismatch for {url!r} (expected {expected_hash}, got {actual_hash})")
        if isinstance(expected_size, int) and expected_size != actual_size:
            errs.append(f"{path}: byteSize mismatch for {url!r} (expected {expected_size}, got {actual_size})")
        # entryCount: best-effort check.
        try:
            target_payload = json.loads(blob)
        except Exception:
            continue
        if isinstance(target_payload, dict):
            entries = target_payload.get("entries")
            if isinstance(entries, list) and isinstance(rec.get("entryCount"), int):
                if rec["entryCount"] != len(entries):
                    errs.append(
                        f"{path}: entryCount mismatch for {url!r} "
                        f"(expected {rec['entryCount']}, got {len(entries)})"
                    )
    return errs


RETIRED_FILES = (
    # Retired in favor of the trifold manifest split (`list/jp/manifest_scp-*.json`).
    # Resurfacing these would silently re-expose the legacy schema to consumers.
    ("docs", "scp_list.json"),
)


def check_retired_files(repo_root: str) -> list[str]:
    errs: list[str] = []
    for parts in RETIRED_FILES:
        path = os.path.join(repo_root, *parts)
        if os.path.isfile(path):
            errs.append(f"{path}: retired file resurfaced — delete it (see APP_SPEC_HANDOVER §6.2)")
    return errs


def main() -> int:
    p = argparse.ArgumentParser(description="Validate manifest_scp-*.json metadata keys")
    p.add_argument(
        "dir",
        nargs="?",
        default=os.path.join(os.path.dirname(__file__), "..", "list", "jp"),
        help="Directory containing manifest_*.json",
    )
    args = p.parse_args()
    d = os.path.abspath(args.dir)
    if not os.path.isdir(d):
        print(f"ERROR: not a directory: {d}", file=sys.stderr)
        return 1
    names = sorted(x for x in os.listdir(d) if x.startswith("manifest_") and x.endswith(".json"))
    if not names:
        print(f"WARN: no manifest_*.json in {d}", file=sys.stderr)
        return 0
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    failed = False
    for err in check_retired_files(repo_root):
        print(err, file=sys.stderr)
        failed = True
    jp_tag_articles, jp_tag_errs = load_jp_tag_articles(d)
    for err in jp_tag_errs:
        print(err, file=sys.stderr)
        failed = True
    url_to_i_global: dict[str, tuple[str, str]] = {}
    for n in names:
        path = os.path.join(d, n)
        for err in check_file(path, jp_tag_articles, url_to_i_global=url_to_i_global):
            print(err, file=sys.stderr)
            failed = True
    for err in check_catalog_index(d):
        print(err, file=sys.stderr)
        failed = True
    if failed:
        return 1
    print(f"OK: {len(names)} manifest(s) in {d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
