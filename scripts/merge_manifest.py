#!/usr/bin/env python3
"""Git custom merge driver for `list/jp/manifest_*.json` and `catalog_index.json`.

並行する weekly / daily / update workflow が同じ manifest を partial に書き換える際の
3-way merge を JSON 構造を理解した上で「情報量の多い側を優先」で自動解消する。

Git からの呼び出し: `merge_manifest.py %O %A %B %P` （`.gitattributes` の merge driver で指定）。
- `%O` = 共通祖先（base）
- `%A` = 自分側（ours、ローカル commit）
- `%B` = 相手側（theirs、main の最新）
- `%P` = 元のパス名（`list/jp/manifest_tales.json` 等）

成功時は `%A` を上書きして exit 0、失敗時は exit 1（手動マージにフォールバック）。

マージ規則:
- `entries`: `i` をキーに union。同一 `i` が両側にある場合、埋まっているフィールド数が多い方を採用、同点は ours。
- `metadata`: per-`i` でマージ。`lu` は max、`desc`/`img` は非空優先（タイなら長い方）、それ以外は ours 優先（無ければ theirs）。
- `listVersion`: `max(ours, theirs) + 1`（必ず単調増加）。
- `generatedAt`: ISO 8601 lexicographic max。
- `schemaVersion`: ours を維持（migration 中の混在を想定しないため、不一致なら exit 1）。
- `goiRegions` / `canonRegions`: 新しい `generatedAt` を持つ側を丸ごと採用（構造が複雑なため per-field では merge しない）。
- `files`（catalog_index）: `kind` をキーに union、`generatedAt` の新しい側を採用。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any


def _load(path: str) -> Any | None:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as ex:
        print(f"merge_manifest: failed to load {path}: {ex}", file=sys.stderr)
        return None


def _populated_fields(d: dict[str, Any]) -> int:
    """空でないフィールドの数を返す（None / 空文字 / 空リスト / 空辞書は除外）。"""
    n = 0
    for v in d.values():
        if v is None:
            continue
        if isinstance(v, (str, list, dict)) and not v:
            continue
        n += 1
    return n


def _merge_entry(ours: dict[str, Any], theirs: dict[str, Any]) -> dict[str, Any]:
    """同一 `i` を持つ entry 同士を per-field で merge。情報量の多い側を base にする。"""
    base = ours if _populated_fields(ours) >= _populated_fields(theirs) else theirs
    other = theirs if base is ours else ours
    out = dict(base)
    for k, v in other.items():
        if k in out and out[k]:
            continue
        if v not in (None, "", [], {}):
            out[k] = v
    # `lu` だけは max（より新しいタイムスタンプを優先）
    lus = [x for x in (ours.get("lu"), theirs.get("lu")) if isinstance(x, int) and x > 0]
    if lus:
        out["lu"] = max(lus)
    return out


def _merge_entries(
    ours: list[dict[str, Any]] | None,
    theirs: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    by_i: dict[str, dict[str, Any]] = {}
    for e in ours or []:
        if isinstance(e, dict) and isinstance(e.get("i"), str) and e["i"].strip():
            by_i[e["i"].strip()] = e
    for e in theirs or []:
        if not (isinstance(e, dict) and isinstance(e.get("i"), str) and e["i"].strip()):
            continue
        ik = e["i"].strip()
        if ik in by_i:
            by_i[ik] = _merge_entry(by_i[ik], e)
        else:
            by_i[ik] = e
    return sorted(by_i.values(), key=lambda r: str(r.get("i") or "").lower())


def _merge_metadata_chunk(ours: dict[str, Any], theirs: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set(ours.keys()) | set(theirs.keys())
    for k in keys:
        ov = ours.get(k)
        tv = theirs.get(k)
        if k == "lu":
            cands = [x for x in (ov, tv) if isinstance(x, int) and x > 0]
            if cands:
                out[k] = max(cands)
            continue
        if k in ("desc", "img"):
            ov_s = ov if isinstance(ov, str) and ov.strip() else ""
            tv_s = tv if isinstance(tv, str) and tv.strip() else ""
            if ov_s and tv_s:
                out[k] = ov_s if len(ov_s) >= len(tv_s) else tv_s
            elif ov_s:
                out[k] = ov_s
            elif tv_s:
                out[k] = tv_s
            continue
        # その他のキー: ours 優先、無ければ theirs
        if ov not in (None, "", [], {}):
            out[k] = ov
        elif tv not in (None, "", [], {}):
            out[k] = tv
    return out


def _merge_metadata(
    ours: dict[str, Any] | None,
    theirs: dict[str, Any] | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set((ours or {}).keys()) | set((theirs or {}).keys())
    for k in keys:
        ov = (ours or {}).get(k)
        tv = (theirs or {}).get(k)
        if isinstance(ov, dict) and isinstance(tv, dict):
            merged = _merge_metadata_chunk(ov, tv)
            if merged:
                out[k] = merged
        elif isinstance(ov, dict):
            if ov:
                out[k] = ov
        elif isinstance(tv, dict):
            if tv:
                out[k] = tv
    # 出力時は `i` でソート（書き出し順を安定化）
    return dict(sorted(out.items(), key=lambda kv: kv[0].lower()))


def _merge_catalog_files(
    ours: list[dict[str, Any]] | None,
    theirs: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    by_kind: dict[str, dict[str, Any]] = {}
    for rec in ours or []:
        if isinstance(rec, dict) and isinstance(rec.get("kind"), str):
            by_kind[rec["kind"]] = rec
    for rec in theirs or []:
        if not (isinstance(rec, dict) and isinstance(rec.get("kind"), str)):
            continue
        k = rec["kind"]
        if k not in by_kind:
            by_kind[k] = rec
            continue
        existing = by_kind[k]
        # generatedAt の新しい方を採用
        og = existing.get("generatedAt") or ""
        tg = rec.get("generatedAt") or ""
        by_kind[k] = rec if (isinstance(tg, str) and isinstance(og, str) and tg > og) else existing
    return sorted(by_kind.values(), key=lambda r: str(r.get("kind") or ""))


def merge(ours: dict[str, Any], theirs: dict[str, Any], path_hint: str) -> dict[str, Any]:
    """ours と theirs を merge した dict を返す（base は使わない、union 方針なので不要）。"""
    is_catalog = os.path.basename(path_hint) == "catalog_index.json"

    # schemaVersion 不一致は migration 中の異常なので fallback させる
    os_v = ours.get("schemaVersion")
    ts_v = theirs.get("schemaVersion")
    if os_v is not None and ts_v is not None and os_v != ts_v:
        raise ValueError(f"schemaVersion mismatch: ours={os_v} theirs={ts_v}")

    out: dict[str, Any] = dict(ours)

    if is_catalog:
        out["files"] = _merge_catalog_files(ours.get("files"), theirs.get("files"))
    else:
        out["entries"] = _merge_entries(ours.get("entries"), theirs.get("entries"))
        out["metadata"] = _merge_metadata(ours.get("metadata"), theirs.get("metadata"))

    # listVersion: 必ず max+1 で単調増加（古い側に巻き戻ることを防ぐ）
    versions = [
        v for v in (ours.get("listVersion"), theirs.get("listVersion"))
        if isinstance(v, int) and v > 0
    ]
    if versions:
        out["listVersion"] = max(versions) + 1

    # generatedAt: lex max
    gens = [
        g for g in (ours.get("generatedAt"), theirs.get("generatedAt"))
        if isinstance(g, str) and g.strip()
    ]
    if gens:
        out["generatedAt"] = max(gens)

    # 領域別構造（goiRegions / canonRegions）は丸ごと新しい側
    for region_key in ("goiRegions", "canonRegions"):
        ov = ours.get(region_key)
        tv = theirs.get(region_key)
        if ov is None and tv is None:
            continue
        og = ours.get("generatedAt") or ""
        tg = theirs.get("generatedAt") or ""
        out[region_key] = tv if (isinstance(tg, str) and isinstance(og, str) and tg > og and tv is not None) else (ov if ov is not None else tv)

    return out


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print("usage: merge_manifest.py %O %A %B %P", file=sys.stderr)
        return 2
    base_path, ours_path, theirs_path, path_hint = argv[0], argv[1], argv[2], argv[3]
    _ = _load(base_path)  # base は参照しないが引数仕様のため受け取る
    ours = _load(ours_path)
    theirs = _load(theirs_path)
    if not isinstance(ours, dict) or not isinstance(theirs, dict):
        print(
            f"merge_manifest: cannot parse JSON (ours={type(ours).__name__} "
            f"theirs={type(theirs).__name__}); falling back to manual merge",
            file=sys.stderr,
        )
        return 1
    try:
        merged = merge(ours, theirs, path_hint)
    except ValueError as ex:
        print(f"merge_manifest: {ex}; falling back to manual merge", file=sys.stderr)
        return 1
    tpath = ours_path + ".merge.tmp"
    with open(tpath, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tpath, ours_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
