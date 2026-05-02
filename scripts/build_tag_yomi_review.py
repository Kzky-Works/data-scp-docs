#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

KANJI_RE = re.compile(r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
KATAKANA_RE = re.compile(r"[\u30A1-\u30FA\u30FC]")

HEADER = ["tag", "yomi", "note"]

COMPOUND_YOMI: tuple[tuple[str, str], ...] = (
    ("財団", "ざいだん"),
    ("博士", "はかせ"),
    ("教授", "きょうじゅ"),
    ("管理官", "かんりかん"),
    ("職員", "しょくいん"),
    ("研究", "けんきゅう"),
    ("研究員", "けんきゅういん"),
    ("研究所", "けんきゅうじょ"),
    ("計画", "けいかく"),
    ("実験", "じっけん"),
    ("調査", "ちょうさ"),
    ("報告", "ほうこく"),
    ("記録", "きろく"),
    ("記憶", "きおく"),
    ("書庫", "しょこ"),
    ("事件", "じけん"),
    ("事案", "じあん"),
    ("異常", "いじょう"),
    ("対策", "たいさく"),
    ("部局", "ぶきょく"),
    ("商事", "しょうじ"),
    ("出版社", "しゅっぱんしゃ"),
    ("大学", "だいがく"),
    ("主義", "しゅぎ"),
    ("人型", "ひとがた"),
    ("夢", "ゆめ"),
    ("林", "はやし"),
    ("団子", "だんご"),
    ("見合", "みあい"),
    ("仕掛", "しかけ"),
    ("天気", "てんき"),
    ("会う", "あう"),
    ("度に", "たびに"),
    ("誕生日", "たんじょうび"),
    ("祝典", "しゅくてん"),
    ("夏休み", "なつやすみ"),
    ("夏季", "かき"),
    ("冬季", "とうき"),
    ("年末", "ねんまつ"),
    ("終末", "しゅうまつ"),
    ("記事", "きじ"),
    ("大会", "たいかい"),
    ("放送", "ほうそう"),
    ("漫画", "まんが"),
    ("世界", "せかい"),
    ("滅亡", "めつぼう"),
    ("犯罪", "はんざい"),
    ("短編", "たんぺん"),
    ("回憶", "かいおく"),
    ("新人", "しんじん"),
    ("愚人", "ぐじん"),
    ("画廊", "がろう"),
    ("聖夜", "せいや"),
    ("相守", "あいまもり"),
    ("金字塔", "きんじとう"),
    ("登り", "のぼり"),
    ("即興", "そっきょう"),
    ("反復", "はんぷく"),
    ("絶対", "ぜったい"),
    ("紅藍", "こうらん"),
    ("合戦", "かっせん"),
    ("怪談", "かいだん"),
    ("新鋭", "しんえい"),
    ("沈黙", "ちんもく"),
    ("霊異", "れいい"),
    ("月間", "げっかん"),
    ("再誕", "さいたん"),
    ("周年", "しゅうねん"),
    ("遅刻", "ちこく"),
    ("投稿", "とうこう"),
    ("語", "ご"),
    ("未満", "みまん"),
    ("渉外長", "しょうがいちょう"),
    ("社", "しゃ"),
    ("漢字", "かんじ"),
    ("計画", "けいかく"),
    ("調停官", "ちょうていかん"),
)


def katakana_to_hiragana(text: str) -> str:
    out: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            out.append(chr(code - 0x60))
        else:
            out.append(ch)
    return "".join(out)


def load_existing_review(path: Path) -> dict[str, tuple[str, str]]:
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames != HEADER:
            raise ValueError(f"{path}: expected TSV header {HEADER}, got {reader.fieldnames}")
        out: dict[str, tuple[str, str]] = {}
        for line_no, row in enumerate(reader, start=2):
            tag = (row.get("tag") or "").strip()
            if not tag:
                raise ValueError(f"{path}:{line_no}: empty tag")
            out[tag] = ((row.get("yomi") or "").strip(), (row.get("note") or "").strip())
        return out


def initial_yomi(tag: str) -> tuple[str, str]:
    converted = tag
    changed = False
    for raw, yomi in COMPOUND_YOMI:
        if raw in converted:
            converted = converted.replace(raw, yomi)
            changed = True
    converted = katakana_to_hiragana(converted)
    if KANJI_RE.search(converted):
        return "", "needs_review"
    if changed:
        return converted, "auto_compound_needs_review"
    if KATAKANA_RE.search(tag):
        return converted, "auto_kana_needs_review"
    return "", "needs_review"


def kanji_tags_from_jp_tag(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    tags = data.get("tags")
    if not isinstance(tags, list):
        raise ValueError(f"{path}: expected top-level tags list")
    uniq = sorted({str(t).strip() for t in tags if str(t).strip()})
    return [t for t in uniq if KANJI_RE.search(t)]


def build_review(input_path: Path, output_path: Path) -> int:
    tags = kanji_tags_from_jp_tag(input_path)
    existing = load_existing_review(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for tag in tags:
            if tag in existing:
                yomi, note = existing[tag]
            else:
                yomi, note = initial_yomi(tag)
            writer.writerow({"tag": tag, "yomi": yomi, "note": note})
    return len(tags)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build review TSV for SCP-JP tag yomi.")
    parser.add_argument(
        "-i",
        "--input",
        default="data-scp-docs/list/jp/jp_tag.json",
        help="Path to jp_tag.json",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data-scp-docs/list/jp/tag_yomi_review.tsv",
        help="Path to review TSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    count = build_review(Path(args.input), Path(args.output))
    print(f"wrote {count} kanji tags to {args.output}")


if __name__ == "__main__":
    main()
