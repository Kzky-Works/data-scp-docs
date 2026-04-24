# data-scp-docs 変更候補（scp_docs 同梱ミラー）

このフォルダは **[Kzky-Works/data-scp-docs](https://github.com/Kzky-Works/data-scp-docs)** リポジトリへ取り込むための **スクリプト・ワークフロー案**です。GitHub Pages の実データは data-scp-docs 側の `main` が正です。

## 反映手順

1. `data-scp-docs` を clone し、本ディレクトリの内容で上書き（少なくとも `scripts/harvester.py`）。
2. `pip install -r requirements.txt`
3. `python3 scripts/harvester.py`（Wikidot へのリクエストが多く **数十分かかる**場合があります）
4. `python3 scripts/validate_manifests.py`
5. `list/jp/` に `manifest_canons.json` / `manifest_jokes.json` が生成されていることを確認してコミット・プッシュ

## 主な変更（マルチフォーム計画対応）

| 項目 | 内容 |
|------|------|
| **Canon** | `canon-hub-jp` / `canon-hub` / `series-hub-jp` の `#page-content` から単一スラッグリンクを収集 → `manifest_canons.json` |
| **Joke** | `joke-scps` / `joke-scps-jp` からジョーク記事パス（`-j` / `-jp-j` 等）を抽出 → `manifest_jokes.json` |
| **GoI** | `goi-formats-jp` ハブリンクに切替（旧: `goi-format` タグページのみ）。`metadata` に `o`（団体表示名＝リンクテキスト） |
| **Tales** | `foundation-tales`（本家翻訳ハブ）を `foundation-tales-jp` に続けて取得し、`i` で重複除去してマージ |
| **listVersion** | 前回出力と `entries`+`metadata` が同一なら据え置き、変化時のみ `+1`（§13.2） |

旧 **`canons.json` / `jokes.json`（ホスト直下）** は配信しない方針です（`docs/HANDOVER_TALES_CANON_COLLECTION_RULES_ja.md` §13）。
