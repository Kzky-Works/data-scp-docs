# data-scp-docs

**配信 JSON・ハーベスタ・CI の原本**は本リポジトリの **GitHub `main`** です。ローカルでこの clone を編集し、push して同期します。

**関連アプリ**: [Kzky-Works/app-scp-docs](https://github.com/Kzky-Works/app-scp-docs)（`scp_docs`）は取得・キャッシュのみ。データパイプラインの境界は app 側の `docs/DEV_RULE_ARTICLE_DATA_IN_DATA_SCP_DOCS_ja.md` を参照。

## レイアウト

| パス | 内容 |
|------|------|
| `scripts/harvester.py` | Wikidot から `list/jp/manifest_*.json` 等を生成 |
| `scripts/validate_manifests.py` | `manifest_*.json` の metadata 整合性チェック |
| `scripts/build_jp_wikidot_tag_article_map.py` | `list/jp/jp_tag.json` 生成 |
| `scripts/verify_manifest_tales_for_release.py` | Tales 同梱前の `manifest_tales.json` の `lu` 十分率ゲート |
| `docs/GOI_MANIFEST_V3_ja.md` | GoI manifest schema 3 の仕様 |
| `list/jp/` | 配信マニフェスト・タグマップ |
| `.github/workflows/update.yml` | 日次ハーベスト + push |
| `.github/workflows/jp-tag-map.yml` | 週次 `jp_tag.json` + push |

## GitHub Actions の運用順序

- 通常日は **Update list feeds** のみで十分です。`harvester.py` は既存 `list/jp/jp_tag.json` をタグ・オブジェクトクラス推定の正本として使い、OC専用の page-tags 巡回は通常スキップします。
- タグ鮮度を上げたい日は **JP tag map** → **Update list feeds** の順に手動実行します。先に `jp_tag.json` を更新してから manifest に `metadata.g` / `metadata.c` を反映します。
- `jp_tag.json` が読めない場合のみ、`harvester.py` はOC page-tagsへの fallback crawlを行います。

## ローカル実行

```bash
pip install -r requirements.txt
python3 scripts/harvester.py
python3 scripts/validate_manifests.py
```

（任意）Tales リリース前:

```bash
python3 scripts/verify_manifest_tales_for_release.py \
  --url 'https://raw.githubusercontent.com/Kzky-Works/data-scp-docs/main/list/jp/manifest_tales.json'
```

Harvester は Wikidot へのリクエストが多く、**数十分かかる**場合があります。

## 主な収集対象（概要）

| 項目 | 内容 |
|------|------|
| **Canon** | `canon-hub-jp` / `canon-hub` / `series-hub-jp` 等 |
| **Joke** | `joke-scps` / `joke-scps-jp` |
| **GoI** | `goi-formats-jp`（schema 3・`docs/GOI_MANIFEST_V3_ja.md`） |
| **Tales** | `foundation-tales-jp` + `foundation-tales`、`lu` 付与 |
| **listVersion** | 前回出力と差分が無ければ据え置き |

旧 **`canons.json` / `jokes.json`（ホスト直下）** は配信しない方針です（`HANDOVER_TALES_CANON_COLLECTION_RULES_ja.md` §13 は app リポ側ドキュメント）。
