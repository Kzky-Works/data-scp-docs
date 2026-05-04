# データ収集パイプライン

`data-scp-docs` リポジトリの `list/jp/*.json` を Wikidot から自動収集する仕組みの全体像と運用ガイド。

---

## 1. 全体像

```
┌─────────────────────────────────────────────────────────────────┐
│  Wikidot (scp-jp.wikidot.com)                                   │
│   ├─ /sitemap.xml (sitemap-index)                               │
│   │    └─ /sitemap_page_*.xml … 全ページ <loc> + <lastmod>      │
│   ├─ /scp-series-jp, /scp-series, /scp-series-cn …(一覧ハブ)    │
│   ├─ /most-recently-created-jp / -translated / -edited(差分)    │
│   ├─ /system:page-tags/tag/jp/p/N (タグ逆引き)                  │
│   └─ /scp-001-jp (個別記事本文 → img/desc/lu)                   │
└───────────────────────┬─────────────────────────────────────────┘
                        │ HTTP GET (REQUEST_DELAY_SEC = 0.45s)
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  GitHub Actions (data-scp-docs)                                 │
│   ┌──────────┬──────────┬──────────┬──────────────┐             │
│   │ daily    │ weekly   │ monthly  │ jp-tag-map   │             │
│   │ (incr.)  │ (weekly) │ (full)   │ (タグ逆引き) │             │
│   └────┬─────┴────┬─────┴────┬─────┴──────┬───────┘             │
│        ▼          ▼          ▼            ▼                     │
│        scripts/harvester.py        build_jp_wikidot_tag_…       │
└───────────────────────┬─────────────────────────────────────────┘
                        │ git commit & push (main)
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  list/jp/ on GitHub main                                        │
│   ├─ catalog_index.json (manifest 群の集約インデックス)         │
│   ├─ manifest_scp-jp.json / manifest_scp-main.json /            │
│   │  manifest_scp-int.json / manifest_tales.json /              │
│   │  manifest_canons.json / manifest_gois.json /                │
│   │  manifest_jokes.json                                        │
│   ├─ jp_tag.json / jp_tag_compact.json (タグ逆引き)             │
│   └─ tag_yomi.json (手動運用)                                   │
└───────────────────────┬─────────────────────────────────────────┘
                        │ raw.githubusercontent.com 経由
                        ▼
                  iOS アプリ (取得・キャッシュ)
```

---

## 2. ワークフロー一覧

### 2.1 日次（incremental）— 超軽量

| 項目 | 値 |
|---|---|
| ファイル | [.github/workflows/daily.yml](../.github/workflows/daily.yml) |
| Cron | `0 0 * * *` (毎日 00:00 UTC) |
| Timeout | 90 分 |
| コマンド | `python3 scripts/harvester.py --mode incremental` |
| 対象 | 全マニフェスト |

**仕組み**: Wikidot の `/most-recently-created-jp`, `/most-recently-created-translated`, `/most-recently-edited` を state ファイルのカーソル以降だけ読み、新規/編集された記事だけ本文 fetch して manifest に差分反映。

**リクエスト量**: 大半の日は数十リクエスト程度（リスティング数ページ + 本文数件）。`INCREMENTAL_MAX_BODY_FETCHES_PER_RUN = 200` で上限。

### 2.2 週次（weekly）— 中量

| 項目 | 値 |
|---|---|
| ファイル | [.github/workflows/weekly.yml](../.github/workflows/weekly.yml) |
| Cron | `0 1 * * 0` (毎週日曜 01:00 UTC) |
| Timeout | 180 分 |
| コマンド | `python3 scripts/harvester.py --mode weekly --time-budget-minutes 160` |
| 対象 | 全マニフェスト |

**仕組み**: 一覧ハブ全走査 + Tale 本文 lu/img/desc を差分取得。サイトマップ lastmod が `prior_lu` 以下なら本文 fetch をスキップ。`--time-budget-minutes 160` で CI タイムアウト前に graceful stop し、部分結果をコミット → 翌週続きから再開可能。

**リクエスト量**: 一覧 ~115 + Tale 差分 数十〜数百。サイトマップ導入後は新規/編集された記事だけ。

> **TODO（コメント済み）**: daily incremental + sitemap-skip でカバレッジ十分なら、このワークフローは将来 retire 予定。

### 2.3 月次（full）— 整合性監査・初回投入

| 項目 | 値 |
|---|---|
| ファイル | [.github/workflows/update.yml](../.github/workflows/update.yml) |
| Cron | `0 0 1 * *` (毎月 1 日 00:00 UTC) |
| Timeout | 180 分 |
| コマンド | `python3 scripts/harvester.py --mode full` |
| 対象 | 全マニフェスト |

**仕組み**: 一覧ハブ全走査 + 全記事の本文 fetch。incremental/weekly が見落とした構造変更・renames・削除を補正する monthly audit 兼 初回フル投入用。

**リクエスト量**: 約 4,000〜5,000+ 本文 fetch。サイトマップ skip で初回以降は大幅圧縮。

### 2.4 タグ逆引き（jp-tag-map）

| 項目 | 値 |
|---|---|
| ファイル | [.github/workflows/jp-tag-map.yml](../.github/workflows/jp-tag-map.yml) |
| Cron | `0 0 * * 0` (毎週日曜 00:00 UTC) |
| Timeout | 360 分 |
| コマンド | `python3 scripts/build_jp_wikidot_tag_article_map.py` |
| 出力 | `list/jp/jp_tag.json`, `jp_tag_compact.json` |

**仕組み**: `/system:page-tags/tag/jp/p/1..59` を全ページ走査して「タグ → 記事スラッグ」のマップを生成。harvester がこれを使ってオブジェクトクラスやタグ配列を manifest の `metadata.c` / `metadata.g` に反映。

**並び順**: 日曜は `jp-tag-map (00:00) → weekly (01:00)` の順。タグマップが先に更新されてから週次クロールで manifest に反映される設計。

---

## 3. harvester.py の 4 モード

| `--mode` | 一覧ハブ | 本文 fetch | sitemap-skip | state 利用 | 用途 |
|---|---|---|---|---|---|
| `incremental` | × | 差分（cursor 駆動） | △ | ✓ (cursor) | daily.yml |
| `daily` | ✓ | × (前回値復元のみ) | — | × | レガシー（roll-back 用） |
| `weekly` | ✓ | 差分（lu 抜け+sitemap 変化） | ✓ | ✓ (section_offset) | weekly.yml |
| `full` | ✓ | 全件（sitemap 一致なら skip） | ✓ | ✓ (lastFullRunUtc) | update.yml / 初回 |

### 3.1 共通ロジック: サイトマップ lastmod-skip

`run()` 冒頭で `sitemap.py:load_wikidot_sitemap_lastmod()` を 1 回だけ実行（約 26,000 エントリ、~3MB / 数秒）。enrich 関数が `prior_lu >= sitemap_lastmod` の記事は本文 fetch を完全省略する。

`--no-sitemap` フラグで無効化可能（デバッグ用）。

### 3.2 一覧ページの除外規則

- **`<a class="newpage">`**: Wikidot 慣習で「未作成（404 確定）」のため discovery 段階で除外
- **CN 系**: `/scp-series-cn-1-tales-edition` 等の派生ページは記事リンクが 404 を量産するため、`CN_SERIES_VALID_PATHS = {/scp-series-cn, /scp-series-cn-2 〜 -5}` のみ許可

---

## 4. 生成 JSON

すべて `list/jp/` 配下、GitHub raw 経由でアプリが取得。

### 4.1 マニフェスト（schemaVersion 2）

| ファイル | 件数（参考） | 内容 |
|---|---|---|
| `manifest_scp-jp.json` | ~4,979 | JP 支部 SCP 報告書 |
| `manifest_scp-main.json` | ~9,978 | 本家英語訳 SCP（series-1〜10）|
| `manifest_scp-int.json` | ~2,899 | 国際訳文（CN/RU/DE/PL 等）|
| `manifest_tales.json` | ~3,959 | Tales（JP/EN/JP+EN）|
| `manifest_jokes.json` | ~402 | Joke 記事（JP/EN）|

**構造**:
```json
{
  "schemaVersion": 2,
  "listVersion": 123,           // 内容変化時に +1（同一なら据え置き）
  "generatedAt": "2026-05-04T01:00:00Z",
  "entries":  [{ "u": ..., "i": ..., "t": ..., "lu"?: ... }, ...],
  "metadata": {
    "scp-001-jp": {
      "c": "Keter",                     // オブジェクトクラス
      "g": ["keter", "scp", ...],       // タグ配列
      "img": "https://...",             // サムネ URL
      "desc": "...",                    // 抜粋
      "lu": 1730000000,                 // 最終更新 unix（sitemap-skip 判定用）
      "r": "jp" | "en" | "jp+en",       // tales のみ: 出元 region
      "a": ["author1", ...]             // tales のみ: 著者
    }
  }
}
```

### 4.2 Canon マニフェスト（schemaVersion 2 + canonRegions）

`manifest_canons.json` (~118 件) は `entries`/`metadata` に加えて:
```json
{
  "canonRegions": {
    "jp":         [{ "u","i","t","ct","ds","desc","lu" }, ...],
    "en":         [...],
    "seriesJp":   [...]
  }
}
```
- `ds`: 索引ページ由来のキャプション
- `desc`: 個別ハブページ冒頭段落（今回追加）
- `ct`: シリーズタグ
- `lu`: ハブ本体の最終更新

### 4.3 GoI マニフェスト（schemaVersion 3）

`manifest_gois.json` (~48 件) は `goiRegions` のみ:
```json
{
  "schemaVersion": 3,
  "goiRegions": {
    "en":    [{ "u","i","t","desc"? }, ...],
    "jp":    [...],
    "other": [...]
  }
}
```
詳細: [GOI_MANIFEST_V3_ja.md](GOI_MANIFEST_V3_ja.md)

### 4.4 集約インデックス（軽量起動用）

`catalog_index.json`: 上記 7 マニフェストの `listVersion` / `schemaVersion` / `contentHash`(sha256) / `byteSize` / `entryCount` / `generatedAt` を 1 ファイルに集約。アプリは起動時にこれだけを取得し、`listVersion`/`contentHash` が変わった kind だけ本体を再ダウンロードする。

### 4.5 タグ関連

| ファイル | 内容 |
|---|---|
| `jp_tag.json` | タグ → 記事スラッグ + メタの完全版 |
| `jp_tag_compact.json` | アプリ向け軽量版 |
| `tag_yomi.json` | タグ読み仮名（`build_tag_yomi_*.py` 手動運用）|
| `tag_yomi_review.tsv` | yomi のレビュー用 TSV |

### 4.6 状態ファイル

`_harvest_state.json`: ワークフロー間で永続化される状態
```json
{
  "section_offset": 2,                              // weekly のセクション開始位置ローテ
  "incremental": {
    "lastCreatedJpUnix": 1730000000,                // daily の cursor
    "lastCreatedTranslatedUnix": ...,
    "lastEditedUnix": ...,
    "lastIncrementalRunUtc": "2026-05-04T..."
  },
  "lastFullRunUtc": "2026-05-01T..."                // monthly full の最終完走時刻
}
```

---

## 5. 初回 full の手順

新規セットアップ／全件再エンリッチが必要なときは `update.yml`（full モード）を 1 回起動するだけ。3 通りのトリガー方法:

### 方法 A: GitHub UI

1. https://github.com/Kzky-Works/data-scp-docs/actions
2. 左メニューで **Update list feeds (full)** を選択
3. **Run workflow** ドロップダウン → **Run workflow** ボタン

### 方法 B: gh CLI

```bash
gh workflow run update.yml --repo Kzky-Works/data-scp-docs
gh run watch --repo Kzky-Works/data-scp-docs   # 進捗を見る
```

### 方法 C: ローカル実行（デバッグ用）

```bash
cd data-scp-docs
pip install -r requirements.txt
python3 scripts/harvester.py --mode full --git-commit --git-push
```

**所要時間**: 初回 full は 4,000〜5,000 件の本文 fetch で約 30 分〜数時間。サイトマップ skip が効く 2 回目以降は 5〜30 分程度。

---

## 6. 運用 Tips

### 6.1 並列ワークフロー時の競合回避

`concurrency: data-scp-docs-list-jp-write` で同時実行は 1 本のみ（後続は待機、`cancel-in-progress: false`）。`merge_manifest.py` がカスタム merge driver として登録されているため、push 時のリベースで manifest 構造を理解した自動マージが行われる。

### 6.2 ロールバック

- daily.yml が壊れたら `--mode incremental` を `--mode daily` に変更すると旧来のハブ全走査に戻る
- サイトマップ取得が止まったら `--no-sitemap` を付けると従来の prior_img 由来 skip にフォールバック
- weekly section_offset がおかしくなったら `--reset-state` で 0 にリセット

### 6.3 失敗時の通知

各ワークフローは `if: failure()` で GitHub Issue を自動起票するステップがある。`gh issue list` で確認できる。

### 6.4 リリース前検証（手動）

`scripts/verify_manifest_tales_for_release.py` で `manifest_tales.json` の `lu` 充足率を確認してから App Store ビルドを出すのが推奨。

```bash
python3 scripts/verify_manifest_tales_for_release.py \
    --url https://raw.githubusercontent.com/Kzky-Works/data-scp-docs/main/list/jp/manifest_tales.json
```

---

## 7. スクリプト一覧

| スクリプト | 役割 | 実行元 |
|---|---|---|
| `harvester.py` | コア収集 orchestrator (4 モード) | daily/weekly/update.yml |
| `sitemap.py` | Wikidot サイトマップ loader (今回追加) | harvester.py から import |
| `recent_pages.py` | 「最近作成/翻訳/編集」ページの cursor iterator | harvester.py (incremental) |
| `wikidot_utils.py` | 共通ヘルパー（odate パース等） | 全スクリプト |
| `validate_manifests.py` | manifest 整合性チェック | 各 harvest workflow の後処理 |
| `merge_manifest.py` | git カスタム merge driver | `.gitattributes` 経由 |
| `build_jp_wikidot_tag_article_map.py` | タグ逆引き生成 | jp-tag-map.yml |
| `build_tag_yomi_json.py` | タグ読み仮名 JSON 生成 | 手動 |
| `build_tag_yomi_review.py` | タグ読み仮名 TSV 生成 | 手動 |
| `verify_manifest_tales_for_release.py` | リリース前 lu 充足率ゲート | 手動（App Store 提出前）|

---

## 8. アプリ側との境界

iOS アプリは `https://raw.githubusercontent.com/Kzky-Works/data-scp-docs/main/list/jp/` から取得・キャッシュのみ。データ生成・形式変更は本リポジトリ側の責務。境界仕様: [app-scp-docs の `docs/DEV_RULE_ARTICLE_DATA_IN_DATA_SCP_DOCS_ja.md`](https://github.com/Kzky-Works/app-scp-docs/blob/main/docs/DEV_RULE_ARTICLE_DATA_IN_DATA_SCP_DOCS_ja.md)
