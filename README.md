# data-scp-docs

`scp_list.json`（ScpDocs アプリ用リモート一覧）を GitHub Pages で配信するリポジトリです。

## アプリ 3 系統フィード: `scp-jp.json`

ScpDocs の **`SCPArticleListPayload`** 互換（`schemaVersion: 1`）。リポジトリ直下に **`scp-jp.json`** を置き、Pages のルートから配信します（`AppRemoteConfig` の `scp-jp.json` と同じパス）。

| キー | 意味 |
|------|------|
| `u` | 記事の絶対 URL（`https://scp-jp.wikidot.com/scp-…-jp`） |
| `i` | 安定 ID（例: `scp-173-jp`） |
| `t` | 日本支部オリジナル一覧（`scp-series-jp` 系）の行タイトル |
| `o` | 任意。本家メイン和訳一覧（`scp-series` 系）の行タイトル。`docs/scp_list.json` の `mainlistTranslationTitle` をマージ |
| `c` | 任意。オブジェクトクラス（`scp_list` の `objectClass` または記事ページ取得） |
| `g` | 任意。タグ配列（`scp_list` の `tags` または記事ページ取得） |

```bash
pip install -r requirements.txt
# 既定出力: リポジトリ直下 scp-jp.json。一覧は Wikidot、メタは既存 scp_list からマージ（推奨）
python3 scripts/generate_scp_jp_app_feed.py --merge-metadata-from docs/scp_list.json --verbose
# 記事ページを直接叩いて c/g を更新（全件・遅い）
python3 scripts/generate_scp_jp_app_feed.py --merge-metadata-from docs/scp_list.json --with-article-metadata --verbose
```

## アプリ 3 系統フィード: `scp.json`（本家メイン和訳）

ScpDocs の `AppRemoteConfig` は EN 系統のファイル名を **`scp.json`** としている（`scp-en.json` ではない）。

| キー | 意味 |
|------|------|
| `u` / `i` | `https://scp-jp.wikidot.com/scp-NNN`（`-jp` なし） |
| `t` | 本家メイン和訳一覧（`scp-series` …）の行タイトル |
| `o` | 任意。日本支部オリジナル（`-jp`）の行タイトル＝`docs/scp_list.json` の `title` |
| `c` / `g` | `scp_list` の `objectClass` / `tags` をマージ |

```bash
python3 scripts/generate_scp_en_app_feed.py --merge-metadata-from docs/scp_list.json --verbose
```

## アプリ 3 系統フィード: `scp-int.json`（国際支部和訳）

`docs/scp_list.json` の **`hubLinkedPaths`**（`/scp-数字-2文字`、`-jp` 除外）をエントリの正とする。

- **タイトル**: scp-international から辿る各一覧ページをクロールして `<li>` から抽出。取れない場合は **直前コミットの `scp-int.json`**（`--merge-titles-from`、既定でリポジトリ内の同名ファイル）を参照し、それでも無い場合は **`SCP-番号-XX`** 形式のフォールバック。
- **ローカルでクロールが進まない環境**（TLS 等）では `--skip-intl-crawl` でフォールバックのみ生成可。

```bash
# 本番相当（一覧クロールあり。GitHub Actions 既定）
python3 scripts/generate_scp_int_app_feed.py --hub-paths-from docs/scp_list.json --verbose
# タイトルはフォールバックのみ（高速・オフライン向け）
python3 scripts/generate_scp_int_app_feed.py --hub-paths-from docs/scp_list.json --skip-intl-crawl --no-merge-titles --verbose
```

## 一覧 JSON の生成（ソース・オブ・トゥルース）

**`scripts/update_list.py` と `requirements.txt` はこのリポジトリが正です。**  
**`system:page-tags` のタグ逆引き**は **`scripts/build_wikidot_category_catalogs.py`**（標準ライブラリのみ）で、`docs/catalog/*.json` に **カテゴリ別**書き出しします（**`scp_list.json` にタグをマージしない**）。旧 `wikidot_page_tags_merge.py` は非推奨です。  
GitHub Actions は checkout した同じ内容を使い、**別リポジトリからスクリプトを取得しません**（`app-scp-docs` を private にしても一覧 CI は独立して動きます）。

### 国内一覧（軽量）と国際ハブは別ジョブ

- **国内（日次）:** `--domestic-only` … シリーズ一覧＋`mainlistTranslationTitle`＋既存 JSON からのメタマージのみ。**国際一覧の HTTP は行わない**（`hubLinkedPaths` はマージ元ファイルの値のまま）。
- **国際ハブ（週次・別ワークフロー）:** `--hub-linked-paths-only` … 既存 `scp_list.json` を読み、**`hubLinkedPaths` だけ** Wikidot から再取得して書き戻す（重い処理はここだけに集約）。
- **メタ付き（週次）:** 既存 JSON に **`hubLinkedPaths` が入っていれば**、記事メタ取得の前に **国際クロールをスキップ**（空のときだけフル取得）。

ローカルで試す場合:

```bash
pip install -r requirements.txt
mkdir -p docs
# 国内のみ（日次 CI と同趣旨）
python3 scripts/update_list.py --out docs/scp_list.json \
  --merge-metadata-from docs/scp_list.json \
  --domestic-only \
  --verbose
# 国際 hub のみ更新（重い）
python3 scripts/update_list.py --hub-linked-paths-only --in-out docs/scp_list.json --verbose
# メタデータ付き（初回は全記事。2回目以降は増分が既定の CI と同じ）
python3 scripts/update_list.py --out docs/scp_list.json \
  --merge-metadata-from docs/scp_list.json \
  --with-article-metadata --metadata-only-missing \
  --metadata-max-age-days 14 --verbose
# --checkpoint-every N（既定 10）: N 件ごとに docs/scp_list.json へ原子書き込み。0 で定期のみオフ（異常終了時は取得済みがあれば 1 回フラッシュ）。
# Wikidot へ全記事を再取得（負荷大）
python3 scripts/update_list.py --out docs/scp_list.json --with-article-metadata --verbose

# Wikidot page-tags → カテゴリ別カタログ（scp_jp.json / scp.json / joke.json / tales.json / canon.json / goi.json）
mkdir -p docs/catalog
# 日次相当: タグ上限＋既存 JSON へマージ（部分実行でも欠落しにくい）
python3 scripts/build_wikidot_category_catalogs.py \
  --out-dir docs/catalog \
  --scp-list-path docs/scp_list.json \
  --sleep 0.35 \
  --mode incremental \
  --max-tags 100 \
  --shuffle-tags
# 週次相当: 全タグを走査して全置換（スナップショット）
python3 scripts/build_wikidot_category_catalogs.py \
  --out-dir docs/catalog \
  --scp-list-path docs/scp_list.json \
  --sleep 0.35 \
  --mode full \
  --max-tags 0
# 1 ファイルだけ更新: --categories scp_jp など
```

**運用（フル vs 増分）:** 既定の Actions 日次ジョブは **`--mode incremental`** と **`--max-tags 100`** で、タグ雲の一部だけを取りに行き、**既存 `docs/catalog/*.json` のエントリとタグ集合をマージ**します（未取得タグ由来の情報が一括で消えるのを避ける）。**全タグ・全置換のスナップショット**が必要なときは **`Wikidot category catalogs (full)`**（週次）または手動で **`--mode full --max-tags 0`** を使ってください。`--tag-skip-limit`（例: `0`）で 503 多発時にジョブを失敗扱いにできます。

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | **国内のみ（日次）**。**毎日 15:00 UTC（翌日 0:00 JST）** ＋手動。`--domestic-only` で国際一覧を叩かない。 |
| **Update scp-jp.json (app feed)** | **`scp-jp.json`（3 系統 JP フィード）**。**毎日 16:30 UTC** ＋手動。`docs/scp_list.json` からメタをマージしつつ Wikidot 一覧を再取得。 |
| **Update scp.json (app feed)** | **`scp.json`（本家メイン和訳）**。**毎日 16:45 UTC** ＋手動。 |
| **Update scp-int.json (app feed)** | **`scp-int.json`（国際支部パス）**。**毎週月曜 17:00 UTC** ＋手動。`hubLinkedPaths` ＋国際一覧クロール（所要長め）。 |
| **Update scp_list.json (international hub)** | **`hubLinkedPaths` のみ更新（週次）**。**毎週月曜 16:00 UTC** ＋手動。重い国際クロールはこのジョブだけ。 |
| **Update scp_list.json (with article metadata)** | 記事メタ取得。**毎週日曜 15:00 UTC（翌週月曜 0:00 JST）** ＋手動。既存 JSON に hub があれば国際クロール省略。 |
| **Wikidot category catalogs** | **`system:page-tags`** を巡回し **`docs/catalog/`** を更新。**日次 17:30 UTC**: `incremental`＋**100 タグ**＋シャッフル（既存 JSON とマージ）。実体は `wikidot-catalogs-reusable.yml`。 |
| **Wikidot category catalogs (full)** | **週次（日曜 18:15 UTC）**＋手動。**`full`**＋**全タグ**（`max_tags=0`）で **6 JSON を全置換**。 |
| **Catalog tags · …（6 本）** | **手動専用**。ラベルごとに **`categories` を 1 つだけ**指定し、その JSON のみ incremental 更新（他カタログはコミット対象に出ない）。 |

差分があるときだけ `docs/scp_list.json`（または `docs/catalog/*.json`）がコミットされ、Pages が更新されます。

### カタログ JSON（`docs/catalog/`）

| ファイル | 内容 |
|----------|------|
| `scp_jp.json` | `/scp-N-jp` のみ。`series` / `scpNumber` / `slug` / `url` / `title`（`scp_list.title`）/ `objectClass`（タグ語から昇格した OC）/ `tags` |
| `scp.json` | `/scp-N`（本家メイン和訳）のみ。`title` は `scp_list.mainlistTranslationTitle` |
| `joke.json` | `/scp-N-j` のみ |
| `tales.json` / `canon.json` / `goi.json` | SCP 番号形式以外のスラッグ。**Tales / Canon / GoI** はスラッグの **ヒューリスティック**（`goi-format`・`canon` 等）。専用 hub を全部クロールはしていないため、取りこぼし・誤分類は README 運用で都度調整してください。 |

**注意:** `/scp-N-xx`（国際支部、`-jp` 以外）はタグ一覧からは付与しません（誤マージ防止）。

**過去の `scp_list.json` に混ざった tags** は、メタジョブの `--merge-metadata-from` で温存されます。タグを捨てる場合は別途 `tags` を手直しするか、バックアップした上でメタを再取得する運用が必要です。

### Actions の push が rejected になるとき

別のワークフローや手動 push で `main` が進んでいると push が拒否されることがあります。ワークフローでは **コミット後に `fetch` + `rebase origin/main` してから push** するようになっています。まだ失敗する場合は Actions のログを確認し、同時に複数ジョブが走っていないか（`concurrency` で直列化済み）を見てください。

## アプリ側（app-scp-docs）

一覧生成スクリプトは **本リポジトリのみ**で管理しています。アプリの `AppRemoteConfig.scpListJSONURLString` は本リポジトリの GitHub Pages URL を指してください。
