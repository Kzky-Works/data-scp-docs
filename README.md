# data-scp-docs

`scp_list.json`（ScpDocs アプリ用リモート一覧）を GitHub Pages で配信するリポジトリです。

## 一覧 JSON の生成（ソース・オブ・トゥルース）

**`scripts/update_list.py` と `requirements.txt` はこのリポジトリが正です。**  
タグ逆引きマージ用の **`scripts/wikidot_page_tags_merge.py`**（標準ライブラリのみ）もここで管理します。  
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

# Wikidot system:page-tags のタグ一覧からタグを逆引きマージ（標準ライブラリのみ・CI と同じ）
python3 scripts/wikidot_page_tags_merge.py \
  --base-json-path docs/scp_list.json \
  --out docs/scp_list.json \
  --sleep 0.35 \
  --max-tags 100 \
  --shuffle-tags
```

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | **国内のみ（日次）**。**毎日 15:00 UTC（翌日 0:00 JST）** ＋手動。`--domestic-only` で国際一覧を叩かない。 |
| **Update scp_list.json (international hub)** | **`hubLinkedPaths` のみ更新（週次）**。**毎週月曜 16:00 UTC** ＋手動。重い国際クロールはこのジョブだけ。 |
| **Update scp_list.json (with article metadata)** | 記事メタ取得。**毎週日曜 15:00 UTC（翌週月曜 0:00 JST）** ＋手動。既存 JSON に hub があれば国際クロール省略。 |
| **Merge Wikidot page-tags into scp_list.json** | **`system:page-tags` のタグ一覧からタグを逆引き**して `tags` をマージ（ページネーション対応）。**毎日 17:30 UTC（翌 JST 02:30）** に **100 タグ・シャッフル**で実行（約 15 日でクラウド全体を一通り見るイメージ）＋手動。`wikidot_page_tags_merge.py` は依存パッケージ不要。 |

差分があるときだけ `docs/scp_list.json` がコミットされ、Pages が更新されます。

### Actions の push が rejected になるとき

別のワークフローや手動 push で `main` が進んでいると push が拒否されることがあります。ワークフローでは **コミット後に `fetch` + `rebase origin/main` してから push** するようになっています。まだ失敗する場合は Actions のログを確認し、同時に複数ジョブが走っていないか（`concurrency` で直列化済み）を見てください。

## アプリ側（app-scp-docs）

一覧生成スクリプトは **本リポジトリのみ**で管理しています。アプリの `AppRemoteConfig.scpListJSONURLString` は本リポジトリの GitHub Pages URL を指してください。
