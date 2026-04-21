# data-scp-docs

`scp_list.json`（ScpDocs アプリ用リモート一覧）を GitHub Pages で配信するリポジトリです。

## 一覧 JSON の生成（ソース・オブ・トゥルース）

**`scripts/update_list.py` と `requirements.txt` はこのリポジトリが正です。**  
GitHub Actions は checkout した同じ内容を使い、**別リポジトリからスクリプトを取得しません**（`app-scp-docs` を private にしても一覧 CI は独立して動きます）。

ローカルで試す場合:

```bash
pip install -r requirements.txt
mkdir -p docs
python3 scripts/update_list.py --out docs/scp_list.json \
  --merge-metadata-from docs/scp_list.json \
  --reuse-hub-linked-paths-from docs/scp_list.json \
  --verbose
# メタデータ付き（初回は全記事。2回目以降は増分が既定の CI と同じ）
python3 scripts/update_list.py --out docs/scp_list.json \
  --merge-metadata-from docs/scp_list.json \
  --with-article-metadata --metadata-only-missing \
  --metadata-max-age-days 14 --verbose
# --checkpoint-every N（既定 10）: N 件ごとに docs/scp_list.json へ原子書き込み。0 で定期のみオフ（異常終了時は取得済みがあれば 1 回フラッシュ）。
# Wikidot へ全記事を再取得（負荷大）
python3 scripts/update_list.py --out docs/scp_list.json --with-article-metadata --verbose
```

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | シリーズ一覧から `docs/scp_list.json` を生成。**毎日 15:00 UTC（翌日 0:00 JST）** ＋手動。既存から `objectClass` / `tags` をマージし、**`--reuse-hub-linked-paths-from`** で `hubLinkedPaths` の再クロールを省略（国際一覧だけで 10 分以上かかるのを避ける。ハブの生データはメタ付きジョブで更新）。 |
| **Update scp_list.json (with article metadata)** | 各記事からタグ・オブジェクトクラス取得。**毎週日曜 15:00 UTC（翌週月曜 0:00 JST）** ＋手動。既存 JSON をマージし **`--metadata-only-missing`** と **`--metadata-max-age-days 14`** で、タイムスタンプが **14 日以内**の記事は HTTP 省略、それ以外・未取得は再取得して本家のタグ追記に追従。全件取り直すときは `--metadata-only-missing` を外すか `--metadata-max-age-days 0`。 |

差分があるときだけ `docs/scp_list.json` がコミットされ、Pages が更新されます。

### Actions の push が rejected になるとき

別のワークフローや手動 push で `main` が進んでいると push が拒否されることがあります。ワークフローでは **コミット後に `fetch` + `rebase origin/main` してから push** するようになっています。まだ失敗する場合は Actions のログを確認し、同時に複数ジョブが走っていないか（`concurrency` で直列化済み）を見てください。

## アプリ側（app-scp-docs）

一覧生成スクリプトは **本リポジトリのみ**で管理しています。アプリの `AppRemoteConfig.scpListJSONURLString` は本リポジトリの GitHub Pages URL を指してください。
