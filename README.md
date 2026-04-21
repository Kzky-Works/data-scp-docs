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
  --merge-metadata-from docs/scp_list.json
# メタデータ付き（時間がかかります）
python3 scripts/update_list.py --out docs/scp_list.json --with-article-metadata --verbose
```

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | シリーズ一覧＋国際ハブから `docs/scp_list.json` を生成。**毎週月曜 18:00 UTC** ＋手動。既存ファイルから `objectClass` / `tags` を **マージ**するため、メタ付きジョブの結果を週次で消さない。 |
| **Update scp_list.json (with article metadata)** | 上記に加え各記事からタグ・オブジェクトクラス取得。**手動のみ**（数時間かかる場合あり）。 |

差分があるときだけ `docs/scp_list.json` がコミットされ、Pages が更新されます。

### Actions の push が rejected になるとき

別のワークフローや手動 push で `main` が進んでいると push が拒否されることがあります。ワークフローでは **コミット後に `fetch` + `rebase origin/main` してから push** するようになっています。まだ失敗する場合は Actions のログを確認し、同時に複数ジョブが走っていないか（`concurrency` で直列化済み）を見てください。

## アプリ側（app-scp-docs）

一覧生成スクリプトは **本リポジトリのみ**で管理しています。アプリの `AppRemoteConfig.scpListJSONURLString` は本リポジトリの GitHub Pages URL を指してください。
