# data-scp-docs

`scp_list.json`（ScpDocs アプリ用リモート一覧）を GitHub Pages で配信するリポジトリです。

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | **毎回** [app-scp-docs](https://github.com/Kzky-Works/app-scp-docs) の `main` から `update_list.py` を取得 → シリーズ一覧＋国際ハブを取得。**毎週月曜 18:00 UTC** ＋手動。 |
| **Update scp_list.json (with article metadata)** | 上と同様にスクリプト同期のうえ、各記事からタグ・オブジェクトクラス取得。**手動のみ**（数時間かかる場合あり）。 |

変更があるときだけ `scripts/update_list.py` と／または `docs/scp_list.json` がコミットされ、Pages が更新されます。

### スクリプトの「正」

**ソース・オブ・トゥルースは [app-scp-docs](https://github.com/Kzky-Works/app-scp-docs) ルートの `update_list.py`** です。  
このリポジトリの `scripts/update_list.py` は **ワークフロー実行時に毎回そこから取得**されます（手動コピー不要）。

### app-scp-docs がプライベートのとき

`raw.githubusercontent.com` から取れないため、**Fine-grained PAT 等**（`app-scp-docs` の Contents: Read）をこのリポジトリの Secret に **`APP_SCP_DOCS_READ_TOKEN`** として保存してください。設定があると GitHub API 経由でファイルを取得します。

## ローカル開発（app-scp-docs と揃える）

app-scp-docs リポジトリに同梱の `tools/sync_update_list_data_repo.sh` を使うと、隣の `data-scp-docs` に **main と同じ内容**を取り込めます。

```bash
cd app-scp-docs   # ローカル clone 名
./tools/sync_update_list_data_repo.sh pull
```

未 push の `update_list.py` を試したいときは `copy`。

定期実行したい場合は macOS の `launchd` や cron で上記を週 1 などで呼び出してください。

### Actions の push が rejected になるとき

別のワークフローや手動 push で `main` が進んでいると、単純 `git push` が拒否されます。ワークフローでは **コミット後に `fetch` + `rebase origin/main` してから push** するようになっています。まだ失敗する場合は Actions のログを確認し、同時に複数ジョブが走っていないか（`concurrency` で直列化済み）を見てください。
