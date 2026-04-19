# data-scp-docs

`scp_list.json`（ScpDocs アプリ用リモート一覧）を GitHub Pages で配信するリポジトリです。

## 自動更新（GitHub Actions）

| Workflow | 内容 |
|----------|------|
| **Update scp_list.json** | シリーズ一覧＋国際ハブのみ取得。**毎週月曜 18:00 UTC** に実行＋手動可。 |
| **Update scp_list.json (with article metadata)** | 各記事からタグ・オブジェクトクラス取得。**手動のみ**（数時間かかる場合あり）。 |

生成物は `docs/scp_list.json`。変更があるときだけ `main` にコミットされ、Pages が更新されます。

`scripts/update_list.py` は [scp_docs](https://github.com/Kzky-Works/app-scp-docs) 側のスクリプトと同じロジックです。仕様変更時は両方を揃えてください。
