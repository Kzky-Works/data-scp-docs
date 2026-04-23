# data-scp-docs

[GitHub Pages](https://kzky-works.github.io/data-scp-docs/) 向けの **静的 JSON 配信**用リポジトリです。

## 方針（2026-04）

**Wikidot 向けのデータ収集・生成スクリプトおよび GitHub Actions ワークフローは削除済み**です。  
`docs/` 以下のファイル更新は、**別のパイプライン・手作業・別リポジトリ**で行い、ここへコミットする運用に切り替えています。

## 主な配信パス（Pages 上の URL）

| リポジトリ内パス | 例（ホスト直下からのパス） |
|------------------|----------------------------|
| `docs/scp_list.json` | `/scp_list.json`（既定ブランチのルート公開に依存。運用に合わせて調整） |
| `docs/catalog/*.json` | `/catalog/…`（`wikiCatalogBaseURLString` 等と整合） |
| `docs/list/jp/*.json` | `/list/jp/scp-jp.json` など（ScpDocs 3 系統フィード） |

※ GitHub Pages の **公開ルートが `main` の `/` か `gh-pages` の `/` か**で URL が変わります。アプリの `AppRemoteConfig` と実際の Pages 設定を一致させてください。

## `docs/catalog/` のファイル（参考）

| ファイル | 内容の概要 |
|----------|------------|
| `scp_jp.json` | `/scp-N-jp` 系 |
| `scp.json` | 本家メイン和訳 `/scp-N` 系 |
| `joke.json` | `-j` 系 |
| `tales.json` / `canon.json` / `goi.json` | 番号形式以外のスラッグ中心 |

## アプリ（app-scp-docs）

`AppRemoteConfig` のベース URL・パスは、**上記の実際の配信レイアウト**に合わせて設定してください。
