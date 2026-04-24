# data-scp-docs

[GitHub Pages](https://kzky-works.github.io/data-scp-docs/) 向けの JSON 配信リポジトリです。

## 支部別リスト（`list/<code>/`）

マニフェスト（`schemaVersion: 2`）のみ配信します。各ファイルは `entries`（`u` / `i` / `t`）と、必要な記事だけの `metadata`（主キーは `i`）を含みます。

| パス | 内容 |
|------|------|
| `list/jp/manifest_scp-jp.json` | 日本支部オリジナル（`/scp-nnn-jp`） |
| `list/jp/manifest_scp-main.json` | 本家メイン和訳（`/scp-nnn`） |
| `list/jp/manifest_scp-int.json` | 国際支部和訳（`/scp-nnn-xx`、ハブから辿る一覧のスクレイプ） |
| `list/jp/manifest_tales.json` | Tales-JP（`foundation-tales-jp`） |
| `list/jp/manifest_gois.json` | GoI 形式（`goi-format` タグ一覧） |

他支部（例: `ru`）は `BranchConfig` の `code` / `site_host` / `output_dir` を差し替えて `harvester.py` を拡張する想定です。

## 収集（`scripts/harvester.py`）

- **基礎層:** `scp-series-jp` 系・`scp-series` 系の一覧から `u` / `i` / `t`。
- **支部の補助:** 同番号の本家 `/scp-n` 一覧タイトルを `metadata` の `o` に載せる（支部 `t` と異なる場合のみ）。
- **属性層:** `system:page-tags/tag/<object-class>` を巡回し、該当パスへ `metadata` の `c` を付与。
- **国際:** `/scp-international` から辿った各言語一覧ページをクロール（`scp_list.json` は不要）。
- **Tale:** `foundation-tales-jp` を HTML パースし、著者は `metadata` の `a` に載せる。

```bash
pip install -r requirements.txt
python3 scripts/harvester.py
python3 scripts/validate_manifests.py
```

## GitHub Actions

| Workflow | 内容 |
|----------|------|
| **Update list feeds** (`update.yml`) | **毎日 00:00 UTC** ＋手動。`harvester.py` → `validate_manifests.py` の後、`list/jp/` のみ差分コミット。 |

## アプリ（app-scp-docs）

`AppRemoteConfig` の `scpDataHostBaseURLString` と、`list/jp/manifest_*.json` のパスを一致させてください。
