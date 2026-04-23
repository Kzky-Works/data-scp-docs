# data-scp-docs

[GitHub Pages](https://kzky-works.github.io/data-scp-docs/) 向けの JSON 配信リポジトリです。

## 支部別リスト（`list/<code>/`）

| パス | 内容 |
|------|------|
| `list/jp/scp-jp.json` | 日本支部オリジナル（`SCPArticleListPayload`） |
| `list/jp/scp.json` | 本家メイン和訳 |
| `list/jp/scp-int.json` | 国際支部パス（`hubLinkedPaths` 前提） |
| `list/jp/tales.json` | Tales-JP（`SCPGeneralContentListPayload`） |
| `list/jp/gois.json` | GoI 形式（`goi-format` タグ由来、`SCPGeneralContentListPayload`） |

他支部（例: `ru`）は `BranchConfig` の `code` / `site_host` / `output_dir` を差し替えて `harvester.py` を拡張する想定です。

## 収集（`scripts/harvester.py`）

- **基礎層:** `scp-series-jp` 系・`scp-series` 系の一覧から `u` / `i` / `t`。
- **属性層:** `system:page-tags/tag/<object-class>` を優先順で巡回し、一覧に含まれるパスへ `c` を付与（`docs/scp_list.json` があれば `c` / `g` / `o` をマージ）。
- **国際:** `docs/scp_list.json` の `hubLinkedPaths` ＋国際一覧クロールで `scp-int.json` の `t`。
- **著者層:** `foundation-tales-jp` を HTML パースし Tale に `a`（著者）を付与。

```bash
pip install -r requirements.txt
python3 scripts/harvester.py --scp-list docs/scp_list.json
```

## GitHub Actions

| Workflow | 内容 |
|----------|------|
| **Update list feeds** (`update.yml`) | **毎日 00:00 UTC** ＋手動。`harvester.py` 実行後、`list/jp/` のみ差分コミット。 |

`docs/scp_list.json` が無い、または `hubLinkedPaths` が空の場合、`scp-int.json` は空に近くなることがあります。

## アプリ（app-scp-docs）

`AppRemoteConfig` の `scpDataHostBaseURLString` と、`list/jp/…` パス（3 系統＋`tales` / `gois`）を一致させてください。
