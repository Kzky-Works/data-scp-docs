from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import build_jp_wikidot_tag_article_map as tag_map  # noqa: E402


TAG_LIST_FIXTURE = """
<html>
<body>
<div id="page-content">
  <div id="wiki-tabview-main" class="yui-navset">
    <ul class="yui-nav">
      <li class="selected"><a href="javascript:;"><em>最小化</em></a></li>
      <li><a href="javascript:;"><em>メジャー</em></a></li>
      <li><a href="javascript:;"><em>オブジェクトクラス</em></a></li>
      <li><a href="javascript:;"><em>アトリビュート</em></a></li>
      <li><a href="javascript:;"><em>支部</em></a></li>
      <li><a href="javascript:;"><em>FAQ</em></a></li>
    </ul>
    <div class="yui-content">
      <div id="wiki-tab-0-0"></div>
      <div id="wiki-tab-0-1">
        <h2><span>創作物</span></h2>
        <ul>
          <li><strong><a href="/system:page-tags/tag/scp">scp</a></strong></li>
          <li><strong><a href="/system:page-tags/tag/tale">tale</a></strong></li>
          <li><strong><a href="/system:page-tags/tag/not-in-jp-tag">not-in-jp-tag</a></strong></li>
        </ul>
      </div>
      <div id="wiki-tab-0-2">
        <h2><span>基本クラス</span></h2>
        <ul>
          <li><strong><a href="/system:page-tags/tag/safe">safe</a></strong></li>
          <li><strong><a href="/system:page-tags/tag/euclid">euclid</a></strong></li>
        </ul>
      </div>
      <div id="wiki-tab-0-3">
        <h2><span>存在</span></h2>
        <ul>
          <li><strong><a href="/system:page-tags/tag/%E4%BA%BA%E5%9E%8B">人型</a></strong></li>
        </ul>
      </div>
      <div id="wiki-tab-0-4">
        <h2><span>支部タグ</span></h2>
        <ul>
          <li><strong><a href="/system:page-tags/tag/jp">jp</a></strong></li>
        </ul>
      </div>
      <div id="wiki-tab-0-5">
        <h2><span>無視される領域</span></h2>
        <ul>
          <li><strong><a href="/system:page-tags/tag/faq-only">faq-only</a></strong></li>
        </ul>
      </div>
    </div>
  </div>
</div>
<div id="page-info">
  ページリビジョン: 261, 最終更新:
  <span class="odate time_1738371674 format">01 Feb 2025 01:01</span>
</div>
</body>
</html>
"""


class TagTaxonomyTests(unittest.TestCase):
    def test_parse_tag_taxonomy_filters_to_jp_tag_tags(self) -> None:
        allowed = {"scp", "tale", "safe", "euclid", "人型", "jp"}
        taxonomy, updated_at = tag_map.parse_tag_taxonomy_html(TAG_LIST_FIXTURE, allowed)

        self.assertEqual(updated_at, 1738371674)
        self.assertTrue(tag_map.validate_tag_taxonomy(taxonomy, allowed))

        by_pair = {(g["category"], g["subcategory"]): g["tags"] for g in taxonomy}
        self.assertEqual(by_pair[("メジャー", "創作物")], ["scp", "tale"])
        self.assertEqual(by_pair[("オブジェクトクラス", "基本クラス")], ["safe", "euclid"])
        self.assertEqual(by_pair[("アトリビュート", "存在")], ["人型"])
        self.assertEqual(by_pair[("支部", "支部タグ")], ["jp"])

        flat = {tag for group in taxonomy for tag in group["tags"]}
        self.assertNotIn("not-in-jp-tag", flat)
        self.assertNotIn("faq-only", flat)

    def test_partial_previous_taxonomy_fields_are_reusable(self) -> None:
        previous = {
            "tagTaxonomySource": "scp-jp.wikidot.com/tag-list",
            "tagTaxonomyUpdatedAt": 1,
            "tagTaxonomy": [
                {"category": "メジャー", "subcategory": "創作物", "tags": ["scp"]}
            ],
            "ignored": True,
        }

        self.assertEqual(
            tag_map.taxonomy_fields_from_previous(previous),
            {
                "tagTaxonomySource": "scp-jp.wikidot.com/tag-list",
                "tagTaxonomyUpdatedAt": 1,
                "tagTaxonomy": [
                    {"category": "メジャー", "subcategory": "創作物", "tags": ["scp"]}
                ],
            },
        )


if __name__ == "__main__":
    unittest.main()
