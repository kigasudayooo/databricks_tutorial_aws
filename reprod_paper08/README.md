# Paper08 再現プロジェクト - Databricks実装

このプロジェクトは、論文「Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment」(Nakajima et al., 2020) をDatabricksで再現するものです。

## 📋 論文の概要

- **目的**: 日本における関節リウマチ（RA）の有病率を推定し、年齢層別の治療動向と臨床特性を明らかにする
- **データソース**: NDB Japan (2017年4月〜2018年3月)
- **主要結果**: RA有病率 0.65%、825,772人のRA患者を同定

## 🚀 クイックスタート（4ステップ）

Databricksで以下の順に実行するだけです：

### 1. カタログ作成（30秒）
```sql
databricks/bronze/00_setup_catalog.sql
```

### 2. Bronze層 - データ生成（3-5分）
```python
databricks/bronze/02_generate_bronze_data.py
```

### 3. Silver層 - RA患者抽出（1-2分）
```sql
databricks/silver/02_transform_ra_patients.sql
```

### 4. Gold層 - 分析（1-2分）
```python
databricks/gold/02_analysis_and_visualization.py
```

**総実行時間**: 約5-8分 🎉

## 📁 ディレクトリ構造

```text
reprod_paper08/
└── databricks/                    # Databricks実装（メイン）
    ├── bronze/                   # Bronze層（データ生成）
    │   ├── 00_setup_catalog.sql           # Unity Catalog設定
    │   ├── 01_create_bronze_tables.sql    # テーブル定義（参照用）
    │   └── 02_generate_bronze_data.py     # データ生成（実行用）
    ├── silver/                   # Silver層（RA患者抽出）
    │   ├── 01_create_silver_tables.sql    # テーブル定義（参照用）
    │   └── 02_transform_ra_patients.sql   # データ変換（実行用）
    ├── gold/                     # Gold層（分析）
    │   └── 02_analysis_and_visualization.py  # 分析・可視化（実行用）
    ├── config/                   # 共通設定
    │   └── constants.py                   # 定数定義
    ├── QUICKSTART.md            # 最短実行手順
    └── README.md                # 詳細ドキュメント
```

## 📊 生成されるデータ

### Unity Catalog構造

```text
reprod_paper08 (catalog)
  ├── bronze (schema) - 6テーブル、約134,000レコード
  ├── silver (schema) - 2テーブル、約650レコード
  └── gold (schema)   - 4テーブル、約40レコード
```

### 主要結果

| 指標 | 論文値 | 期待される再現値 |
|------|--------|-----------------|
| RA有病率 | 0.65% | ~0.60-0.70% |
| 女性比率 | 76.3% | ~72-80% |
| MTX使用率 | 63.4% | ~58-68% |
| bDMARDs使用率 | 22.9% | ~20-26% |

## 🔍 RA患者定義

論文では複数のRA定義を検討し、**Definition 3**を採用：

| 定義 | 条件 | 期待される有病率 |
|------|------|--------|
| Definition 0 | ICD-10コードのみ | ~0.8-0.9% |
| Definition 2 | ICD-10 + DMARDs ≥1ヶ月 | ~0.69% |
| **Definition 3** | **ICD-10 + DMARDs ≥2ヶ月** | **~0.65%** |
| Definition 4 | ICD-10 + DMARDs ≥6ヶ月 | ~0.46% |

### ICD-10コード

- M05.x: 血清反応陽性関節リウマチ
- M06.x (M061, M064除く): その他の関節リウマチ
- M08.x (M081, M082除く): 若年性関節炎

## 📚 ドキュメント

- **[QUICKSTART.md](databricks/QUICKSTART.md)** - 最短実行手順（推奨）
- **[README.md](databricks/README.md)** - 詳細な技術ドキュメント

## ✅ 前提条件

- **Unity Catalog** が有効なDatabricksワークスペース
- **CREATE CATALOG** 権限
- DBR 13.3 LTS以上のクラスター

## 🎯 特徴

1. **完全自己完結**: 外部データ不要、全てPySparkで生成
2. **Unity Catalog対応**: 3階層構造（catalog.schema.table）
3. **ワンステップ実行**: 各層1ファイル実行で完結
4. **冪等性**: 何度でも再実行可能

## ⚠️ 注意事項

- 本プロジェクトのデータは**完全なダミーデータ**です
- 論文の統計値を参考に分布を設定していますが、実データではありません
- 研究フローの理解とDatabricks実装の練習を目的としています

## 📖 参考文献

1. Nakajima A, et al. (2020) Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment. Modern Rheumatology.

---

**完璧な分析をお楽しみください！** 🎉
