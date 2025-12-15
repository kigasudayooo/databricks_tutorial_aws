# クイックスタート: Databricks実行手順

このガイドでは、Databricksで論文再現を実行するための最短手順を説明します。

## 📋 前提条件

- **Unity Catalog** が有効なDatabricksワークスペース
- **CREATE CATALOG** 権限
- クラスター: DBR 13.3 LTS以上（Single Nodeで十分）

## 🚀 実行手順（4ステップ）

各層で**1つのノートブック**を実行するだけで、テーブル作成とデータ生成が完了します。

### Step 1: カタログとスキーマのセットアップ（30秒）

```sql
bronze/00_setup_catalog.sql
```

**実行内容**:
- カタログ `reprod_paper08` を作成
- スキーマ `bronze`, `silver`, `gold` を作成

**確認**:
```sql
SHOW SCHEMAS IN reprod_paper08;
-- bronze, silver, gold が表示される
```

---

### Step 2: Bronze層 - データ生成（3-5分）

```python
bronze/02_generate_bronze_data.py
```

**実行内容**:
1. 6つのBronzeテーブルを作成
2. 10,000人の患者データを生成
3. レセプト、傷病、薬剤、診療行為データを生成

**確認**:
```sql
SELECT COUNT(*) FROM reprod_paper08.bronze.patients;  -- 10,000
SELECT COUNT(*) FROM reprod_paper08.bronze.sy_disease;  -- ~63,000
```

---

### Step 3: Silver層 - RA患者抽出（1-2分）

```sql
silver/02_transform_ra_patients.sql
```

**実行内容**:
1. 2つのSilverテーブルを作成
2. RA患者定義（Definition 3）を適用
3. DMARDs処方月数を計算
4. RA患者約650人を抽出

**確認**:
```sql
SELECT COUNT(*) FROM reprod_paper08.silver.ra_patients_def3;  -- ~650
SELECT * FROM reprod_paper08.silver.ra_definitions_summary;
```

---

### Step 4: Gold層 - 分析と可視化（1-2分）

```python
gold/02_analysis_and_visualization.py
```

**実行内容**:
1. 4つのGoldテーブルを作成
2. Table 2: 年齢層別有病率と性別比
3. Table 3: 年齢層別薬剤使用率
4. Table 4: 年齢層別手術実施率
5. グラフ可視化

**確認**:
```sql
-- 主要結果サマリー
SELECT * FROM reprod_paper08.gold.summary;

-- 年齢層別分布（Table 2）
SELECT * FROM reprod_paper08.gold.table2_age_distribution;

-- 年齢層別薬剤使用率（Table 3）
SELECT * FROM reprod_paper08.gold.table3_medication;

-- 年齢層別手術実施率（Table 4）
SELECT * FROM reprod_paper08.gold.table4_procedures;
```

---

## ✅ 実行完了！

**総実行時間**: 約5-8分

**作成されたテーブル**: 合計12テーブル
- Bronze: 6テーブル（約134,000レコード）
- Silver: 2テーブル（約650レコード）
- Gold: 4テーブル（約40レコード）

---

## 📊 主要な再現結果

| 指標 | 論文値 | 期待される再現値 |
|------|--------|-----------------|
| RA有病率 | 0.65% | ~0.60-0.70% |
| 女性比率 | 76.3% | ~72-80% |
| MTX使用率 | 63.4% | ~58-68% |
| bDMARDs使用率 | 22.9% | ~20-26% |

**注**: ダミーデータは確率的に生成されるため、実行ごとに多少のばらつきがあります。

---

## 🔄 再実行する場合

各ノートブックは `CREATE OR REPLACE TABLE` または `mode("overwrite")` を使用しているため、何度でも再実行できます。

```python
# Bronze層の再生成
bronze/02_generate_bronze_data.py  # 3-5分

# Silver層の再計算
silver/02_transform_ra_patients.sql  # 1-2分

# Gold層の再分析
gold/02_analysis_and_visualization.py  # 1-2分
```

---

## 📚 詳細なドキュメント

より詳細な情報は [README.md](README.md) を参照してください。

---

## ❓ トラブルシューティング

### エラー: "Catalog 'reprod_paper08' does not exist"
→ `bronze/00_setup_catalog.sql` を実行してください

### エラー: "Table or view not found: reprod_paper08.bronze.patients"
→ Bronze層のデータ生成 (`bronze/02_generate_bronze_data.py`) を実行してください

### エラー: "Permission denied: CREATE CATALOG"
→ Databricks管理者にUnity Catalog の `CREATE CATALOG` 権限を依頼してください

---

## 🎯 Unity Catalog構造

```
reprod_paper08 (catalog)
  ├── bronze (schema)
  │   ├── patients (10,000)
  │   ├── re_receipt (~25,000)
  │   ├── sy_disease (~63,000)
  │   ├── iy_medication (~1,300)
  │   ├── si_procedure (~25,500)
  │   └── ho_insurer (~10,000)
  ├── silver (schema)
  │   ├── ra_patients_def3 (~650)
  │   └── ra_definitions_summary (4)
  └── gold (schema)
      ├── table2_age_distribution (10)
      ├── table3_medication (10)
      ├── table4_procedures (10)
      └── summary (6-8)
```

---

**完璧な分析をお楽しみください！** 🎉
