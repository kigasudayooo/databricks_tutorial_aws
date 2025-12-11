# Unity Catalog移行ガイド

## 概要

このプロジェクトをUnity Catalog（3階層構造）で実行する場合の修正ガイドです。

## 構造の違い

### Hive Metastore（2階層）- 元の実装
```
reprod_paper08 (database)
  └── bronze_patients (table)
  └── bronze_re_receipt (table)
  └── silver_ra_patients_def3 (table)
  └── gold_summary (table)
```

### Unity Catalog（3階層）- 推奨
```
reprod_paper08 (catalog)
  ├── bronze (schema)
  │   └── patients (table)
  │   └── re_receipt (table)
  │   └── sy_disease (table)
  │   └── iy_medication (table)
  │   └── si_procedure (table)
  │   └── ho_insurer (table)
  ├── silver (schema)
  │   └── ra_patients_def3 (table)
  │   └── ra_definitions_summary (table)
  └── gold (schema)
      └── table2_age_distribution (table)
      └── table3_medication (table)
      └── table4_procedures (table)
      └── summary (table)
```

## 必要な修正箇所

### 1. セットアップファイル

**新規作成**: `bronze/00_setup_catalog.sql`（既に作成済み）

```sql
-- カタログ作成
CREATE CATALOG IF NOT EXISTS reprod_paper08;

-- スキーマ作成
CREATE SCHEMA IF NOT EXISTS reprod_paper08.bronze;
CREATE SCHEMA IF NOT EXISTS reprod_paper08.silver;
CREATE SCHEMA IF NOT EXISTS reprod_paper08.gold;
```

### 2. テーブル名の変更パターン

全てのファイルで以下のパターンで置換：

| 元のテーブル名 | Unity Catalog対応 |
|--------------|------------------|
| `bronze_patients` | `reprod_paper08.bronze.patients` |
| `bronze_re_receipt` | `reprod_paper08.bronze.re_receipt` |
| `bronze_sy_disease` | `reprod_paper08.bronze.sy_disease` |
| `bronze_iy_medication` | `reprod_paper08.bronze.iy_medication` |
| `bronze_si_procedure` | `reprod_paper08.bronze.si_procedure` |
| `bronze_ho_insurer` | `reprod_paper08.bronze.ho_insurer` |
| `silver_ra_patients_def3` | `reprod_paper08.silver.ra_patients_def3` |
| `silver_ra_definitions_summary` | `reprod_paper08.silver.ra_definitions_summary` |
| `gold_table2_age_distribution` | `reprod_paper08.gold.table2_age_distribution` |
| `gold_table3_medication` | `reprod_paper08.gold.table3_medication` |
| `gold_table4_procedures` | `reprod_paper08.gold.table4_procedures` |
| `gold_summary` | `reprod_paper08.gold.summary` |

### 3. 修正が必要なファイル

#### A. Bronze層

**`01_create_bronze_tables.sql`**

修正前:
```sql
USE reprod_paper08;

CREATE TABLE IF NOT EXISTS bronze_patients (
  ...
) USING DELTA;
```

修正後:
```sql
CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.patients (
  ...
) USING DELTA;
```

**`02_generate_bronze_data.py`**

修正前:
```python
spark.sql("USE reprod_paper08")
df_patients.write.format("delta").mode("overwrite").saveAsTable("bronze_patients")
```

修正後:
```python
df_patients.write.format("delta").mode("overwrite").saveAsTable("reprod_paper08.bronze.patients")
```

#### B. Silver層

**`01_create_silver_tables.sql`**

修正前:
```sql
USE reprod_paper08;

CREATE TABLE IF NOT EXISTS silver_ra_patients_def3 (
  ...
) USING DELTA;
```

修正後:
```sql
CREATE TABLE IF NOT EXISTS reprod_paper08.silver.ra_patients_def3 (
  ...
) USING DELTA;
```

**`02_transform_ra_patients.sql`**

修正前:
```sql
USE reprod_paper08;

SELECT DISTINCT 共通キー
FROM bronze_sy_disease
WHERE ICD10コード IN ...;
```

修正後:
```sql
SELECT DISTINCT 共通キー
FROM reprod_paper08.bronze.sy_disease
WHERE ICD10コード IN ...;
```

#### C. Gold層

**`01_create_gold_tables.sql`**

修正前:
```sql
USE reprod_paper08;

CREATE TABLE IF NOT EXISTS gold_table2_age_distribution (
  ...
) USING DELTA;
```

修正後:
```sql
CREATE TABLE IF NOT EXISTS reprod_paper08.gold.table2_age_distribution (
  ...
) USING DELTA;
```

**`02_analysis_and_visualization.py`**

修正前:
```python
spark.sql("USE reprod_paper08")
df_ra = spark.table("silver_ra_patients_def3")
```

修正後:
```python
df_ra = spark.table("reprod_paper08.silver.ra_patients_def3")
```

### 4. 一括置換コマンド（参考）

各ファイルで以下のパターンを検索・置換：

**Bronze層**:
```
検索: FROM bronze_(\w+)
置換: FROM reprod_paper08.bronze.$1

検索: JOIN bronze_(\w+)
置換: JOIN reprod_paper08.bronze.$1

検索: saveAsTable\("bronze_(\w+)"\)
置換: saveAsTable("reprod_paper08.bronze.$1")
```

**Silver層**:
```
検索: FROM silver_(\w+)
置換: FROM reprod_paper08.silver.$1

検索: saveAsTable\("silver_(\w+)"\)
置換: saveAsTable("reprod_paper08.silver.$1")
```

**Gold層**:
```
検索: FROM gold_(\w+)
置換: FROM reprod_paper08.gold.$1

検索: saveAsTable\("gold_(\w+)"\)
置換: saveAsTable("reprod_paper08.gold.$1")
```

## 推奨する実行手順

### オプション1: 既存実装をそのまま使用（Hive Metastore）

Unity Catalogが不要な場合は、既存のファイル（`00_setup_database.sql`）をそのまま使用してください。

### オプション2: Unity Catalog対応版を使用

1. **00_setup_catalog.sql** を実行（カタログとスキーマ作成）
2. 他の全ファイルでテーブル名を3階層に修正
3. 順番に実行

### オプション3: 簡易版を使用（推奨）

新規に作成した簡易版のファイルを使用：
- 全てのテーブル名が3階層対応済み
- `USE` 文を削除済み
- フルパスでテーブル参照

## 検証方法

```sql
-- カタログの確認
SHOW CATALOGS;

-- スキーマの確認
SHOW SCHEMAS IN reprod_paper08;

-- テーブルの確認
SHOW TABLES IN reprod_paper08.bronze;
SHOW TABLES IN reprod_paper08.silver;
SHOW TABLES IN reprod_paper08.gold;

-- データの確認
SELECT COUNT(*) FROM reprod_paper08.bronze.patients;
SELECT COUNT(*) FROM reprod_paper08.silver.ra_patients_def3;
SELECT * FROM reprod_paper08.gold.summary;
```

## トラブルシューティング

### 問題: カタログ作成権限がない

**エラー**: `Error: Permission denied to create catalog`

**解決策**:
1. Unity Catalog Metastore が有効になっているか確認
2. ワークスペース管理者に権限を依頼
3. または、既存のカタログ（`main` など）を使用

### 問題: テーブルが見つからない

**エラー**: `Table or view not found: bronze_patients`

**原因**: 2階層の名前を使用している

**解決策**: フルパス（`reprod_paper08.bronze.patients`）を使用

## Unity Catalogの利点

1. **3階層の名前空間**: catalog.schema.table
2. **細かいアクセス制御**: カタログ、スキーマ、テーブル単位で権限設定
3. **データガバナンス**: 自動的にデータリネージが記録される
4. **クロスワークスペース共有**: カタログを複数ワークスペースで共有可能

## まとめ

- **Unity Catalogを使う場合**: `00_setup_catalog.sql` を使用し、全テーブル名を3階層に修正
- **Hive Metastoreを使う場合**: 既存の `00_setup_database.sql` をそのまま使用

どちらの方法でも、メダリオンアーキテクチャとデータフローは同じです。
