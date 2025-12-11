-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog: カタログとスキーマのセットアップ
-- MAGIC
-- MAGIC このノートブックでは、Paper08再現プロジェクト用のカタログとスキーマを作成します。
-- MAGIC
-- MAGIC ## Unity Catalog 3階層構造
-- MAGIC
-- MAGIC ```
-- MAGIC reprod_paper08 (catalog)
-- MAGIC   ├── bronze (schema)
-- MAGIC   ├── silver (schema)
-- MAGIC   └── gold (schema)
-- MAGIC ```
-- MAGIC
-- MAGIC ## 実行内容
-- MAGIC 1. `reprod_paper08` カタログの作成
-- MAGIC 2. `bronze`, `silver`, `gold` スキーマの作成
-- MAGIC 3. 設定の確認

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. カタログ作成

-- COMMAND ----------

-- reprod_paper08 カタログを作成（既に存在する場合はスキップ）
CREATE CATALOG IF NOT EXISTS reprod_paper08
COMMENT 'Paper08: RA有病率と治療パターン再現プロジェクト - Nakajima et al., 2020, Modern Rheumatology';

-- COMMAND ----------

SELECT "カタログ 'reprod_paper08' を作成しました" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. スキーマ作成

-- COMMAND ----------

-- Bronze スキーマ（生データ）
CREATE SCHEMA IF NOT EXISTS reprod_paper08.bronze
COMMENT 'Bronze層: NDBレセプトデータを模したダミーデータ（10,000患者）';

-- Silver スキーマ（変換済みデータ）
CREATE SCHEMA IF NOT EXISTS reprod_paper08.silver
COMMENT 'Silver層: RA患者定義を適用した分析用データ（Definition 3: ICD-10 + DMARDs ≥2ヶ月）';

-- Gold スキーマ（分析結果）
CREATE SCHEMA IF NOT EXISTS reprod_paper08.gold
COMMENT 'Gold層: 年齢層別分析結果（論文Table 2-4再現）';

-- COMMAND ----------

SELECT "全スキーマ（bronze, silver, gold）を作成しました" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. カタログ・スキーマ情報確認

-- COMMAND ----------

-- カタログの詳細情報を表示
DESCRIBE CATALOG EXTENDED reprod_paper08;

-- COMMAND ----------

-- スキーマ一覧を確認
SHOW SCHEMAS IN reprod_paper08;

-- COMMAND ----------

-- 各スキーマの詳細情報
DESCRIBE SCHEMA EXTENDED reprod_paper08.bronze;

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED reprod_paper08.silver;

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED reprod_paper08.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. 既存テーブル確認

-- COMMAND ----------

-- Bronze スキーマのテーブル（初回実行時は空）
SHOW TABLES IN reprod_paper08.bronze;

-- COMMAND ----------

-- Silver スキーマのテーブル（初回実行時は空）
SHOW TABLES IN reprod_paper08.silver;

-- COMMAND ----------

-- Gold スキーマのテーブル（初回実行時は空）
SHOW TABLES IN reprod_paper08.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC カタログ `reprod_paper08` と3つのスキーマの作成が完了しました。
-- MAGIC
-- MAGIC ### 作成された構造
-- MAGIC
-- MAGIC ```
-- MAGIC reprod_paper08 (catalog)
-- MAGIC   ├── bronze (schema) - 生データ
-- MAGIC   ├── silver (schema) - 変換済みデータ
-- MAGIC   └── gold (schema) - 分析結果
-- MAGIC ```
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC 次のノートブックを実行してください：
-- MAGIC - `01_create_bronze_tables.sql` - Bronzeテーブルの作成
