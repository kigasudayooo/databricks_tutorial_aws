-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer: データベースセットアップ
-- MAGIC
-- MAGIC このノートブックでは、Paper08再現プロジェクト用のデータベースを作成します。
-- MAGIC
-- MAGIC ## 実行内容
-- MAGIC 1. `reprod_paper08` データベースの作成
-- MAGIC 2. データベースの選択
-- MAGIC 3. 設定の確認

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. データベース作成

-- COMMAND ----------

-- reprod_paper08 データベースを作成（既に存在する場合はスキップ）
CREATE DATABASE IF NOT EXISTS reprod_paper08
COMMENT 'Paper08: RA有病率と治療パターン再現プロジェクト - Nakajima et al., 2020, Modern Rheumatology'
LOCATION 'dbfs:/user/hive/warehouse/reprod_paper08.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. データベース選択

-- COMMAND ----------

-- 作成したデータベースを使用
USE reprod_paper08;

SELECT "データベース 'reprod_paper08' を選択しました" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. データベース情報確認

-- COMMAND ----------

-- データベースの詳細情報を表示
DESCRIBE DATABASE EXTENDED reprod_paper08;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. 既存テーブル一覧

-- COMMAND ----------

-- 既存のテーブルを確認（初回実行時は空）
SHOW TABLES IN reprod_paper08;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC データベース `reprod_paper08` の作成が完了しました。
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC 次のノートブックを実行してください：
-- MAGIC - `01_create_bronze_tables.sql` - Bronzeテーブルの作成
