-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer: テーブル定義（DDL）
-- MAGIC
-- MAGIC このノートブックでは、Gold層の分析結果テーブルを作成します。
-- MAGIC
-- MAGIC ## Gold層のテーブル構成
-- MAGIC
-- MAGIC | テーブル名 | 内容 | レコード数 |
-- MAGIC |-----------|------|-----------|
-- MAGIC | gold_table2_age_distribution | 年齢層別分布（Table 2） | 10（9年齢群+合計） |
-- MAGIC | gold_table3_medication | 年齢層別薬剤使用率（Table 3） | 10 |
-- MAGIC | gold_table4_procedures | 年齢層別手術実施率（Table 4） | 10 |
-- MAGIC | gold_summary | 主要結果サマリー | 6-8 |
-- MAGIC
-- MAGIC ## 論文との対応
-- MAGIC
-- MAGIC - **Table 2**: 年齢群別患者数、性別比、有病率
-- MAGIC - **Table 3**: 年齢群別薬剤使用パターン（MTX, SSZ, bDMARDs等）
-- MAGIC - **Table 4**: 年齢群別手術・検査実施率（TJR, 超音波等）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## データベース選択

-- COMMAND ----------

USE reprod_paper08;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. gold_table2_age_distribution（年齢層別分布）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_table2_age_distribution (
  age_group STRING COMMENT '年齢群（16-19, ..., 85+, Total）',
  n BIGINT COMMENT 'RA患者数',
  pct_of_total DOUBLE COMMENT '全RA患者に占める割合（%）',
  female_pct DOUBLE COMMENT '女性の割合（%）',
  fm_ratio DOUBLE COMMENT '女性/男性比',
  prevalence DOUBLE COMMENT '有病率（%、同年齢群の全人口に対する割合）',
  paper_pct DOUBLE COMMENT '論文値: 全RA患者に占める割合（%）',
  paper_prevalence DOUBLE COMMENT '論文値: 有病率（%）'
)
USING DELTA
COMMENT 'Gold層: Table 2 - 年齢層別RA患者分布、性別比、有病率'
LOCATION 'dbfs:/user/hive/warehouse/reprod_paper08.db/gold_table2_age_distribution';

-- COMMAND ----------

SELECT "Table gold_table2_age_distribution created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. gold_table3_medication（年齢層別薬剤使用率）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_table3_medication (
  age_group STRING COMMENT '年齢群',
  n BIGINT COMMENT 'RA患者数',
  MTX DOUBLE COMMENT 'MTX使用率（%）',
  SSZ DOUBLE COMMENT 'SSZ使用率（%）',
  BUC DOUBLE COMMENT 'BUC使用率（%）',
  TAC DOUBLE COMMENT 'TAC使用率（%）',
  IGT DOUBLE COMMENT 'IGT使用率（%）',
  LEF DOUBLE COMMENT 'LEF使用率（%）',
  TNFI DOUBLE COMMENT 'TNFI使用率（%）',
  IL6I DOUBLE COMMENT 'IL6I使用率（%）',
  ABT DOUBLE COMMENT 'ABT使用率（%）',
  JAKi DOUBLE COMMENT 'JAKi使用率（%）',
  CS DOUBLE COMMENT 'コルチコステロイド使用率（%）',
  bDMARDs DOUBLE COMMENT 'bDMARDs全体の使用率（%）',
  TNFI_ABT_ratio DOUBLE COMMENT 'TNFI/ABT使用比率'
)
USING DELTA
COMMENT 'Gold層: Table 3 - 年齢層別薬剤使用パターン'
LOCATION 'dbfs:/user/hive/warehouse/reprod_paper08.db/gold_table3_medication';

-- COMMAND ----------

SELECT "Table gold_table3_medication created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. gold_table4_procedures（年齢層別手術実施率）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_table4_procedures (
  age_group STRING COMMENT '年齢群',
  n BIGINT COMMENT 'RA患者数',
  TJR DOUBLE COMMENT '人工関節全置換術実施率（%）',
  ARTHROPLASTY DOUBLE COMMENT '関節形成術実施率（%）',
  SYNOVECTOMY DOUBLE COMMENT '滑膜切除術実施率（%）',
  ULTRASOUND DOUBLE COMMENT '関節超音波検査実施率（%）',
  BMD DOUBLE COMMENT '骨密度測定実施率（%）',
  any_RA_surgery DOUBLE COMMENT 'RA関連手術実施率（%）'
)
USING DELTA
COMMENT 'Gold層: Table 4 - 年齢層別手術・検査実施率'
LOCATION 'dbfs:/user/hive/warehouse/reprod_paper08.db/gold_table4_procedures';

-- COMMAND ----------

SELECT "Table gold_table4_procedures created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. gold_summary（主要結果サマリー）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_summary (
  metric STRING COMMENT '指標名',
  reproduced STRING COMMENT '再現値',
  paper STRING COMMENT '論文値',
  note STRING COMMENT '備考'
)
USING DELTA
COMMENT 'Gold層: 主要結果サマリー - 論文値との比較'
LOCATION 'dbfs:/user/hive/warehouse/reprod_paper08.db/gold_summary';

-- COMMAND ----------

SELECT "Table gold_summary created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 作成済みテーブル一覧

-- COMMAND ----------

SHOW TABLES IN reprod_paper08 LIKE 'gold*';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## テーブル詳細確認

-- COMMAND ----------

-- Table 2のスキーマ確認
DESCRIBE EXTENDED gold_table2_age_distribution;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC Gold層の全テーブルの作成が完了しました。
-- MAGIC
-- MAGIC ### 作成されたテーブル
-- MAGIC 1. ✅ gold_table2_age_distribution
-- MAGIC 2. ✅ gold_table3_medication
-- MAGIC 3. ✅ gold_table4_procedures
-- MAGIC 4. ✅ gold_summary
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC 次のノートブックを実行して分析を実施してください：
-- MAGIC - `02_analysis_and_visualization.py` - 年齢層別分析と可視化
