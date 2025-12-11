-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer: テーブル定義（DDL） - Unity Catalog対応
-- MAGIC
-- MAGIC このノートブックでは、Silver層のテーブルを作成します。
-- MAGIC
-- MAGIC ## Unity Catalog 構造
-- MAGIC
-- MAGIC ```
-- MAGIC reprod_paper08 (catalog)
-- MAGIC   └── silver (schema)
-- MAGIC       ├── ra_patients_def3
-- MAGIC       └── ra_definitions_summary
-- MAGIC ```
-- MAGIC
-- MAGIC ## Silver層のテーブル構成
-- MAGIC
-- MAGIC | テーブル名 | 内容 | 予想レコード数 |
-- MAGIC |-----------|------|---------------|
-- MAGIC | ra_patients_def3 | Definition 3のRA患者マスタ | ~650 |
-- MAGIC | ra_definitions_summary | RA定義別患者数サマリー | 4 |
-- MAGIC
-- MAGIC ## RA患者定義
-- MAGIC
-- MAGIC **Definition 3（採用）**: ICD-10コード + DMARDs処方 ≥2ヶ月
-- MAGIC - 有病率: 約0.65%
-- MAGIC - 患者数: 約650人（10,000人中）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 前提条件
-- MAGIC
-- MAGIC - `00_setup_catalog.sql` が実行済みであること
-- MAGIC - カタログ `reprod_paper08` とスキーマ `silver` が作成済みであること
-- MAGIC - Bronze層のデータが生成済みであること

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. ra_patients_def3（RA患者マスタ）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.silver.ra_patients_def3 (
  patient_id BIGINT COMMENT '患者ID',
  共通キー STRING COMMENT '共通キー（匿名化ID）',
  age INT COMMENT '年齢',
  age_group STRING COMMENT '年齢群',
  sex STRING COMMENT '性別（1: 男性, 2: 女性）',
  birth_date STRING COMMENT '生年月日',
  is_ra_candidate BOOLEAN COMMENT 'RA候補フラグ',

  -- DMARDs処方情報
  prescription_months INT COMMENT 'DMARDs処方月数',

  -- csDMARDs（従来型合成DMARD）使用フラグ
  MTX INT COMMENT 'メトトレキサート使用フラグ（0 or 1）',
  SSZ INT COMMENT 'サラゾスルファピリジン使用フラグ',
  BUC INT COMMENT 'ブシラミン使用フラグ',
  TAC INT COMMENT 'タクロリムス使用フラグ',
  IGT INT COMMENT 'イグラチモド使用フラグ',
  LEF INT COMMENT 'レフルノミド使用フラグ',

  -- bDMARDs（生物学的DMARD）使用フラグ
  TNFI INT COMMENT 'TNF阻害薬使用フラグ',
  IL6I INT COMMENT 'IL-6阻害薬使用フラグ',
  ABT INT COMMENT 'アバタセプト使用フラグ',

  -- tsDMARDs（分子標的型合成DMARD）使用フラグ
  JAKi INT COMMENT 'JAK阻害薬使用フラグ',

  -- その他の薬剤
  CS INT COMMENT 'コルチコステロイド使用フラグ',

  -- 複合フラグ
  bDMARDs INT COMMENT '生物学的DMARD全体の使用フラグ',
  any_DMARD INT COMMENT '何らかのDMARD使用フラグ',

  -- 診療行為（手術・検査）フラグ
  TJR INT COMMENT '人工関節全置換術フラグ',
  ARTHROPLASTY INT COMMENT '関節形成術フラグ',
  SYNOVECTOMY INT COMMENT '滑膜切除術フラグ',
  ULTRASOUND INT COMMENT '関節超音波検査フラグ',
  BMD INT COMMENT '骨密度測定フラグ',
  any_RA_surgery INT COMMENT 'RA関連手術フラグ（TJR or ARTHROPLASTY or SYNOVECTOMY）'
)
USING DELTA
COMMENT 'Silver層: Definition 3 によるRA患者マスタ（ICD-10 + DMARDs ≥2ヶ月）';

-- COMMAND ----------

SELECT "Table reprod_paper08.silver.ra_patients_def3 created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. ra_definitions_summary（RA定義別サマリー）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.silver.ra_definitions_summary (
  definition STRING COMMENT 'RA定義名（def_0, def_2, def_3, def_4）',
  n_patients BIGINT COMMENT '該当患者数',
  prevalence_pct DOUBLE COMMENT '有病率（%）'
)
USING DELTA
COMMENT 'Silver層: RA定義別の患者数と有病率サマリー';

-- COMMAND ----------

SELECT "Table reprod_paper08.silver.ra_definitions_summary created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 作成済みテーブル一覧

-- COMMAND ----------

SHOW TABLES IN reprod_paper08.silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## テーブル詳細確認

-- COMMAND ----------

-- RA患者マスタのスキーマ確認
DESCRIBE EXTENDED reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC Silver層の全テーブルの作成が完了しました。
-- MAGIC
-- MAGIC ### 作成されたテーブル（Unity Catalog）
-- MAGIC 1. ✅ `reprod_paper08.silver.ra_patients_def3`
-- MAGIC 2. ✅ `reprod_paper08.silver.ra_definitions_summary`
-- MAGIC
-- MAGIC ### 検証クエリ
-- MAGIC ```sql
-- MAGIC -- 全テーブルの確認
-- MAGIC SHOW TABLES IN reprod_paper08.silver;
-- MAGIC
-- MAGIC -- カウント確認（データ変換後）
-- MAGIC SELECT COUNT(*) FROM reprod_paper08.silver.ra_patients_def3;  -- ~650
-- MAGIC ```
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC 次のノートブックを実行してデータを変換してください：
-- MAGIC - `02_transform_ra_patients.sql` - RA患者定義適用とSilverデータ作成
