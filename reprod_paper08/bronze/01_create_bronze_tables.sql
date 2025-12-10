-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer: テーブル定義（DDL） - Unity Catalog対応
-- MAGIC
-- MAGIC このノートブックでは、Bronze層の全テーブルをUnity Catalog形式で作成します。
-- MAGIC
-- MAGIC ## Bronze層のテーブル構成
-- MAGIC
-- MAGIC | テーブル名 | 内容 | 予想レコード数 |
-- MAGIC |-----------|------|---------------|
-- MAGIC | patients | 患者マスタ | 10,000 |
-- MAGIC | re_receipt | レセプト基本情報（RE） | ~25,000 |
-- MAGIC | sy_disease | 傷病名（SY: ICD-10） | ~63,000 |
-- MAGIC | iy_medication | 医薬品情報（IY） | ~1,300 |
-- MAGIC | si_procedure | 診療行為（SI） | ~25,500 |
-- MAGIC | ho_insurer | 保険者情報（HO） | ~10,000 |
-- MAGIC
-- MAGIC 全てのテーブルは **Delta Lake形式** で作成され、Unity Catalogで管理されます。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 前提条件
-- MAGIC
-- MAGIC - `00_setup_catalog.sql` が実行済みであること
-- MAGIC - カタログ `reprod_paper08` とスキーマ `bronze` が作成済みであること

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. patients（患者マスタ）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.patients (
  patient_id BIGINT COMMENT '患者ID（0から連番）',
  共通キー STRING COMMENT '共通キー（ハッシュ化された匿名ID）',
  age INT COMMENT '年齢（2017年10月1日時点）',
  age_group STRING COMMENT '年齢群（16-19, 20-29, ..., 85+）',
  sex STRING COMMENT '性別（1: 男性, 2: 女性）',
  birth_date STRING COMMENT '生年月日（YYYY-MM-DD形式）',
  is_ra_candidate BOOLEAN COMMENT 'RA候補フラグ（true: RA候補として生成された患者）'
)
USING DELTA
COMMENT 'Bronze層: 患者マスタテーブル（10,000人のダミーデータ）';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.patients created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. re_receipt（レセプト基本情報）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.re_receipt (
  共通キー STRING COMMENT '共通キー（患者識別子）',
  検索番号 STRING COMMENT '検索番号（レセプト識別子）',
  データ識別 STRING COMMENT 'データ識別（1: 医科, 2: DPC）',
  診療年月 STRING COMMENT '診療年月（YYYYMM形式）',
  男女区分 STRING COMMENT '性別（1: 男性, 2: 女性）',
  生年月日 STRING COMMENT '生年月日（YYYY-MM-DD形式）',
  fy STRING COMMENT '会計年度（2017）',
  year STRING COMMENT '診療年（YYYY）',
  month STRING COMMENT '診療月（MM）',
  prefecture_number STRING COMMENT '都道府県番号（01-47）'
)
USING DELTA
COMMENT 'Bronze層: レセプト基本情報（RE）テーブル - NDBレセプト共通レコード';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.re_receipt created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. sy_disease（傷病名）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.sy_disease (
  共通キー STRING COMMENT '共通キー（患者識別子）',
  検索番号 STRING COMMENT '検索番号（レセプト識別子）',
  ICD10コード STRING COMMENT 'ICD-10傷病名コード（M05.x, M06.x, M08.x など）',
  診療年月 STRING COMMENT '診療年月（YYYYMM形式）'
)
USING DELTA
COMMENT 'Bronze層: 傷病名（SY）テーブル - ICD-10コードを含む';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.sy_disease created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. iy_medication（医薬品情報）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.iy_medication (
  共通キー STRING COMMENT '共通キー（患者識別子）',
  検索番号 STRING COMMENT '検索番号（レセプト識別子）',
  医薬品コード STRING COMMENT '医薬品コード（YJコード相当）',
  drug_category STRING COMMENT '薬剤カテゴリ（csDMARD, TNFI, IL6I, ABT, JAKi, CS）',
  drug_name STRING COMMENT '薬剤名（MTX, SSZ, IFX, etc.）'
)
USING DELTA
COMMENT 'Bronze層: 医薬品情報（IY）テーブル - DMARDs、ステロイド等の処方記録';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.iy_medication created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. si_procedure（診療行為）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.si_procedure (
  共通キー STRING COMMENT '共通キー（患者識別子）',
  検索番号 STRING COMMENT '検索番号（レセプト識別子）',
  procedure_type STRING COMMENT '診療行為タイプ（TJR, ARTHROPLASTY, SYNOVECTOMY, ULTRASOUND, BMD, VISIT）'
)
USING DELTA
COMMENT 'Bronze層: 診療行為（SI）テーブル - 手術・検査の実施記録';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.si_procedure created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. ho_insurer（保険者情報）

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS reprod_paper08.bronze.ho_insurer (
  共通キー STRING COMMENT '共通キー（患者識別子）',
  insurer_type STRING COMMENT '保険者種別（社保, 国保, 後期高齢者）',
  insurer_number STRING COMMENT '保険者番号'
)
USING DELTA
COMMENT 'Bronze層: 保険者情報（HO）テーブル - 患者の保険種別';

-- COMMAND ----------

SELECT "Table reprod_paper08.bronze.ho_insurer created successfully" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 作成済みテーブル一覧

-- COMMAND ----------

SHOW TABLES IN reprod_paper08.bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## テーブル詳細確認

-- COMMAND ----------

-- 患者マスタのスキーマ確認
DESCRIBE EXTENDED reprod_paper08.bronze.patients;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC Bronze層の全6テーブルの作成が完了しました。
-- MAGIC
-- MAGIC ### 作成されたテーブル（Unity Catalog）
-- MAGIC 1. ✅ `reprod_paper08.bronze.patients`
-- MAGIC 2. ✅ `reprod_paper08.bronze.re_receipt`
-- MAGIC 3. ✅ `reprod_paper08.bronze.sy_disease`
-- MAGIC 4. ✅ `reprod_paper08.bronze.iy_medication`
-- MAGIC 5. ✅ `reprod_paper08.bronze.si_procedure`
-- MAGIC 6. ✅ `reprod_paper08.bronze.ho_insurer`
-- MAGIC
-- MAGIC ### 検証クエリ
-- MAGIC ```sql
-- MAGIC -- 全テーブルの確認
-- MAGIC SHOW TABLES IN reprod_paper08.bronze;
-- MAGIC
-- MAGIC -- カウント確認（データ生成後）
-- MAGIC SELECT COUNT(*) FROM reprod_paper08.bronze.patients;  -- 10,000
-- MAGIC ```
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC 次のノートブックを実行してデータを生成してください：
-- MAGIC - `02_generate_bronze_data.py` - Bronze層データ生成（PySpark）
