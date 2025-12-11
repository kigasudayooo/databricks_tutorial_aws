-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer: RA患者定義適用とデータ変換（SQL） - Unity Catalog対応
-- MAGIC
-- MAGIC このノートブックでは、Bronze層データにRA患者定義を適用し、Silver層データを作成します。
-- MAGIC
-- MAGIC ## RA患者定義の概要
-- MAGIC
-- MAGIC | 定義 | 条件 | 期待される患者数 | 有病率 |
-- MAGIC |------|------|------------------|--------|
-- MAGIC | Definition 0 | ICD-10コードのみ | ~800-900 | ~0.8-0.9% |
-- MAGIC | Definition 2 | ICD-10 + DMARDs ≥1ヶ月 | ~690 | ~0.69% |
-- MAGIC | **Definition 3** | **ICD-10 + DMARDs ≥2ヶ月** | **~650** | **~0.65%** |
-- MAGIC | Definition 4 | ICD-10 + DMARDs ≥6ヶ月 | ~460 | ~0.46% |
-- MAGIC
-- MAGIC **本分析ではDefinition 3を採用**（論文と同様）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 前提条件
-- MAGIC
-- MAGIC - `00_setup_catalog.sql` が実行済みであること
-- MAGIC - Bronze層のテーブルが作成され、データが生成済みであること
-- MAGIC - Silver層のテーブルが作成済みであること（`01_create_silver_tables.sql`）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. RA関連コードの定義

-- COMMAND ----------

-- RA関連ICD-10コード（論文と同じ定義）
CREATE OR REPLACE TEMP VIEW ra_icd10_codes AS
SELECT code FROM (VALUES
    -- M05.x: 血清反応陽性関節リウマチ
    ('M050'), ('M051'), ('M052'), ('M053'), ('M058'), ('M059'),
    -- M06.x: その他の関節リウマチ（M061とM064を除く）
    ('M060'), ('M062'), ('M063'), ('M068'), ('M069'),
    -- M08.x: 若年性関節炎（M081とM082を除く）
    ('M080'), ('M083'), ('M084'), ('M088'), ('M089')
) AS t(code);

SELECT "RA ICD-10 codes defined" AS status;

-- COMMAND ----------

-- 全DMARDsコード
CREATE OR REPLACE TEMP VIEW all_dmard_codes AS
SELECT code FROM (VALUES
    -- csDMARDs
    ('1199101'), ('1199102'),  -- MTX
    ('1199201'),               -- SSZ
    ('1199401'),               -- BUC
    ('1199301'),               -- TAC
    ('1199501'),               -- IGT
    ('1199601'),               -- LEF
    -- bDMARDs - TNFI
    ('4400101'), ('4400102'), ('4400103'), ('4400104'), ('4400105'),
    -- bDMARDs - IL6I
    ('4400201'), ('4400202'),
    -- bDMARDs - ABT
    ('4400301'),
    -- tsDMARDs - JAKi
    ('4400401'), ('4400402')
) AS t(code);

SELECT "All DMARDs codes defined" AS status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Definition 0: ICD-10コードのみ

-- COMMAND ----------

-- RA関連ICD-10コードを持つ患者を抽出
CREATE OR REPLACE TEMP VIEW ra_patients_def0 AS
SELECT DISTINCT 共通キー
FROM reprod_paper08.bronze.sy_disease
WHERE `ICD10コード` IN (SELECT code FROM ra_icd10_codes);

-- 患者数を確認
SELECT COUNT(*) AS def0_patient_count
FROM ra_patients_def0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. DMARDs処方月数の計算

-- COMMAND ----------

-- CRITICAL: DMARDs処方月数を計算
-- 各患者について、レセプトの`診療年月`を JOIN して、ユニークな月数をカウント
CREATE OR REPLACE TEMP VIEW dmard_prescription_months AS
SELECT
    iy.`共通キー`,
    COUNT(DISTINCT re.`診療年月`) AS prescription_months
FROM reprod_paper08.bronze.iy_medication iy
INNER JOIN reprod_paper08.bronze.re_receipt re ON iy.`検索番号` = re.検索番号
WHERE iy.`共通キー` IN (SELECT `共通キー` FROM ra_patients_def0)
  AND iy.`医薬品コード` IN (SELECT code FROM all_dmard_codes)
GROUP BY iy.`共通キー`;

-- 処方月数の分布を確認
SELECT
    COUNT(*) AS patients_with_dmards,
    ROUND(AVG(prescription_months), 1) AS avg_prescription_months,
    MIN(prescription_months) AS min_months,
    MAX(prescription_months) AS max_months,
    ROUND(PERCENTILE_APPROX(prescription_months, 0.25), 0) AS q25,
    ROUND(PERCENTILE_APPROX(prescription_months, 0.50), 0) AS median,
    ROUND(PERCENTILE_APPROX(prescription_months, 0.75), 0) AS q75
FROM dmard_prescription_months;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. 各RA定義の適用

-- COMMAND ----------

-- Definition 2: ICD-10 + DMARDs ≥1ヶ月
CREATE OR REPLACE TEMP VIEW ra_patients_def2 AS
SELECT DISTINCT d0.共通キー
FROM ra_patients_def0 d0
INNER JOIN dmard_prescription_months dpm ON d0.`共通キー` = dpm.`共通キー`
WHERE dpm.prescription_months >= 1;

-- Definition 3: ICD-10 + DMARDs ≥2ヶ月（主要定義）
CREATE OR REPLACE TEMP VIEW ra_patients_def3 AS
SELECT DISTINCT d0.共通キー
FROM ra_patients_def0 d0
INNER JOIN dmard_prescription_months dpm ON d0.`共通キー` = dpm.`共通キー`
WHERE dpm.prescription_months >= 2;

-- Definition 4: ICD-10 + DMARDs ≥6ヶ月
CREATE OR REPLACE TEMP VIEW ra_patients_def4 AS
SELECT DISTINCT d0.共通キー
FROM ra_patients_def0 d0
INNER JOIN dmard_prescription_months dpm ON d0.`共通キー` = dpm.`共通キー`
WHERE dpm.prescription_months >= 6;

-- 各定義の患者数を確認
SELECT '定義別患者数' AS category;

SELECT
    'Definition 0 (ICD-10のみ)' AS definition,
    COUNT(*) AS n_patients,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients), 2) AS prevalence_pct
FROM ra_patients_def0
UNION ALL
SELECT
    'Definition 2 (DMARDs ≥1ヶ月)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients), 2)
FROM ra_patients_def2
UNION ALL
SELECT
    'Definition 3 (DMARDs ≥2ヶ月) ★採用★',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients), 2)
FROM ra_patients_def3
UNION ALL
SELECT
    'Definition 4 (DMARDs ≥6ヶ月)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients), 2)
FROM ra_patients_def4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. 薬剤使用フラグの作成

-- COMMAND ----------

-- 各患者の薬剤使用パターンを集計
CREATE OR REPLACE TEMP VIEW drug_usage AS
SELECT
    p.`共通キー`,
    -- csDMARDs
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199101', '1199102') THEN 1 ELSE 0 END) AS MTX,
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199201') THEN 1 ELSE 0 END) AS SSZ,
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199401') THEN 1 ELSE 0 END) AS BUC,
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199301') THEN 1 ELSE 0 END) AS TAC,
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199501') THEN 1 ELSE 0 END) AS IGT,
    MAX(CASE WHEN iy.`医薬品コード` IN ('1199601') THEN 1 ELSE 0 END) AS LEF,
    -- bDMARDs - TNFI
    MAX(CASE WHEN iy.`医薬品コード` IN ('4400101','4400102','4400103','4400104','4400105') THEN 1 ELSE 0 END) AS TNFI,
    -- bDMARDs - IL6I
    MAX(CASE WHEN iy.`医薬品コード` IN ('4400201','4400202') THEN 1 ELSE 0 END) AS IL6I,
    -- bDMARDs - ABT
    MAX(CASE WHEN iy.`医薬品コード` IN ('4400301') THEN 1 ELSE 0 END) AS ABT,
    -- tsDMARDs - JAKi
    MAX(CASE WHEN iy.`医薬品コード` IN ('4400401','4400402') THEN 1 ELSE 0 END) AS JAKi,
    -- CS (コルチコステロイド)
    MAX(CASE WHEN iy.`医薬品コード` IN ('2454001','2454002','2454003') THEN 1 ELSE 0 END) AS CS
FROM ra_patients_def3 p
LEFT JOIN reprod_paper08.bronze.iy_medication iy ON p.`共通キー` = iy.`共通キー`
GROUP BY p.`共通キー`;

-- 薬剤使用率を確認
SELECT '薬剤使用率（Definition 3患者）' AS category;

SELECT
    ROUND(AVG(MTX) * 100, 1) AS MTX_pct,
    ROUND(AVG(SSZ) * 100, 1) AS SSZ_pct,
    ROUND(AVG(BUC) * 100, 1) AS BUC_pct,
    ROUND(AVG(TAC) * 100, 1) AS TAC_pct,
    ROUND(AVG(IGT) * 100, 1) AS IGT_pct,
    ROUND(AVG(LEF) * 100, 1) AS LEF_pct,
    ROUND(AVG(TNFI) * 100, 1) AS TNFI_pct,
    ROUND(AVG(IL6I) * 100, 1) AS IL6I_pct,
    ROUND(AVG(ABT) * 100, 1) AS ABT_pct,
    ROUND(AVG(JAKi) * 100, 1) AS JAKi_pct,
    ROUND(AVG(CS) * 100, 1) AS CS_pct
FROM drug_usage;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. 診療行為（手術・検査）フラグの作成

-- COMMAND ----------

-- 各患者の手術・検査実施パターンを集計
CREATE OR REPLACE TEMP VIEW procedure_usage AS
SELECT
    p.`共通キー`,
    MAX(CASE WHEN si.procedure_type = 'TJR' THEN 1 ELSE 0 END) AS TJR,
    MAX(CASE WHEN si.procedure_type = 'ARTHROPLASTY' THEN 1 ELSE 0 END) AS ARTHROPLASTY,
    MAX(CASE WHEN si.procedure_type = 'SYNOVECTOMY' THEN 1 ELSE 0 END) AS SYNOVECTOMY,
    MAX(CASE WHEN si.procedure_type = 'ULTRASOUND' THEN 1 ELSE 0 END) AS ULTRASOUND,
    MAX(CASE WHEN si.procedure_type = 'BMD' THEN 1 ELSE 0 END) AS BMD
FROM ra_patients_def3 p
LEFT JOIN reprod_paper08.bronze.si_procedure si ON p.`共通キー` = si.`共通キー`
GROUP BY p.`共通キー`;

-- 手術・検査実施率を確認
SELECT '手術・検査実施率（Definition 3患者）' AS category;

SELECT
    ROUND(AVG(TJR) * 100, 2) AS TJR_pct,
    ROUND(AVG(ARTHROPLASTY) * 100, 2) AS ARTHROPLASTY_pct,
    ROUND(AVG(SYNOVECTOMY) * 100, 2) AS SYNOVECTOMY_pct,
    ROUND(AVG(ULTRASOUND) * 100, 2) AS ULTRASOUND_pct,
    ROUND(AVG(BMD) * 100, 2) AS BMD_pct
FROM procedure_usage;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. RA患者マスタの作成（患者属性 + 薬剤フラグ + 手術フラグ）

-- COMMAND ----------

-- 患者基本情報に年齢群を追加
CREATE OR REPLACE TEMP VIEW ra_patients_base AS
SELECT
    p.*,
    CASE
        WHEN p.age BETWEEN 16 AND 19 THEN '16-19'
        WHEN p.age BETWEEN 20 AND 29 THEN '20-29'
        WHEN p.age BETWEEN 30 AND 39 THEN '30-39'
        WHEN p.age BETWEEN 40 AND 49 THEN '40-49'
        WHEN p.age BETWEEN 50 AND 59 THEN '50-59'
        WHEN p.age BETWEEN 60 AND 69 THEN '60-69'
        WHEN p.age BETWEEN 70 AND 79 THEN '70-79'
        WHEN p.age BETWEEN 80 AND 84 THEN '80-84'
        ELSE '85+'
    END AS age_group
FROM reprod_paper08.bronze.patients p
WHERE p.`共通キー` IN (SELECT `共通キー` FROM ra_patients_def3);

-- 基本統計を確認
SELECT
    COUNT(*) AS total_patients,
    SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) AS female_count,
    ROUND(SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS female_pct
FROM ra_patients_base;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. 最終的なRA患者マスタの作成
-- MAGIC
-- MAGIC テーブル作成とデータ挿入を同時に実行します。

-- COMMAND ----------

-- 全情報を結合してSilverテーブルを作成
CREATE OR REPLACE TABLE reprod_paper08.silver.ra_patients_def3
COMMENT 'Silver層: Definition 3 によるRA患者マスタ（ICD-10 + DMARDs ≥2ヶ月）'
AS
SELECT
    pb.patient_id,
    pb.`共通キー`,
    pb.age,
    pb.age_group,
    pb.sex,
    pb.birth_date,
    pb.is_ra_candidate,

    -- 処方月数
    COALESCE(dpm.prescription_months, 0) AS prescription_months,

    -- 薬剤使用フラグ
    COALESCE(du.MTX, 0) AS MTX,
    COALESCE(du.SSZ, 0) AS SSZ,
    COALESCE(du.BUC, 0) AS BUC,
    COALESCE(du.TAC, 0) AS TAC,
    COALESCE(du.IGT, 0) AS IGT,
    COALESCE(du.LEF, 0) AS LEF,
    COALESCE(du.TNFI, 0) AS TNFI,
    COALESCE(du.IL6I, 0) AS IL6I,
    COALESCE(du.ABT, 0) AS ABT,
    COALESCE(du.JAKi, 0) AS JAKi,
    COALESCE(du.CS, 0) AS CS,

    -- 複合フラグ: bDMARDs使用
    CASE
        WHEN COALESCE(du.TNFI, 0) = 1 OR COALESCE(du.IL6I, 0) = 1 OR COALESCE(du.ABT, 0) = 1
        THEN 1
        ELSE 0
    END AS bDMARDs,

    -- 複合フラグ: 全DMARDs使用
    CASE
        WHEN COALESCE(du.MTX, 0) = 1 OR COALESCE(du.SSZ, 0) = 1 OR COALESCE(du.BUC, 0) = 1 OR
             COALESCE(du.TAC, 0) = 1 OR COALESCE(du.IGT, 0) = 1 OR COALESCE(du.LEF, 0) = 1 OR
             COALESCE(du.TNFI, 0) = 1 OR COALESCE(du.IL6I, 0) = 1 OR COALESCE(du.ABT, 0) = 1 OR
             COALESCE(du.JAKi, 0) = 1
        THEN 1
        ELSE 0
    END AS any_DMARD,

    -- 診療行為フラグ
    COALESCE(pu.TJR, 0) AS TJR,
    COALESCE(pu.ARTHROPLASTY, 0) AS ARTHROPLASTY,
    COALESCE(pu.SYNOVECTOMY, 0) AS SYNOVECTOMY,
    COALESCE(pu.ULTRASOUND, 0) AS ULTRASOUND,
    COALESCE(pu.BMD, 0) AS BMD,

    -- 複合フラグ: RA関連手術
    CASE
        WHEN COALESCE(pu.TJR, 0) = 1 OR COALESCE(pu.ARTHROPLASTY, 0) = 1 OR COALESCE(pu.SYNOVECTOMY, 0) = 1
        THEN 1
        ELSE 0
    END AS any_RA_surgery

FROM ra_patients_base pb
LEFT JOIN drug_usage du ON pb.`共通キー` = du.`共通キー`
LEFT JOIN procedure_usage pu ON pb.`共通キー` = pu.`共通キー`
LEFT JOIN dmard_prescription_months dpm ON pb.`共通キー` = dpm.`共通キー`;

-- 挿入件数を確認
SELECT COUNT(*) AS inserted_records
FROM reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. RA定義別サマリーの作成

-- COMMAND ----------

-- 定義別患者数と有病率をサマリーテーブルに作成
CREATE OR REPLACE TABLE reprod_paper08.silver.ra_definitions_summary
COMMENT 'Silver層: RA定義別の患者数と有病率サマリー'
AS
SELECT
    'def_0' AS definition,
    (SELECT COUNT(*) FROM ra_patients_def0) AS n_patients,
    (SELECT COUNT(*) FROM ra_patients_def0) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients) AS prevalence_pct
UNION ALL
SELECT
    'def_2',
    (SELECT COUNT(*) FROM ra_patients_def2),
    (SELECT COUNT(*) FROM ra_patients_def2) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients)
UNION ALL
SELECT
    'def_3',
    (SELECT COUNT(*) FROM ra_patients_def3),
    (SELECT COUNT(*) FROM ra_patients_def3) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients)
UNION ALL
SELECT
    'def_4',
    (SELECT COUNT(*) FROM ra_patients_def4),
    (SELECT COUNT(*) FROM ra_patients_def4) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients);

-- サマリーを表示
SELECT * FROM reprod_paper08.silver.ra_definitions_summary ORDER BY definition;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. データ品質チェック

-- COMMAND ----------

SELECT '==============================================';
SELECT 'Silver Layer データ品質サマリー';
SELECT '==============================================';

-- 基本統計
SELECT
    COUNT(*) AS total_ra_patients,
    SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) AS female_count,
    ROUND(SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS female_pct,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reprod_paper08.bronze.patients), 2) AS prevalence_pct
FROM reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

-- 年齢分布
SELECT
    age_group,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM reprod_paper08.silver.ra_patients_def3
GROUP BY age_group
ORDER BY
    CASE age_group
        WHEN '16-19' THEN 1 WHEN '20-29' THEN 2 WHEN '30-39' THEN 3
        WHEN '40-49' THEN 4 WHEN '50-59' THEN 5 WHEN '60-69' THEN 6
        WHEN '70-79' THEN 7 WHEN '80-84' THEN 8 WHEN '85+' THEN 9
    END;

-- COMMAND ----------

-- 65歳以上・85歳以上の割合
SELECT
    SUM(CASE WHEN age >= 65 THEN 1 ELSE 0 END) AS elderly_65_count,
    ROUND(SUM(CASE WHEN age >= 65 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS elderly_65_pct,
    SUM(CASE WHEN age >= 85 THEN 1 ELSE 0 END) AS elderly_85_count,
    ROUND(SUM(CASE WHEN age >= 85 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS elderly_85_pct
FROM reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

-- 薬剤使用率
SELECT
    ROUND(AVG(MTX) * 100, 1) AS MTX_pct,
    ROUND(AVG(SSZ) * 100, 1) AS SSZ_pct,
    ROUND(AVG(BUC) * 100, 1) AS BUC_pct,
    ROUND(AVG(TAC) * 100, 1) AS TAC_pct,
    ROUND(AVG(IGT) * 100, 1) AS IGT_pct,
    ROUND(AVG(LEF) * 100, 1) AS LEF_pct,
    ROUND(AVG(TNFI) * 100, 1) AS TNFI_pct,
    ROUND(AVG(IL6I) * 100, 1) AS IL6I_pct,
    ROUND(AVG(ABT) * 100, 1) AS ABT_pct,
    ROUND(AVG(JAKi) * 100, 1) AS JAKi_pct,
    ROUND(AVG(CS) * 100, 1) AS CS_pct,
    ROUND(AVG(bDMARDs) * 100, 1) AS bDMARDs_pct
FROM reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

-- 手術・検査実施率
SELECT
    ROUND(AVG(TJR) * 100, 2) AS TJR_pct,
    ROUND(AVG(ARTHROPLASTY) * 100, 2) AS ARTHROPLASTY_pct,
    ROUND(AVG(SYNOVECTOMY) * 100, 2) AS SYNOVECTOMY_pct,
    ROUND(AVG(ULTRASOUND) * 100, 2) AS ULTRASOUND_pct,
    ROUND(AVG(BMD) * 100, 2) AS BMD_pct,
    ROUND(AVG(any_RA_surgery) * 100, 2) AS any_RA_surgery_pct
FROM reprod_paper08.silver.ra_patients_def3;

-- COMMAND ----------

SELECT '==============================================';
SELECT 'Silver Layer 処理完了！';
SELECT '==============================================';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 完了
-- MAGIC
-- MAGIC Silver層のデータ変換が完了しました。
-- MAGIC
-- MAGIC ### 作成されたデータ（Unity Catalog）
-- MAGIC 1. ✅ `reprod_paper08.silver.ra_patients_def3` - RA患者マスタ（~650件）
-- MAGIC 2. ✅ `reprod_paper08.silver.ra_definitions_summary` - 定義別サマリー（4件）
-- MAGIC
-- MAGIC ### 検証クエリ
-- MAGIC ```sql
-- MAGIC -- 患者数確認
-- MAGIC SELECT COUNT(*) FROM reprod_paper08.silver.ra_patients_def3;  -- ~650
-- MAGIC
-- MAGIC -- 定義別サマリー確認
-- MAGIC SELECT * FROM reprod_paper08.silver.ra_definitions_summary ORDER BY definition;
-- MAGIC ```
-- MAGIC
-- MAGIC ### 主要な結果
-- MAGIC - **有病率**: 約0.65%
-- MAGIC - **女性比率**: 約76%
-- MAGIC - **65歳以上**: 約55-60%
-- MAGIC
-- MAGIC ### 次のステップ
-- MAGIC Gold層の実装に進んでください：
-- MAGIC 1. `gold/01_create_gold_tables.sql` - Goldテーブル作成
-- MAGIC 2. `gold/02_analysis_and_visualization.py` - 分析と可視化
