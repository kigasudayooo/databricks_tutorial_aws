# Databricks notebook source
# MAGIC %md
# MAGIC # RA患者コホート分析
# MAGIC 
# MAGIC NDBデータからRA（関節リウマチ）患者を抽出し、年齢層別の薬剤使用率・手術実施率を集計する。
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## このノートブックでできること
# MAGIC 
# MAGIC | 処理 | 内容 |
# MAGIC |------|------|
# MAGIC | 患者抽出 | ICD-10コードとDMARDs処方月数でRA患者を定義・抽出 |
# MAGIC | 薬剤集計 | csDMARDs、bDMARDs、tsDMARDs、ステロイドの使用率を年齢層別に集計 |
# MAGIC | 手術集計 | 人工関節置換術、関節形成術等の実施率を年齢層別に集計 |
# MAGIC | 結果保存 | Silver/Gold層に保存し、SQLやBIツールから参照可能にする |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 入出力
# MAGIC 
# MAGIC **入力（Bronze層）**
# MAGIC - `bronze.patients` : 患者マスタ
# MAGIC - `bronze.sy_disease` : 傷病名
# MAGIC - `bronze.iy_medication` : 医薬品
# MAGIC - `bronze.si_procedure` : 診療行為
# MAGIC - `bronze.re_receipt` : レセプト
# MAGIC 
# MAGIC **出力（Silver/Gold層）**
# MAGIC - `silver.ra_patients` : RA患者マスタ（1患者1行、各種フラグ付き）
# MAGIC - `gold.age_distribution` : 年齢層別患者数・有病率
# MAGIC - `gold.medication_usage` : 年齢層別薬剤使用率
# MAGIC - `gold.procedure_usage` : 年齢層別手術実施率
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 実行方法
# MAGIC 
# MAGIC 1. 「1. 設定」で分析パラメータを確認・変更
# MAGIC 2. Run All で全セル実行
# MAGIC 3. データ更新時・定義変更時も Run All で再実行

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 設定
# MAGIC 
# MAGIC 分析の定義をここで設定する。定義を変更する場合はこのセクションのみ修正する。

# COMMAND ----------

# --- カタログ・スキーマ ---
CATALOG = "reprod_paper08"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# --- RA患者定義 ---
# DMARDs処方月数の閾値
# 1: Definition 2（1ヶ月以上）
# 2: Definition 3（2ヶ月以上）← 論文のメイン定義
# 6: Definition 4（6ヶ月以上）
DMARD_MONTHS_THRESHOLD = 2

# --- 年齢層の区切り ---
AGE_BINS = [16, 20, 30, 40, 50, 60, 70, 80, 85, 150]
AGE_LABELS = ["16-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-84", "85+"]

# COMMAND ----------

# --- RA関連ICD-10コード ---
RA_ICD10_CODES = [
    # M05: 血清反応陽性関節リウマチ
    "M050", "M051", "M052", "M053", "M058", "M059",
    # M06: その他の関節リウマチ（M061, M064を除く）
    "M060", "M062", "M063", "M068", "M069",
    # M08: 若年性関節炎（M081, M082を除く）
    "M080", "M083", "M084", "M088", "M089"
]

# --- DMARDsコード（薬効分類別）---
DMARD_CODES = {
    # csDMARDs（従来型合成DMARD）
    "MTX": ["1199101", "1199102"],       # メトトレキサート
    "SSZ": ["1199201"],                   # サラゾスルファピリジン
    "BUC": ["1199401"],                   # ブシラミン
    "TAC": ["1199301"],                   # タクロリムス
    "IGT": ["1199501"],                   # イグラチモド
    "LEF": ["1199601"],                   # レフルノミド
    # bDMARDs（生物学的DMARD）
    "TNFI": ["4400101", "4400102", "4400103", "4400104", "4400105"],
    "IL6I": ["4400201", "4400202"],
    "ABT":  ["4400301"],
    # tsDMARDs（分子標的型合成DMARD）
    "JAKi": ["4400401", "4400402"],
}

# ステロイド
CS_CODES = ["2454001", "2454002", "2454003"]

# 全DMARDsコード（フラット化）
ALL_DMARD_CODES = [code for codes in DMARD_CODES.values() for code in codes]

# --- 診療行為コード ---
PROCEDURE_TYPES = ["TJR", "ARTHROPLASTY", "SYNOVECTOMY", "ULTRASOUND", "BMD"]

# COMMAND ----------

# 設定確認
print(f"カタログ: {CATALOG}")
print(f"RA患者定義: DMARDs処方 {DMARD_MONTHS_THRESHOLD}ヶ月以上")
print(f"ICD-10コード: {len(RA_ICD10_CODES)}種類")
print(f"DMARDsコード: {len(ALL_DMARD_CODES)}種類")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ライブラリと共通関数

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# COMMAND ----------

def sql_list(codes):
    """リストをSQL IN句用の文字列に変換"""
    return ", ".join([f"'{c}'" for c in codes])


def add_age_group(df, age_col="age"):
    """年齢からage_groupカラムを追加"""
    conditions = []
    for i, label in enumerate(AGE_LABELS):
        lower = AGE_BINS[i]
        upper = AGE_BINS[i + 1]
        conditions.append(
            (F.col(age_col) >= lower) & (F.col(age_col) < upper)
        )
    
    expr = F.when(conditions[0], AGE_LABELS[0])
    for cond, label in zip(conditions[1:], AGE_LABELS[1:]):
        expr = expr.when(cond, label)
    
    return df.withColumn("age_group", expr)


def aggregate_flags_by_age(df, flag_cols):
    """年齢層別にフラグの平均（使用率）を集計"""
    aggs = [F.count("*").alias("n")]
    for col in flag_cols:
        aggs.append(F.round(F.avg(col) * 100, 2).alias(col))
    
    return df.groupBy("age_group").agg(*aggs)


def add_total_row(df, flag_cols, source_df):
    """集計結果にTotal行を追加"""
    aggs = [F.lit("Total").alias("age_group"), F.count("*").alias("n")]
    for col in flag_cols:
        aggs.append(F.round(F.avg(col) * 100, 2).alias(col))
    
    total = source_df.agg(*aggs)
    return df.union(total)


def sort_by_age_group(df):
    """age_groupでソート"""
    order = {label: i for i, label in enumerate(AGE_LABELS)}
    order["Total"] = len(AGE_LABELS)
    
    return df.orderBy(
        F.when(F.col("age_group") == "Total", len(AGE_LABELS))
         .otherwise(
             F.coalesce(*[
                 F.when(F.col("age_group") == label, F.lit(i))
                 for label, i in order.items()
             ])
         )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. データ取得
# MAGIC 
# MAGIC Bronze層からデータを取得する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 RA候補患者の抽出

# COMMAND ----------

# ICD-10コードでRA候補を抽出
df_ra_candidates = spark.sql(f"""
SELECT DISTINCT common_key
FROM {CATALOG}.{SCHEMA_BRONZE}.sy_disease
WHERE icd10_code IN ({sql_list(RA_ICD10_CODES)})
""")

n_candidates = df_ra_candidates.count()
print(f"RA候補患者数（ICD-10コードあり）: {n_candidates:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 DMARDs処方月数の計算

# COMMAND ----------

# 患者ごとのDMARDs処方月数を計算
df_dmard_months = spark.sql(f"""
SELECT
    iy.common_key,
    COUNT(DISTINCT re.service_month) AS prescription_months
FROM {CATALOG}.{SCHEMA_BRONZE}.iy_medication iy
INNER JOIN {CATALOG}.{SCHEMA_BRONZE}.re_receipt re 
    ON iy.receipt_id = re.receipt_id
WHERE iy.drug_code IN ({sql_list(ALL_DMARD_CODES)})
GROUP BY iy.common_key
""")

# 分布確認
print("DMARDs処方月数の分布:")
df_dmard_months.describe("prescription_months").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 患者抽出
# MAGIC 
# MAGIC RA候補のうち、DMARDs処方月数が閾値以上の患者を抽出する。

# COMMAND ----------

# RA候補 × DMARDs処方月数を結合
df_ra_with_dmard = df_ra_candidates.join(df_dmard_months, "common_key", "inner")

# 閾値でフィルタ
df_ra_keys = df_ra_with_dmard.filter(
    F.col("prescription_months") >= DMARD_MONTHS_THRESHOLD
).select("common_key", "prescription_months")

n_ra = df_ra_keys.count()
print(f"RA患者数（DMARDs {DMARD_MONTHS_THRESHOLD}ヶ月以上）: {n_ra:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 参考: 定義別の患者数
# MAGIC 
# MAGIC `DMARD_MONTHS_THRESHOLD` を変更すれば異なる定義での分析が可能。

# COMMAND ----------

print("定義別RA患者数:")
print("-" * 40)
for name, threshold, desc in [
    ("Definition 0", 0, "ICD-10のみ"),
    ("Definition 2", 1, "DMARDs 1ヶ月以上"),
    ("Definition 3", 2, "DMARDs 2ヶ月以上"),
    ("Definition 4", 6, "DMARDs 6ヶ月以上"),
]:
    if threshold == 0:
        count = n_candidates
    else:
        count = df_ra_with_dmard.filter(F.col("prescription_months") >= threshold).count()
    marker = " <-- 現在の設定" if threshold == DMARD_MONTHS_THRESHOLD else ""
    print(f"  {name} ({desc}): {count:,}{marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 薬剤・診療行為フラグの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 薬剤使用フラグ

# COMMAND ----------

# 薬剤別のCASE文を生成
drug_case_clauses = []
for drug_name, codes in DMARD_CODES.items():
    clause = f"MAX(CASE WHEN drug_code IN ({sql_list(codes)}) THEN 1 ELSE 0 END) AS {drug_name}"
    drug_case_clauses.append(clause)

# ステロイド
drug_case_clauses.append(
    f"MAX(CASE WHEN drug_code IN ({sql_list(CS_CODES)}) THEN 1 ELSE 0 END) AS CS"
)

# SQLで薬剤使用フラグを作成
df_drug_flags = spark.sql(f"""
SELECT
    common_key,
    {', '.join(drug_case_clauses)}
FROM {CATALOG}.{SCHEMA_BRONZE}.iy_medication
WHERE common_key IN (SELECT common_key FROM ra_patients_temp)
GROUP BY common_key
""".replace("ra_patients_temp", f"({df_ra_keys.select('common_key').toPandas()['common_key'].apply(lambda x: f\"'{x}'\").str.cat(sep=', ')})"))

# 上記は大規模データでは非効率なので、実際には一時ビューを使う
df_ra_keys.createOrReplaceTempView("ra_patients_temp")

df_drug_flags = spark.sql(f"""
SELECT
    iy.common_key,
    {', '.join(drug_case_clauses)}
FROM {CATALOG}.{SCHEMA_BRONZE}.iy_medication iy
WHERE iy.common_key IN (SELECT common_key FROM ra_patients_temp)
GROUP BY iy.common_key
""")

print(f"薬剤フラグ作成完了: {df_drug_flags.count():,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 診療行為フラグ

# COMMAND ----------

# 診療行為フラグを作成
proc_case_clauses = [
    f"MAX(CASE WHEN procedure_type = '{pt}' THEN 1 ELSE 0 END) AS {pt}"
    for pt in PROCEDURE_TYPES
]

df_proc_flags = spark.sql(f"""
SELECT
    si.common_key,
    {', '.join(proc_case_clauses)}
FROM {CATALOG}.{SCHEMA_BRONZE}.si_procedure si
WHERE si.common_key IN (SELECT common_key FROM ra_patients_temp)
GROUP BY si.common_key
""")

print(f"診療行為フラグ作成完了: {df_proc_flags.count():,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver層: RA患者マスタの作成

# COMMAND ----------

# 患者マスタを取得
df_patients = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.patients")

# 全情報を結合
df_ra_master = (
    df_ra_keys
    .join(df_patients.select("common_key", "patient_id", "age", "sex", "birth_date"), 
          "common_key", "inner")
    .join(df_drug_flags, "common_key", "left")
    .join(df_proc_flags, "common_key", "left")
)

# 年齢層を追加
df_ra_master = add_age_group(df_ra_master)

# NULL を 0 に変換（フラグ列）
flag_cols = list(DMARD_CODES.keys()) + ["CS"] + PROCEDURE_TYPES
for col in flag_cols:
    df_ra_master = df_ra_master.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# 複合フラグを追加
df_ra_master = df_ra_master.withColumn(
    "bDMARDs",
    F.when((F.col("TNFI") == 1) | (F.col("IL6I") == 1) | (F.col("ABT") == 1), 1).otherwise(0)
).withColumn(
    "any_RA_surgery",
    F.when((F.col("TJR") == 1) | (F.col("ARTHROPLASTY") == 1) | (F.col("SYNOVECTOMY") == 1), 1).otherwise(0)
)

# COMMAND ----------

# Silver層に保存
df_ra_master.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_SILVER}.ra_patients"
)

n_saved = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.ra_patients").count()
print(f"Silver層保存完了: {CATALOG}.{SCHEMA_SILVER}.ra_patients ({n_saved:,}件)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold層: 年齢層別集計

# COMMAND ----------

# Silver層から読み込み
df_ra = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.ra_patients")
df_all = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.patients")

total_ra = df_ra.count()
total_pop = df_all.count()

print(f"RA患者数: {total_ra:,}")
print(f"全患者数: {total_pop:,}")
print(f"有病率: {total_ra / total_pop * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 年齢層別患者分布

# COMMAND ----------

# RA患者の年齢層別集計
df_ra_by_age = df_ra.groupBy("age_group").agg(
    F.count("*").alias("n"),
    F.sum(F.when(F.col("sex") == "2", 1).otherwise(0)).alias("n_female"),
    F.sum(F.when(F.col("sex") == "1", 1).otherwise(0)).alias("n_male")
)

# 全患者の年齢層別集計（有病率計算用）
df_all_by_age = add_age_group(df_all).groupBy("age_group").agg(
    F.count("*").alias("n_pop")
)

# 結合して指標計算
df_age_dist = (
    df_ra_by_age
    .join(df_all_by_age, "age_group", "left")
    .withColumn("pct_of_total", F.round(F.col("n") / total_ra * 100, 1))
    .withColumn("female_pct", F.round(F.col("n_female") / F.col("n") * 100, 1))
    .withColumn("fm_ratio", F.round(F.col("n_female") / F.col("n_male"), 2))
    .withColumn("prevalence", F.round(F.col("n") / F.col("n_pop") * 100, 2))
    .select("age_group", "n", "pct_of_total", "female_pct", "fm_ratio", "prevalence")
)

# Total行を追加
n_female = df_ra.filter("sex = '2'").count()
n_male = df_ra.filter("sex = '1'").count()
total_row = spark.createDataFrame([
    ("Total", total_ra, 100.0, round(n_female/total_ra*100, 1), 
     round(n_female/n_male, 2), round(total_ra/total_pop*100, 2))
], ["age_group", "n", "pct_of_total", "female_pct", "fm_ratio", "prevalence"])

df_age_dist = sort_by_age_group(df_age_dist.union(total_row))

# 保存
df_age_dist.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_GOLD}.age_distribution"
)
print(f"保存完了: {CATALOG}.{SCHEMA_GOLD}.age_distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 年齢層別薬剤使用率

# COMMAND ----------

med_cols = list(DMARD_CODES.keys()) + ["CS", "bDMARDs"]

df_med_usage = aggregate_flags_by_age(df_ra, med_cols)
df_med_usage = add_total_row(df_med_usage, med_cols, df_ra)
df_med_usage = sort_by_age_group(df_med_usage)

# 保存
df_med_usage.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_GOLD}.medication_usage"
)
print(f"保存完了: {CATALOG}.{SCHEMA_GOLD}.medication_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 年齢層別手術実施率

# COMMAND ----------

proc_cols = PROCEDURE_TYPES + ["any_RA_surgery"]

df_proc_usage = aggregate_flags_by_age(df_ra, proc_cols)
df_proc_usage = add_total_row(df_proc_usage, proc_cols, df_ra)
df_proc_usage = sort_by_age_group(df_proc_usage)

# 保存
df_proc_usage.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA_GOLD}.procedure_usage"
)
print(f"保存完了: {CATALOG}.{SCHEMA_GOLD}.procedure_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 結果確認

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 年齢層別患者分布

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.age_distribution"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 年齢層別薬剤使用率

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.medication_usage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 年齢層別手術実施率

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.procedure_usage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 可視化

# COMMAND ----------

# pandas に変換
pdf_age = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.age_distribution").filter("age_group != 'Total'").toPandas()
pdf_med = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.medication_usage").filter("age_group != 'Total'").toPandas()

fig, axes = plt.subplots(2, 2, figsize=(12, 8))

# 1. 年齢層別患者数
ax1 = axes[0, 0]
ax1.bar(pdf_age["age_group"], pdf_age["n"], color="steelblue")
ax1.set_xlabel("Age Group")
ax1.set_ylabel("Number of Patients")
ax1.set_title("RA Patient Distribution by Age")
ax1.tick_params(axis="x", rotation=45)

# 2. 有病率
ax2 = axes[0, 1]
ax2.plot(pdf_age["age_group"], pdf_age["prevalence"], "o-", color="coral")
ax2.set_xlabel("Age Group")
ax2.set_ylabel("Prevalence (%)")
ax2.set_title("RA Prevalence by Age")
ax2.tick_params(axis="x", rotation=45)

# 3. MTX使用率
ax3 = axes[1, 0]
ax3.bar(pdf_med["age_group"], pdf_med["MTX"], color="green")
ax3.set_xlabel("Age Group")
ax3.set_ylabel("MTX Usage (%)")
ax3.set_title("MTX Usage by Age")
ax3.tick_params(axis="x", rotation=45)

# 4. bDMARDs使用率
ax4 = axes[1, 1]
ax4.bar(pdf_med["age_group"], pdf_med["bDMARDs"], color="purple")
ax4.set_xlabel("Age Group")
ax4.set_ylabel("bDMARDs Usage (%)")
ax4.set_title("bDMARDs Usage by Age")
ax4.tick_params(axis="x", rotation=45)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. サマリー

# COMMAND ----------

# 主要指標
print("=" * 50)
print("分析結果サマリー")
print("=" * 50)
print(f"RA患者定義: ICD-10 + DMARDs {DMARD_MONTHS_THRESHOLD}ヶ月以上")
print(f"RA患者数: {total_ra:,}")
print(f"有病率: {total_ra / total_pop * 100:.2f}%")
print(f"女性比率: {n_female / total_ra * 100:.1f}%")
print()
print("薬剤使用率:")
med_totals = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.medication_usage").filter("age_group = 'Total'").first()
print(f"  MTX: {med_totals['MTX']}%")
print(f"  bDMARDs: {med_totals['bDMARDs']}%")
print(f"  CS: {med_totals['CS']}%")
print()
print("手術実施率:")
proc_totals = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.procedure_usage").filter("age_group = 'Total'").first()
print(f"  RA関連手術: {proc_totals['any_RA_surgery']}%")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 完了
# MAGIC 
# MAGIC 生成されたテーブル:
# MAGIC 
# MAGIC | 層 | テーブル | 内容 |
# MAGIC |----|---------|------|
# MAGIC | Silver | `ra_patients` | RA患者マスタ（1患者1行） |
# MAGIC | Gold | `age_distribution` | 年齢層別患者数・有病率 |
# MAGIC | Gold | `medication_usage` | 年齢層別薬剤使用率 |
# MAGIC | Gold | `procedure_usage` | 年齢層別手術実施率 |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 定義を変更する場合
# MAGIC 
# MAGIC 「1. 設定」セクションの `DMARD_MONTHS_THRESHOLD` を変更して Run All
# MAGIC 
# MAGIC ### データが更新された場合
# MAGIC 
# MAGIC Run All で Silver/Gold 層が再作成される
# MAGIC 
# MAGIC ### 結果を他ツールで使う場合
# MAGIC 
# MAGIC - SQL: `SELECT * FROM {CATALOG}.{SCHEMA_GOLD}.medication_usage`
# MAGIC - BI: カタログから直接接続可能