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
# MAGIC ## 研究者にとっての価値
# MAGIC
# MAGIC ### 再現性の担保
# MAGIC - **自己再現性**: このノートブックを保存すれば、半年後も同じ解析を再実行可能
# MAGIC - **他者による再現**: 共同研究者や査読者が同じ環境で結果を検証可能
# MAGIC - **透明性**: 解析手順が全て記録され、Supplementary Materialとして論文に添付可能
# MAGIC
# MAGIC ### 試行錯誤の高速化
# MAGIC - **パラメータ変更**: `DMARD_MONTHS_THRESHOLD`を変更するだけで異なる定義での分析が可能
# MAGIC - **感度分析**: 査読者から「異なる定義で試して」と言われても、Run Allで即座に対応
# MAGIC - **対象患者数の確認**: 研究計画段階で、実際のデータで患者数を確認できる
# MAGIC
# MAGIC ### データ管理の一元化
# MAGIC - **ファイル散乱の防止**: `ra_patients_final_v2.csv`のようなファイルが増殖しない
# MAGIC - **最新版の明確化**: Silver/Goldテーブルが常に最新状態
# MAGIC - **履歴管理**: Delta Lakeにより過去の状態も参照可能
# MAGIC
# MAGIC ### 継続性
# MAGIC - **年次更新**: Bronze層データ更新時にRun Allで最新の集計を自動生成
# MAGIC - **引き継ぎ**: 担当者変更時もノートブックがあれば解析内容を継承可能
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
# MAGIC
# MAGIC **重要**: 設定を変更したら必ず全セルを再実行してください。Silver/Gold層が一貫性を保つために必要です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 設定
# MAGIC
# MAGIC 分析の定義をここで設定する。定義を変更する場合はこのセクションのみ修正する。
# MAGIC
# MAGIC ### パラメータ設計の意図
# MAGIC
# MAGIC **RA患者定義の閾値**
# MAGIC DMARDs処方月数による絞り込みは、ICD-10コードのみでは含まれる「疑い例」や「除外診断」を排除し、
# MAGIC 実際に治療を受けているRA患者を特定するためのものです。
# MAGIC
# MAGIC - **1ヶ月以上**: 感度重視（偽陰性を減らす）、サンプルサイズ最大
# MAGIC - **2ヶ月以上**: バランス型、論文のメイン定義として推奨
# MAGIC - **6ヶ月以上**: 特異度重視（偽陽性を減らす）、確実なRA患者のみ
# MAGIC
# MAGIC **年齢層の設定**
# MAGIC 10歳刻みを基本としつつ、80歳以上を細分化しています。これは：
# MAGIC - 高齢者では薬剤選択が年齢により大きく異なる
# MAGIC - 80-84歳と85歳以上で治療方針が変わる臨床実態を反映
# MAGIC
# MAGIC **研究上の活用**
# MAGIC - 査読者から「異なる定義での感度分析を」と言われた場合、この値を変更してRun All
# MAGIC - 複数の定義での結果を並べて、定義の影響を評価可能

# COMMAND ----------

# --- カタログ・スキーマ ---
CATALOG = "reprod_paper08"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# --- RA患者定義 ---
# DMARDs処方月数の閾値
# 1: Definition 2（1ヶ月以上）- 感度重視
# 2: Definition 3（2ヶ月以上）← 論文のメイン定義（バランス型）
# 6: Definition 4（6ヶ月以上）- 特異度重視
DMARD_MONTHS_THRESHOLD = 2

# --- 年齢層の区切り ---
# 16歳以上の成人RAを対象とし、高齢者層を細分化
AGE_BINS = [16, 20, 30, 40, 50, 60, 70, 80, 85, 150]
AGE_LABELS = ["16-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-84", "85+"]

# COMMAND ----------

# --- RA関連ICD-10コード ---
# M061（成人スティル病）とM064（炎症性多発性関節症）は除外
# これらは厳密にはRAとは異なる疾患概念のため
RA_ICD10_CODES = [
    # M05: 血清反応陽性関節リウマチ（抗CCP抗体陽性等）
    "M050", "M051", "M052", "M053", "M058", "M059",
    # M06: その他の関節リウマチ（血清反応陰性RA等）
    "M060", "M062", "M063", "M068", "M069",
    # M08: 若年性関節炎（16歳以上に持ち越した症例も含む）
    "M080", "M083", "M084", "M088", "M089"
]

# --- DMARDsコード（薬効分類別）---
# 日本リウマチ学会のガイドラインに基づく分類
DMARD_CODES = {
    # csDMARDs（conventional synthetic DMARDs: 従来型合成DMARD）
    "MTX": ["1199101", "1199102"],       # メトトレキサート - 第一選択薬
    "SSZ": ["1199201"],                   # サラゾスルファピリジン - 軽症例
    "BUC": ["1199401"],                   # ブシラミン - 日本で開発
    "TAC": ["1199301"],                   # タクロリムス - 免疫抑制剤
    "IGT": ["1199501"],                   # イグラチモド - 日本で開発
    "LEF": ["1199601"],                   # レフルノミド - MTX不応例
    # bDMARDs（biological DMARDs: 生物学的DMARD）
    # 高額医療であり、csDMARDs不応例に使用
    "TNFI": ["4400101", "4400102", "4400103", "4400104", "4400105"],  # TNF阻害薬
    "IL6I": ["4400201", "4400202"],       # IL-6阻害薬（トシリズマブ等）
    "ABT":  ["4400301"],                  # アバタセプト（T細胞共刺激阻害）
    # tsDMARDs（targeted synthetic DMARDs: 分子標的型合成DMARD）
    # 最新の治療薬、経口投与可能
    "JAKi": ["4400401", "4400402"],       # JAK阻害薬（トファシチニブ等）
}

# ステロイド（副腎皮質ステロイド）
# 抗炎症目的で併用されるが、長期使用は副作用のリスク
CS_CODES = ["2454001", "2454002", "2454003"]

# 全DMARDsコード（フラット化）
# RA患者定義のための処方月数カウントに使用
ALL_DMARD_CODES = [code for codes in DMARD_CODES.values() for code in codes]

# --- 診療行為コード ---
# 関節破壊が進行した場合の外科的介入を評価
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
# MAGIC
# MAGIC このセクションでは、ノートブック全体で使用する共通関数を定義します。
# MAGIC 関数として定義することで、コードの重複を避け、保守性を高めます。
# MAGIC
# MAGIC **研究上のポイント**
# MAGIC - 年齢層の定義を変更したい場合、AGE_BINSとAGE_LABELSを変更するだけで、全ての集計に反映される
# MAGIC - 集計ロジックは関数化されているため、バグがあっても修正箇所は1箇所のみ
# MAGIC - 他の研究プロジェクトでも、これらの関数を再利用可能

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib_fontja  # 日本語フォント対応

# COMMAND ----------

def sql_list(codes):
    """リストをSQL IN句用の文字列に変換

    研究上の用途: ICD-10コードや薬剤コードのリストをSQLクエリで使える形式に変換
    """
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
# MAGIC
# MAGIC ### 研究上のポイント
# MAGIC
# MAGIC このセクションでは、まずICD-10コードでRA候補を抽出し、その後DMARDs処方で絞り込みます。
# MAGIC 2段階の抽出により、各ステップでの患者数の変化を確認できます。
# MAGIC
# MAGIC **従来の課題**
# MAGIC - IT部門に「RAの患者データをください」と依頼
# MAGIC - 数日後にデータが届く
# MAGIC - 「想定より少ない」→ 定義を緩めて再依頼
# MAGIC - さらに数日待つ...
# MAGIC
# MAGIC **このノートブックでの解決**
# MAGIC - ICD-10のみの候補数をその場で確認
# MAGIC - DMARDs閾値を変えた場合の患者数もその場で確認
# MAGIC - 研究計画段階で現実的な患者数を把握し、倫理審査に提出できる

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 RA候補患者の抽出
# MAGIC
# MAGIC まずICD-10コードでRA関連診断のある患者を抽出します。
# MAGIC この時点では「疑い」や「除外診断」も含まれる可能性があります。

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
# MAGIC
# MAGIC 各患者について、DMARDsが処方された「月数」をカウントします。
# MAGIC これは「処方回数」ではなく、「何ヶ月分の処方があったか」を示します。
# MAGIC
# MAGIC **臨床的意義**
# MAGIC - 1回だけの処方 → 試験的投与や除外診断の可能性
# MAGIC - 2ヶ月以上の処方 → 継続的な治療を受けている確度が高い
# MAGIC - 6ヶ月以上の処方 → 確実にRA治療を受けている慢性患者

# COMMAND ----------

# 患者ごとのDMARDs処方月数を計算
# service_month（診療年月）をDISTINCTでカウントすることで、処方のあった月数を算出
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
# MAGIC
# MAGIC ### 研究上の活用
# MAGIC
# MAGIC **査読対応での活用**
# MAGIC - 査読者から「定義が厳しすぎる/緩すぎる」とコメントされた場合、このセクションの結果を見て妥当性を説明できる
# MAGIC - 「感度分析として他の定義でも試してみて」と言われた場合、閾値を変更してRun Allで即座に対応可能
# MAGIC
# MAGIC **研究計画での活用**
# MAGIC - 倫理審査前に、各定義での患者数を確認し、統計的検出力を検討できる
# MAGIC - サンプルサイズが不足しそうなら、定義を調整してから研究計画書を提出できる

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
# MAGIC
# MAGIC ### データ構造の変換
# MAGIC
# MAGIC ここでは、トランザクション型データ（1処方1行）を集約型データ（1患者1行）に変換します。
# MAGIC 各患者について、各薬剤・診療行為を「使用した/しなかった」の0/1フラグとして保持します。
# MAGIC
# MAGIC **研究上の意義**
# MAGIC - **集計の効率化**: フラグ化することで、年齢層別の使用率計算が単純な平均値計算になる
# MAGIC - **多変量解析への対応**: ロジスティック回帰等の説明変数として直接使用可能
# MAGIC - **クロス集計**: 薬剤間の併用パターン分析が容易

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 薬剤使用フラグ
# MAGIC
# MAGIC 各患者について、期間中に1回でも処方があれば「使用あり（1）」とします。

# COMMAND ----------

# 薬剤別のCASE文を生成
# DMARD_CODESの各薬剤グループについて、使用の有無をMAX(CASE WHEN...)で判定
drug_case_clauses = []
for drug_name, codes in DMARD_CODES.items():
    clause = f"MAX(CASE WHEN drug_code IN ({sql_list(codes)}) THEN 1 ELSE 0 END) AS {drug_name}"
    drug_case_clauses.append(clause)

# ステロイド
drug_case_clauses.append(
    f"MAX(CASE WHEN drug_code IN ({sql_list(CS_CODES)}) THEN 1 ELSE 0 END) AS CS"
)

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

# print(f"診療行為フラグ作成完了: {df_proc_flags.count():,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver層: RA患者マスタの作成
# MAGIC
# MAGIC ### データ層構造の意義
# MAGIC
# MAGIC **Silver層の役割**
# MAGIC - 1患者1行の形式で、全ての情報を統合
# MAGIC - 薬剤・診療行為を0/1フラグとして保持
# MAGIC - 後続の集計（Gold層）の基礎データとなる
# MAGIC
# MAGIC **研究上の利点**
# MAGIC - **再利用性**: 異なる集計軸（性別、地域等）での分析にも対応可能
# MAGIC - **データ品質**: この段階で欠損値処理やフラグ作成を完了させ、Gold層の集計をシンプルに
# MAGIC - **共有**: 共同研究者にSilver層テーブルを共有すれば、各自が自由に集計可能
# MAGIC - **監査証跡**: いつ誰がどのデータを使ったかがUnity Catalogで記録される

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
# 薬剤・診療行為データがない患者は、使用なし（0）として扱う
flag_cols = list(DMARD_CODES.keys()) + ["CS"] + PROCEDURE_TYPES
for col in flag_cols:
    df_ra_master = df_ra_master.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# 複合フラグを追加
# 臨床的に意味のある薬剤グループや診療行為グループを作成
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
# MAGIC
# MAGIC ### Gold層の意義
# MAGIC
# MAGIC **Gold層の役割**
# MAGIC - 論文の表・図に直接使える形式に集計
# MAGIC - BIツール（Tableau、Power BI等）で可視化可能
# MAGIC - SQLで簡単にアクセス可能
# MAGIC
# MAGIC **研究上の利点**
# MAGIC - **論文執筆の効率化**: このテーブルをCSVエクスポートして論文に貼り付けるだけ
# MAGIC - **プレゼン資料作成**: Gold層テーブルからグラフを直接作成
# MAGIC - **結果の再現性**: 「Table 2の数値はどこから?」→「このGoldテーブルです」と明示できる
# MAGIC - **データ更新への対応**: Bronze層が更新されても、Run Allで最新の集計を自動生成

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
    .withColumn("pct_of_total", F.round(F.col("n") / F.when(F.lit(total_ra) == 0, None).otherwise(F.lit(total_ra)) * 100, 1))
    .withColumn("female_pct", F.round(F.col("n_female") / F.when(F.col("n") == 0, None).otherwise(F.col("n")) * 100, 1))
    .withColumn("fm_ratio", F.round(F.col("n_female") / F.when(F.col("n_male") == 0, None).otherwise(F.col("n_male")), 2))
    .withColumn("prevalence", F.round(F.col("n") / F.when(F.col("n_pop") == 0, None).otherwise(F.col("n_pop")) * 100, 2))
    .select("age_group", "n", "pct_of_total", "female_pct", "fm_ratio", "prevalence")
)

# Total行を追加
n_female = df_ra.filter("sex = '2'").count()
n_male = df_ra.filter("sex = '1'").count()
total_row = spark.createDataFrame([
    ("Total", total_ra, 100.0, round(n_female/total_ra*100, 1) if total_ra != 0 else None, 
     round(n_female/n_male, 2) if n_male != 0 else None, round(total_ra/total_pop*100, 2) if total_pop != 0 else None)
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
# MAGIC
# MAGIC Goldテーブルのデータを用いて、論文やプレゼン用のグラフを作成します。
# MAGIC
# MAGIC ### 研究上の活用
# MAGIC
# MAGIC **論文での使用**
# MAGIC - これらのグラフを高解像度でエクスポートし、論文のFigureとして使用可能
# MAGIC - グラフの体裁（色、フォント等）を調整して、ジャーナルのスタイルに合わせる
# MAGIC
# MAGIC **プレゼンテーションでの使用**
# MAGIC - 学会発表用のスライドに直接貼り付け可能
# MAGIC - データ更新時もこのセルを再実行するだけで最新のグラフを生成
# MAGIC
# MAGIC **探索的データ解析**
# MAGIC - 年齢層による傾向を視覚的に把握
# MAGIC - 異常値や予想外のパターンを発見

# COMMAND ----------

# pandas に変換
# Sparkデータフレームをpandasに変換してmatplotlibで可視化
pdf_age = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.age_distribution").filter("age_group != 'Total'").toPandas()
pdf_med = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.medication_usage").filter("age_group != 'Total'").toPandas()

fig, axes = plt.subplots(2, 2, figsize=(12, 8))

# 1. 年齢層別患者数
ax1 = axes[0, 0]
ax1.bar(pdf_age["age_group"], pdf_age["n"], color="steelblue")
ax1.set_xlabel("年齢層")
ax1.set_ylabel("患者数")
ax1.set_title("年齢層別RA患者数分布")
ax1.tick_params(axis="x", rotation=45)

# 2. 有病率
ax2 = axes[0, 1]
ax2.plot(pdf_age["age_group"], pdf_age["prevalence"], "o-", color="coral")
ax2.set_xlabel("年齢層")
ax2.set_ylabel("有病率 (%)")
ax2.set_title("年齢層別RA有病率")
ax2.tick_params(axis="x", rotation=45)

# 3. MTX使用率
ax3 = axes[1, 0]
ax3.bar(pdf_med["age_group"], pdf_med["MTX"], color="green")
ax3.set_xlabel("年齢層")
ax3.set_ylabel("MTX使用率 (%)")
ax3.set_title("年齢層別MTX使用率")
ax3.tick_params(axis="x", rotation=45)

# 4. bDMARDs使用率
ax4 = axes[1, 1]
ax4.bar(pdf_med["age_group"], pdf_med["bDMARDs"], color="purple")
ax4.set_xlabel("年齢層")
ax4.set_ylabel("bDMARDs使用率 (%)")
ax4.set_title("年齢層別bDMARDs使用率")
ax4.tick_params(axis="x", rotation=45)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. サマリー
# MAGIC
# MAGIC ### このノートブックの成果物
# MAGIC
# MAGIC | データ層 | テーブル名 | 用途 |
# MAGIC |---------|-----------|------|
# MAGIC | Silver | `ra_patients` | 1患者1行の詳細データ、追加分析の基礎 |
# MAGIC | Gold | `age_distribution` | 論文Table 1相当、患者背景の記述 |
# MAGIC | Gold | `medication_usage` | 論文Table 2相当、治療パターン分析 |
# MAGIC | Gold | `procedure_usage` | 論文Table 3相当、外科的介入の評価 |
# MAGIC
# MAGIC ### 次のステップ
# MAGIC
# MAGIC **このノートブックで完成していること**
# MAGIC - RA患者の抽出・集計
# MAGIC - 年齢層別の基本的な記述統計
# MAGIC
# MAGIC **さらに発展させるには**
# MAGIC - 経年変化の分析（複数年のデータを結合）
# MAGIC - 地域差の分析（都道府県別の集計）
# MAGIC - 多変量解析（ロジスティック回帰等）
# MAGIC - 医療費の分析（bDMARDsの医療費インパクト）
# MAGIC
# MAGIC これらの発展的分析も、Silver層テーブルを基に別のノートブックで実施できます。

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
# MAGIC ## 研究実務でのTips
# MAGIC
# MAGIC ### 定義を変更する場合
# MAGIC 1. 「1. 設定」セクションの `DMARD_MONTHS_THRESHOLD` を変更
# MAGIC 2. Run All で全セル実行
# MAGIC 3. セクション10のサマリーで結果を確認
# MAGIC 4. 異なる定義での結果を比較し、感度分析として論文に記載
# MAGIC
# MAGIC ### データが更新された場合（年次更新等）
# MAGIC 1. Bronze層に新年度のデータを追加
# MAGIC 2. このノートブックをRun All
# MAGIC 3. Silver/Gold層が自動で再生成される
# MAGIC 4. 過去との比較は、Delta Lakeのタイムトラベル機能で可能
# MAGIC
# MAGIC ### 結果を論文・プレゼンで使う場合
# MAGIC ```sql
# MAGIC -- SQLでGoldテーブルを取得
# MAGIC SELECT * FROM reprod_paper08.gold.medication_usage
# MAGIC ```
# MAGIC - CSVエクスポートして論文の表に使用
# MAGIC - Power BI/Tableauで可視化してプレゼン資料に使用
# MAGIC - このノートブック自体をSupplementary Materialとして投稿
# MAGIC
# MAGIC ### 共同研究者と共有する場合
# MAGIC 1. このノートブックをワークスペースで共有（閲覧権限）
# MAGIC 2. Silver/Goldテーブルへのアクセス権限を付与
# MAGIC 3. 共同研究者は同じ環境で結果を確認・検証可能
# MAGIC 4. 変更履歴はGit連携で管理
# MAGIC
# MAGIC ### 監査・倫理審査対応
# MAGIC - Unity Catalogのアクセスログで「誰がいつデータを参照したか」を記録
# MAGIC - ノートブックの実行履歴で「いつどの設定で解析したか」を追跡可能
# MAGIC - Delta Lakeの履歴で「データの変更」を追跡可能
# MAGIC
# MAGIC ### トラブルシューティング
# MAGIC - **患者数が想定より少ない**: セクション4の定義別患者数を確認し、閾値を調整
# MAGIC - **エラーが出る**: セルを上から順に実行し、どこでエラーが出るかを特定
# MAGIC - **結果が前回と異なる**: Bronze層データが更新されていないか確認
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 参考: Delta Lakeタイムトラベルの使い方
# MAGIC
# MAGIC ```sql
# MAGIC -- 2024年1月1日時点のデータを参照
# MAGIC SELECT * FROM reprod_paper08.silver.ra_patients
# MAGIC VERSION AS OF '2024-01-01'
# MAGIC
# MAGIC -- 10世代前のデータを参照
# MAGIC SELECT * FROM reprod_paper08.silver.ra_patients
# MAGIC VERSION AS OF 10
# MAGIC ```
# MAGIC
# MAGIC これにより、「論文投稿時のデータ」「査読対応時のデータ」を明確に区別して管理できます。
