# Databricks notebook source
# MAGIC %md
# MAGIC # Paper08 再現プロジェクト: 共通定数定義
# MAGIC
# MAGIC 全レイヤー（Bronze/Silver/Gold）で使用する定数を定義します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 研究期間の定義

# COMMAND ----------

# 研究期間（論文と同じ）
STUDY_PERIOD_START = "2017-04-01"
STUDY_PERIOD_END = "2018-03-31"
REFERENCE_DATE = "2017-10-01"  # 年齢計算基準日

# 会計年度
FY = "2017"

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ生成パラメータ

# COMMAND ----------

# 総患者数（実際のNDBの1/100スケール）
N_TOTAL_PATIENTS = 10000

# RA有病率（論文値: 0.65%）
RA_PREVALENCE = 0.0065

# RA候補患者の生成比率（定義適用後に目標有病率になるよう調整）
RA_CANDIDATE_RATIO = 0.015  # 1.5%の患者をRA候補として生成

# 女性比率（RA患者における）
RA_FEMALE_RATIO = 0.763  # 76.3%

# 一般人口の女性比率
GENERAL_FEMALE_RATIO = 0.51  # 51%

# COMMAND ----------

# MAGIC %md
# MAGIC ## 年齢群定義

# COMMAND ----------

# 年齢群の定義（論文Table 2と同じ）
AGE_GROUPS = [
    ("16-19", 16, 19),
    ("20-29", 20, 29),
    ("30-39", 30, 39),
    ("40-49", 40, 49),
    ("50-59", 50, 59),
    ("60-69", 60, 69),
    ("70-79", 70, 79),
    ("80-84", 80, 84),
    ("85+", 85, 100),
]

# RA患者の年齢群別分布（論文Table 2の%値）
AGE_GROUP_RA_DISTRIBUTION = {
    "16-19": 0.5,
    "20-29": 1.5,
    "30-39": 4.8,
    "40-49": 10.1,
    "50-59": 14.9,
    "60-69": 26.4,
    "70-79": 28.6,
    "80-84": 6.1,
    "85+": 7.0,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## RA関連ICD-10コード

# COMMAND ----------

# RA関連ICD-10コード（論文の定義に基づく）
RA_ICD10_CODES = [
    # M05.x: 血清反応陽性関節リウマチ
    "M050", "M051", "M052", "M053", "M058", "M059",
    # M06.x: その他の関節リウマチ（M061とM064を除く）
    "M060", "M062", "M063", "M068", "M069",
    # M08.x: 若年性関節炎（M081とM082を除く）
    "M080", "M083", "M084", "M088", "M089",
]

# 除外するICD-10コード（論文で除外されているもの）
EXCLUDED_ICD10_CODES = [
    "M061",  # 成人発症スティル病
    "M064",  # 炎症性多発性関節症
    "M081", "M082",  # その他の若年性関節炎
]

# 非RA疾患コード（ダミーデータ生成用）
NON_RA_ICD10_CODES = [
    "J00",   # 急性鼻咽頭炎
    "J06",   # 急性上気道感染症
    "I10",   # 本態性高血圧
    "E11",   # 2型糖尿病
    "K21",   # 胃食道逆流症
    "M54",   # 背部痛
    "G43",   # 片頭痛
    "F32",   # うつ病性エピソード
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## DMARDs医薬品コード

# COMMAND ----------

# DMARDs医薬品マスタ（論文に基づく分類）
DMARDS_MASTER = {
    # csDMARDs（従来型合成DMARD）
    "MTX": {
        "codes": ["1199101", "1199102"],
        "category": "csDMARD",
        "name_ja": "メトトレキサート",
    },
    "SSZ": {
        "codes": ["1199201"],
        "category": "csDMARD",
        "name_ja": "サラゾスルファピリジン",
    },
    "BUC": {
        "codes": ["1199401"],
        "category": "csDMARD",
        "name_ja": "ブシラミン",
    },
    "TAC": {
        "codes": ["1199301"],
        "category": "csDMARD",
        "name_ja": "タクロリムス",
    },
    "IGT": {
        "codes": ["1199501"],
        "category": "csDMARD",
        "name_ja": "イグラチモド",
    },
    "LEF": {
        "codes": ["1199601"],
        "category": "csDMARD",
        "name_ja": "レフルノミド",
    },

    # bDMARDs（生物学的DMARD）- TNFI
    "IFX": {
        "codes": ["4400101"],
        "category": "TNFI",
        "name_ja": "インフリキシマブ",
    },
    "ETN": {
        "codes": ["4400102"],
        "category": "TNFI",
        "name_ja": "エタネルセプト",
    },
    "ADA": {
        "codes": ["4400103"],
        "category": "TNFI",
        "name_ja": "アダリムマブ",
    },
    "GLM": {
        "codes": ["4400104"],
        "category": "TNFI",
        "name_ja": "ゴリムマブ",
    },
    "CZP": {
        "codes": ["4400105"],
        "category": "TNFI",
        "name_ja": "セルトリズマブペゴル",
    },

    # bDMARDs - IL6I
    "TCZ": {
        "codes": ["4400201"],
        "category": "IL6I",
        "name_ja": "トシリズマブ",
    },
    "SAR": {
        "codes": ["4400202"],
        "category": "IL6I",
        "name_ja": "サリルマブ",
    },

    # bDMARDs - ABT
    "ABT": {
        "codes": ["4400301"],
        "category": "ABT",
        "name_ja": "アバタセプト",
    },

    # tsDMARDs（分子標的型合成DMARD）- JAKi
    "TOF": {
        "codes": ["4400401"],
        "category": "JAKi",
        "name_ja": "トファシチニブ",
    },
    "BAR": {
        "codes": ["4400402"],
        "category": "JAKi",
        "name_ja": "バリシチニブ",
    },
}

# カテゴリ別にコードをまとめる
CSDMARD_CODES = [code for drug, info in DMARDS_MASTER.items()
                 if info["category"] == "csDMARD" for code in info["codes"]]

BDMARD_TNFI_CODES = [code for drug, info in DMARDS_MASTER.items()
                     if info["category"] == "TNFI" for code in info["codes"]]

BDMARD_IL6I_CODES = [code for drug, info in DMARDS_MASTER.items()
                     if info["category"] == "IL6I" for code in info["codes"]]

BDMARD_ABT_CODES = [code for drug, info in DMARDS_MASTER.items()
                    if info["category"] == "ABT" for code in info["codes"]]

TSDMARD_JAKI_CODES = [code for drug, info in DMARDS_MASTER.items()
                      if info["category"] == "JAKi" for code in info["codes"]]

# 全DMARDsコード
ALL_DMARD_CODES = (
    CSDMARD_CODES + BDMARD_TNFI_CODES + BDMARD_IL6I_CODES +
    BDMARD_ABT_CODES + TSDMARD_JAKI_CODES
)

# 経口ステロイドコード
CS_CODES = ["2454001", "2454002", "2454003"]

# NSAIDsコード（ダミーデータ生成用）
NSAID_CODES = ["1141001", "1141002", "1141003"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 薬剤使用率（論文値）

# COMMAND ----------

# 薬剤使用率（論文Table 3の全年齢平均値）
DRUG_USAGE_RATES = {
    "MTX": 0.634,    # 63.4%
    "SSZ": 0.249,    # 24.9%
    "BUC": 0.145,    # 14.5%
    "TAC": 0.106,    # 10.6%
    "IGT": 0.047,    # 4.7%
    "LEF": 0.030,    # 3.0%
    "TNFI": 0.135,   # 13.5%
    "IL6I": 0.060,   # 6.0%
    "ABT": 0.045,    # 4.5%
    "JAKi": 0.010,   # 1.0%
    "CS": 0.450,     # 45.0%（ステロイド）
    "NSAID": 0.600,  # 60.0%（推定）
}

# bDMARDs全体の使用率
BDMARD_TOTAL_RATE = 0.229  # 22.9%

# COMMAND ----------

# MAGIC %md
# MAGIC ## 診療行為（手術・検査）コード

# COMMAND ----------

# 診療行為タイプ
PROCEDURE_TYPES = {
    "TJR": {
        "name_ja": "人工関節全置換術",
        "base_rate": 0.01,  # 1%
    },
    "ARTHROPLASTY": {
        "name_ja": "関節形成術",
        "base_rate": 0.003,  # 0.3%
    },
    "SYNOVECTOMY": {
        "name_ja": "滑膜切除術",
        "base_rate": 0.001,  # 0.1%
    },
    "ULTRASOUND": {
        "name_ja": "関節超音波検査",
        "base_rate": 0.18,  # 18%
    },
    "BMD": {
        "name_ja": "骨密度測定",
        "base_rate": 0.05,  # 5%（年齢により増加）
    },
    "VISIT": {
        "name_ja": "通常受診",
        "base_rate": 1.0,  # 全員
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog設定

# COMMAND ----------

# Unity Catalog 3階層構造
CATALOG_NAME = "reprod_paper08"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# テーブル命名規則（Unity Catalog対応: catalog.schema.table）
# Bronze層
BRONZE_TABLES = {
    "patients": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.patients",
    "re_receipt": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.re_receipt",
    "sy_disease": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.sy_disease",
    "iy_medication": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.iy_medication",
    "si_procedure": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.si_procedure",
    "ho_insurer": f"{CATALOG_NAME}.{BRONZE_SCHEMA}.ho_insurer",
}

# Silver層
SILVER_TABLES = {
    "ra_patients_def3": f"{CATALOG_NAME}.{SILVER_SCHEMA}.ra_patients_def3",
    "ra_definitions_summary": f"{CATALOG_NAME}.{SILVER_SCHEMA}.ra_definitions_summary",
}

# Gold層
GOLD_TABLES = {
    "table2_age_distribution": f"{CATALOG_NAME}.{GOLD_SCHEMA}.table2_age_distribution",
    "table3_medication": f"{CATALOG_NAME}.{GOLD_SCHEMA}.table3_medication",
    "table4_procedures": f"{CATALOG_NAME}.{GOLD_SCHEMA}.table4_procedures",
    "summary": f"{CATALOG_NAME}.{GOLD_SCHEMA}.summary",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 論文の参照値（検証用）

# COMMAND ----------

# 論文の主要結果（検証用）
PAPER_REFERENCE_VALUES = {
    "total_ra_patients": 825772,
    "prevalence_pct": 0.65,
    "female_ratio_pct": 76.3,
    "elderly_65_plus_pct": 60.8,
    "elderly_85_plus_pct": 7.0,
    "mtx_usage_pct": 63.4,
    "bdmard_usage_pct": 22.9,
    "cs_usage_pct": 45.0,
    "tjr_rate_pct": 0.93,
}

# 年齢群別の論文値（Table 2）
PAPER_TABLE2_VALUES = {
    "16-19": {"pct": 0.5, "prevalence": 0.03, "female_pct": 64.0, "fm_ratio": 1.77},
    "20-29": {"pct": 1.5, "prevalence": 0.07, "female_pct": 79.8, "fm_ratio": 3.94},
    "30-39": {"pct": 4.8, "prevalence": 0.27, "female_pct": 80.7, "fm_ratio": 3.61},
    "40-49": {"pct": 10.1, "prevalence": 0.44, "female_pct": 80.7, "fm_ratio": 4.18},
    "50-59": {"pct": 14.9, "prevalence": 0.79, "female_pct": 79.3, "fm_ratio": 3.84},
    "60-69": {"pct": 26.4, "prevalence": 1.23, "female_pct": 76.9, "fm_ratio": 3.33},
    "70-79": {"pct": 28.6, "prevalence": 1.63, "female_pct": 74.3, "fm_ratio": 2.89},
    "80-84": {"pct": 6.1, "prevalence": 1.14, "female_pct": 74.2, "fm_ratio": 2.88},
    "85+": {"pct": 7.0, "prevalence": 0.89, "female_pct": 77.7, "fm_ratio": 3.49},
}

# MTX使用率（年齢群別、Table 3）
PAPER_TABLE3_MTX_VALUES = {
    "16-19": 59.9, "20-29": 60.9, "30-39": 64.7, "40-49": 69.9,
    "50-59": 73.1, "60-69": 70.9, "70-79": 60.4, "80-84": 50.5, "85+": 38.2
}

# bDMARDs使用率（年齢群別、Table 3）
PAPER_TABLE3_BDMARD_VALUES = {
    "16-19": 50.9, "20-29": 42.9, "30-39": 34.8, "40-49": 30.9,
    "50-59": 27.9, "60-69": 22.4, "70-79": 17.8, "80-84": 15.0, "85+": 13.7
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## ヘルパー関数

# COMMAND ----------

def get_full_table_name(table_key, layer="bronze"):
    """
    テーブルの完全修飾名を取得

    Parameters:
    -----------
    table_key : str
        テーブルキー（例: "patients", "ra_patients_def3"）
    layer : str
        レイヤー名（"bronze", "silver", "gold"）

    Returns:
    --------
    str : 完全修飾テーブル名（例: "reprod_paper08.bronze_patients"）
    """
    if layer == "bronze":
        table_name = BRONZE_TABLES.get(table_key, f"{BRONZE_PREFIX}{table_key}")
    elif layer == "silver":
        table_name = SILVER_TABLES.get(table_key, f"{SILVER_PREFIX}{table_key}")
    elif layer == "gold":
        table_name = GOLD_TABLES.get(table_key, f"{GOLD_PREFIX}{table_key}")
    else:
        raise ValueError(f"Unknown layer: {layer}")

    return f"{DATABASE_NAME}.{table_name}"


def get_age_group(age):
    """
    年齢から年齢群を取得

    Parameters:
    -----------
    age : int
        年齢

    Returns:
    --------
    str : 年齢群（"16-19", ..., "85+"）
    """
    for group_name, min_age, max_age in AGE_GROUPS:
        if min_age <= age <= max_age:
            return group_name
    return "Unknown"


def normalize_distribution(distribution_dict):
    """
    分布の正規化（合計を1.0にする）

    Parameters:
    -----------
    distribution_dict : dict
        分布の辞書（例: AGE_GROUP_RA_DISTRIBUTION）

    Returns:
    --------
    dict : 正規化された分布
    """
    total = sum(distribution_dict.values())
    return {k: v / total for k, v in distribution_dict.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定の確認

# COMMAND ----------

print("=" * 60)
print("Paper08 再現プロジェクト: 設定確認")
print("=" * 60)
print(f"\n【データベース設定】")
print(f"データベース名: {DATABASE_NAME}")
print(f"\n【データ生成パラメータ】")
print(f"総患者数: {N_TOTAL_PATIENTS:,}")
print(f"RA有病率: {RA_PREVALENCE:.2%}")
print(f"RA候補比率: {RA_CANDIDATE_RATIO:.2%}")
print(f"\n【ICD-10コード】")
print(f"RA関連コード数: {len(RA_ICD10_CODES)}")
print(f"非RA疾患コード数: {len(NON_RA_ICD10_CODES)}")
print(f"\n【DMARDs設定】")
print(f"csDMARDs: {len(CSDMARD_CODES)} codes")
print(f"bDMARDs (TNFI): {len(BDMARD_TNFI_CODES)} codes")
print(f"bDMARDs (IL6I): {len(BDMARD_IL6I_CODES)} codes")
print(f"bDMARDs (ABT): {len(BDMARD_ABT_CODES)} codes")
print(f"tsDMARDs (JAKi): {len(TSDMARD_JAKI_CODES)} codes")
print(f"全DMARD codes: {len(ALL_DMARD_CODES)}")
print(f"\n【年齢群】")
for group_name, min_age, max_age in AGE_GROUPS:
    dist_pct = AGE_GROUP_RA_DISTRIBUTION.get(group_name, 0)
    print(f"  {group_name}: {min_age}-{max_age}歳 (RA分布: {dist_pct}%)")
print("\n" + "=" * 60)
