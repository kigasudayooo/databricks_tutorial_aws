#!/usr/bin/env python3
"""
Paper08 再現プロジェクト: データ生成スクリプト
Bronze -> Silver -> Gold のパイプラインを一括実行
"""

import hashlib
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

np.random.seed(42)

# ディレクトリ設定
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data/bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data/silver")
GOLD_DIR = os.path.join(BASE_DIR, "data/gold")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

print("=" * 60)
print("Paper08 再現データ生成を開始します")
print("=" * 60)

# =============================================================================
# 定数・マスタデータの定義
# =============================================================================
STUDY_PERIOD_START = "2017-04-01"
STUDY_PERIOD_END = "2018-03-31"
REFERENCE_DATE = "2017-10-01"

N_TOTAL_PATIENTS = 10000
RA_PREVALENCE = 0.0065

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

RA_ICD10_CODES = [
    "M050",
    "M051",
    "M052",
    "M053",
    "M058",
    "M059",
    "M060",
    "M062",
    "M063",
    "M068",
    "M069",
    "M080",
    "M083",
    "M084",
    "M088",
    "M089",
]

NON_RA_ICD10_CODES = ["J00", "J06", "I10", "E11", "K21", "M54", "G43", "F32"]

DMARDS_MASTER = {
    "MTX": {"codes": ["1199101", "1199102"], "category": "csDMARD"},
    "SSZ": {"codes": ["1199201"], "category": "csDMARD"},
    "TAC": {"codes": ["1199301"], "category": "csDMARD"},
    "BUC": {"codes": ["1199401"], "category": "csDMARD"},
    "IGT": {"codes": ["1199501"], "category": "csDMARD"},
    "LEF": {"codes": ["1199601"], "category": "csDMARD"},
    "IFX": {"codes": ["4400101"], "category": "TNFI"},
    "ETN": {"codes": ["4400102"], "category": "TNFI"},
    "ADA": {"codes": ["4400103"], "category": "TNFI"},
    "GLM": {"codes": ["4400104"], "category": "TNFI"},
    "CZP": {"codes": ["4400105"], "category": "TNFI"},
    "TCZ": {"codes": ["4400201"], "category": "IL6I"},
    "SAR": {"codes": ["4400202"], "category": "IL6I"},
    "ABT": {"codes": ["4400301"], "category": "ABT"},
    "TOF": {"codes": ["4400401"], "category": "JAKi"},
    "BAR": {"codes": ["4400402"], "category": "JAKi"},
}

CS_CODES = ["2454001", "2454002", "2454003"]
ALL_DMARD_CODES = [code for drug in DMARDS_MASTER.values() for code in drug["codes"]]

DRUG_USAGE_RATES = {
    "MTX": 0.634,
    "SSZ": 0.249,
    "BUC": 0.145,
    "TAC": 0.106,
    "IGT": 0.047,
    "LEF": 0.030,
    "TNFI": 0.135,
    "IL6I": 0.060,
    "ABT": 0.045,
    "JAKi": 0.010,
    "CS": 0.450,
    "NSAID": 0.600,
}

# =============================================================================
# Bronze Layer: ダミーデータ生成
# =============================================================================
print("\n[1/3] Bronze Layer: ダミーデータ生成中...")


def generate_hash_key(patient_id: int) -> str:
    return hashlib.sha256(f"patient_{patient_id}".encode()).hexdigest()[:32]


def generate_birth_date(age: int, reference_date: str) -> str:
    ref = datetime.strptime(reference_date, "%Y-%m-%d")
    birth_year = ref.year - age
    birth_month = np.random.randint(1, 13)
    birth_day = np.random.randint(1, 29)
    return f"{birth_year}-{birth_month:02d}-{birth_day:02d}"


def assign_age_group(age: int) -> str:
    for group_name, min_age, max_age in AGE_GROUPS:
        if min_age <= age <= max_age:
            return group_name
    return "Unknown"


# 患者データ生成
n_ra_patients = int(N_TOTAL_PATIENTS * RA_PREVALENCE * 1.5)
n_non_ra_patients = N_TOTAL_PATIENTS - n_ra_patients
patients = []

age_probs = np.array(list(AGE_GROUP_RA_DISTRIBUTION.values()))
age_probs = age_probs / age_probs.sum()  # 確率を正規化して合計1にする

for i in range(n_ra_patients):
    age_group = np.random.choice(list(AGE_GROUP_RA_DISTRIBUTION.keys()), p=age_probs)
    for group_name, min_age, max_age in AGE_GROUPS:
        if group_name == age_group:
            age = np.random.randint(min_age, max_age + 1)
            break
    sex = "2" if np.random.random() < 0.763 else "1"
    patients.append(
        {
            "patient_id": i,
            "共通キー": generate_hash_key(i),
            "age": age,
            "age_group": age_group,
            "sex": sex,
            "birth_date": generate_birth_date(age, REFERENCE_DATE),
            "is_ra_candidate": True,
        }
    )

for i in range(n_non_ra_patients):
    patient_id = n_ra_patients + i
    age = np.random.randint(16, 90)
    sex = "2" if np.random.random() < 0.51 else "1"
    patients.append(
        {
            "patient_id": patient_id,
            "共通キー": generate_hash_key(patient_id),
            "age": age,
            "age_group": assign_age_group(age),
            "sex": sex,
            "birth_date": generate_birth_date(age, REFERENCE_DATE),
            "is_ra_candidate": False,
        }
    )

df_patients = pd.DataFrame(patients)
print(f"  患者数: {len(df_patients)} (RA候補: {df_patients['is_ra_candidate'].sum()})")

# レセプト基本データ生成
months = pd.date_range(start=STUDY_PERIOD_START, end=STUDY_PERIOD_END, freq="MS")
re_records = []
for _, patient in df_patients.iterrows():
    n_visits = np.random.randint(2, 13) if patient["is_ra_candidate"] else np.random.randint(1, 5)
    visit_months = np.random.choice(months, size=min(n_visits, len(months)), replace=False)
    for visit_month in visit_months:
        visit_dt = pd.Timestamp(visit_month)
        re_records.append(
            {
                "共通キー": patient["共通キー"],
                "検索番号": f"RCP{patient['patient_id']:08d}{visit_dt.strftime('%Y%m')}",
                "データ識別": "1",
                "診療年月": visit_dt.strftime("%Y%m"),
                "男女区分": patient["sex"],
                "生年月日": patient["birth_date"],
                "fy": "2017",
                "year": visit_dt.strftime("%Y"),
                "month": visit_dt.strftime("%m"),
                "prefecture_number": f"{np.random.randint(1, 48):02d}",
            }
        )
df_re = pd.DataFrame(re_records)
print(f"  レセプト数: {len(df_re)}")

# 傷病データ生成
df_merged = df_re.merge(df_patients[["共通キー", "is_ra_candidate"]], on="共通キー", how="left")
sy_records = []
for _, receipt in df_merged.iterrows():
    n_diseases = np.random.randint(1, 5)
    for j in range(n_diseases):
        if receipt["is_ra_candidate"] and j == 0:
            icd10 = np.random.choice(RA_ICD10_CODES)
        else:
            icd10 = np.random.choice(NON_RA_ICD10_CODES)
        sy_records.append(
            {
                "共通キー": receipt["共通キー"],
                "検索番号": receipt["検索番号"],
                "ICD10コード": icd10,
                "診療年月": receipt["診療年月"],
            }
        )
df_sy = pd.DataFrame(sy_records)
print(f"  傷病レコード数: {len(df_sy)}")

# 医薬品データ生成
df_merged2 = df_re.merge(df_patients[["共通キー", "is_ra_candidate", "age"]], on="共通キー", how="left")
iy_records = []
for _, receipt in df_merged2.iterrows():
    if receipt["is_ra_candidate"]:
        age = receipt["age"]
        mtx_rate = DRUG_USAGE_RATES["MTX"] * (0.6 if age >= 85 else 0.8 if age >= 80 else 1.0)

        if np.random.random() < mtx_rate:
            iy_records.append(
                {
                    "共通キー": receipt["共通キー"],
                    "検索番号": receipt["検索番号"],
                    "医薬品コード": "1199101",
                    "drug_category": "csDMARD",
                    "drug_name": "MTX",
                }
            )

        for drug, rate in [("SSZ", 0.249), ("BUC", 0.145), ("TAC", 0.106)]:
            if np.random.random() < rate:
                iy_records.append(
                    {
                        "共通キー": receipt["共通キー"],
                        "検索番号": receipt["検索番号"],
                        "医薬品コード": DMARDS_MASTER[drug]["codes"][0],
                        "drug_category": "csDMARD",
                        "drug_name": drug,
                    }
                )

        bdmard_rate = 0.229 * (2.0 if age < 40 else 0.6 if age >= 85 else 1.0)
        if np.random.random() < bdmard_rate:
            if age >= 70:
                bdmard_type = np.random.choice(["TNFI", "IL6I", "ABT"], p=[0.5, 0.25, 0.25])
            else:
                bdmard_type = np.random.choice(["TNFI", "IL6I", "ABT"], p=[0.6, 0.25, 0.15])

            if bdmard_type == "TNFI":
                drug = np.random.choice(["IFX", "ETN", "ADA", "GLM", "CZP"])
            elif bdmard_type == "IL6I":
                drug = np.random.choice(["TCZ", "SAR"])
            else:
                drug = "ABT"
            iy_records.append(
                {
                    "共通キー": receipt["共通キー"],
                    "検索番号": receipt["検索番号"],
                    "医薬品コード": DMARDS_MASTER[drug]["codes"][0],
                    "drug_category": bdmard_type,
                    "drug_name": drug,
                }
            )

        if np.random.random() < 0.01:
            drug = np.random.choice(["TOF", "BAR"])
            iy_records.append(
                {
                    "共通キー": receipt["共通キー"],
                    "検索番号": receipt["検索番号"],
                    "医薬品コード": DMARDS_MASTER[drug]["codes"][0],
                    "drug_category": "JAKi",
                    "drug_name": drug,
                }
            )

        if np.random.random() < 0.45:
            iy_records.append(
                {
                    "共通キー": receipt["共通キー"],
                    "検索番号": receipt["検索番号"],
                    "医薬品コード": "2454001",
                    "drug_category": "CS",
                    "drug_name": "CS",
                }
            )

df_iy = pd.DataFrame(iy_records)
print(f"  医薬品レコード数: {len(df_iy)}")

# 診療行為データ生成
si_records = []
for _, receipt in df_merged2.iterrows():
    if receipt["is_ra_candidate"]:
        age = receipt["age"]
        if np.random.random() < 0.01:
            si_records.append(
                {"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "TJR"}
            )
        if np.random.random() < 0.003:
            si_records.append(
                {"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "ARTHROPLASTY"}
            )
        if np.random.random() < 0.001 and age < 70:
            si_records.append(
                {"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "SYNOVECTOMY"}
            )
        if np.random.random() < 0.18:
            si_records.append(
                {"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "ULTRASOUND"}
            )
        if np.random.random() < 0.05 + age / 100 * 0.15:
            si_records.append(
                {"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "BMD"}
            )
    si_records.append({"共通キー": receipt["共通キー"], "検索番号": receipt["検索番号"], "procedure_type": "VISIT"})

df_si = pd.DataFrame(si_records)
print(f"  診療行為レコード数: {len(df_si)}")

# Bronze保存
df_patients.to_parquet(f"{BRONZE_DIR}/patients.parquet", index=False)
df_re.to_parquet(f"{BRONZE_DIR}/re_receipt.parquet", index=False)
df_sy.to_parquet(f"{BRONZE_DIR}/sy_disease.parquet", index=False)
df_iy.to_parquet(f"{BRONZE_DIR}/iy_medication.parquet", index=False)
df_si.to_parquet(f"{BRONZE_DIR}/si_procedure.parquet", index=False)
print("  Bronze保存完了")

# =============================================================================
# Silver Layer: RA患者定義適用
# =============================================================================
print("\n[2/3] Silver Layer: RA患者定義適用中...")

# Definition 0: ICD-10のみ
definition_0_patients = set(df_sy[df_sy["ICD10コード"].isin(RA_ICD10_CODES)]["共通キー"].unique())
print(f"  Definition 0: {len(definition_0_patients)} patients")

# DMARDs処方月数計算
df_dmard = df_iy[(df_iy["共通キー"].isin(definition_0_patients)) & (df_iy["医薬品コード"].isin(ALL_DMARD_CODES))].copy()
df_dmard = df_dmard.merge(df_re[["検索番号", "診療年月"]].drop_duplicates(), on="検索番号", how="left")
df_dmard_months = df_dmard.groupby("共通キー")["診療年月"].nunique().reset_index()
df_dmard_months.columns = ["共通キー", "prescription_months"]

# 各定義の適用
ra_definitions = {
    "def_0": definition_0_patients,
    "def_2": definition_0_patients & set(df_dmard_months[df_dmard_months["prescription_months"] >= 1]["共通キー"]),
    "def_3": definition_0_patients & set(df_dmard_months[df_dmard_months["prescription_months"] >= 2]["共通キー"]),
    "def_4": definition_0_patients & set(df_dmard_months[df_dmard_months["prescription_months"] >= 6]["共通キー"]),
}

for def_name, patients in ra_definitions.items():
    prevalence = len(patients) / len(df_patients) * 100
    print(f"  {def_name}: {len(patients)} patients ({prevalence:.2f}%)")

# Definition 3のRA患者マスタ作成
ra_patients_def3 = ra_definitions["def_3"]
df_ra_patients = df_patients[df_patients["共通キー"].isin(ra_patients_def3)].copy()

# 薬剤使用フラグ
drug_categories = {
    "MTX": ["1199101", "1199102"],
    "SSZ": ["1199201"],
    "BUC": ["1199401"],
    "TAC": ["1199301"],
    "IGT": ["1199501"],
    "LEF": ["1199601"],
    "TNFI": ["4400101", "4400102", "4400103", "4400104", "4400105"],
    "IL6I": ["4400201", "4400202"],
    "ABT": ["4400301"],
    "JAKi": ["4400401", "4400402"],
    "CS": ["2454001", "2454002", "2454003"],
}

df_ra_meds = df_iy[df_iy["共通キー"].isin(ra_patients_def3)]
for cat_name, codes in drug_categories.items():
    patients_with_drug = set(df_ra_meds[df_ra_meds["医薬品コード"].isin(codes)]["共通キー"])
    df_ra_patients[cat_name] = df_ra_patients["共通キー"].isin(patients_with_drug)

df_ra_patients["bDMARDs"] = df_ra_patients["TNFI"] | df_ra_patients["IL6I"] | df_ra_patients["ABT"]

# 手術フラグ
proc_types = ["TJR", "ARTHROPLASTY", "SYNOVECTOMY", "ULTRASOUND", "BMD"]
df_ra_proc = df_si[df_si["共通キー"].isin(ra_patients_def3)]
for proc in proc_types:
    patients_with_proc = set(df_ra_proc[df_ra_proc["procedure_type"] == proc]["共通キー"])
    df_ra_patients[proc] = df_ra_patients["共通キー"].isin(patients_with_proc)
df_ra_patients["any_RA_surgery"] = (
    df_ra_patients["TJR"] | df_ra_patients["ARTHROPLASTY"] | df_ra_patients["SYNOVECTOMY"]
)

# Silver保存
df_ra_patients.to_parquet(f"{SILVER_DIR}/ra_patients_def3.parquet", index=False)
df_definitions = pd.DataFrame(
    [
        {"definition": k, "n_patients": len(v), "prevalence_pct": len(v) / len(df_patients) * 100}
        for k, v in ra_definitions.items()
    ]
)
df_definitions.to_parquet(f"{SILVER_DIR}/ra_definitions_summary.parquet", index=False)
print("  Silver保存完了")

# =============================================================================
# Gold Layer: 分析結果
# =============================================================================
print("\n[3/3] Gold Layer: 分析結果作成中...")

age_group_order = ["16-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-84", "85+"]

# Table 2: 年齢層別統計
table2_data = []
for age_group in age_group_order:
    ra_group = df_ra_patients[df_ra_patients["age_group"] == age_group]
    n_ra = len(ra_group)
    n_female = (ra_group["sex"] == "2").sum()
    n_male = (ra_group["sex"] == "1").sum()
    all_group = df_patients[df_patients["age_group"] == age_group]
    table2_data.append(
        {
            "Age Group": age_group,
            "N": n_ra,
            "% of Total": f"{n_ra / len(df_ra_patients) * 100:.1f}" if len(df_ra_patients) > 0 else "0",
            "Female %": f"{n_female / n_ra * 100:.1f}" if n_ra > 0 else "0",
            "F/M Ratio": f"{n_female / n_male:.2f}" if n_male > 0 else "N/A",
            "Prevalence %": f"{n_ra / len(all_group) * 100:.2f}" if len(all_group) > 0 else "0",
        }
    )
df_table2 = pd.DataFrame(table2_data)

# Table 3: 薬剤使用率
table3_data = []
drug_cols = ["MTX", "SSZ", "BUC", "TAC", "TNFI", "IL6I", "ABT", "JAKi", "CS", "bDMARDs"]
for age_group in age_group_order:
    ra_group = df_ra_patients[df_ra_patients["age_group"] == age_group]
    row = {"Age Group": age_group, "N": len(ra_group)}
    for col in drug_cols:
        row[col] = f"{ra_group[col].mean() * 100:.1f}" if len(ra_group) > 0 else "0"
    if len(ra_group) > 0 and ra_group["ABT"].mean() > 0:
        row["TNFI/ABT"] = f"{ra_group['TNFI'].mean() / ra_group['ABT'].mean():.1f}"
    else:
        row["TNFI/ABT"] = "N/A"
    table3_data.append(row)
df_table3 = pd.DataFrame(table3_data)

# Table 4: 手術実施率
table4_data = []
proc_cols = ["TJR", "ARTHROPLASTY", "SYNOVECTOMY", "ULTRASOUND", "BMD", "any_RA_surgery"]
for age_group in age_group_order:
    ra_group = df_ra_patients[df_ra_patients["age_group"] == age_group]
    row = {"Age Group": age_group, "N": len(ra_group)}
    for col in proc_cols:
        row[col] = f"{ra_group[col].mean() * 100:.2f}" if len(ra_group) > 0 else "0"
    table4_data.append(row)
df_table4 = pd.DataFrame(table4_data)

# サマリー
total_ra = len(df_ra_patients)
summary_data = {
    "Metric": [
        "Total RA Patients",
        "Prevalence (%)",
        "Female Ratio (%)",
        "Age >= 65 (%)",
        "MTX Usage (%)",
        "bDMARDs Usage (%)",
    ],
    "Value": [
        f"{total_ra:,}",
        f"{total_ra / len(df_patients) * 100:.2f}",
        f"{(df_ra_patients['sex'] == '2').mean() * 100:.1f}",
        f"{(df_ra_patients['age'] >= 65).mean() * 100:.1f}",
        f"{df_ra_patients['MTX'].mean() * 100:.1f}",
        f"{df_ra_patients['bDMARDs'].mean() * 100:.1f}",
    ],
}
df_summary = pd.DataFrame(summary_data)

# Gold保存
df_table2.to_parquet(f"{GOLD_DIR}/table2_age_distribution.parquet", index=False)
df_table3.to_parquet(f"{GOLD_DIR}/table3_medication.parquet", index=False)
df_table4.to_parquet(f"{GOLD_DIR}/table4_procedures.parquet", index=False)
df_summary.to_parquet(f"{GOLD_DIR}/summary.parquet", index=False)

df_table2.to_csv(f"{GOLD_DIR}/table2_age_distribution.csv", index=False)
df_table3.to_csv(f"{GOLD_DIR}/table3_medication.csv", index=False)
df_table4.to_csv(f"{GOLD_DIR}/table4_procedures.csv", index=False)
df_summary.to_csv(f"{GOLD_DIR}/summary.csv", index=False)
print("  Gold保存完了")

# =============================================================================
# 結果サマリー表示
# =============================================================================
print("\n" + "=" * 60)
print("データ生成完了!")
print("=" * 60)

print("\n【主要結果サマリー】")
print(df_summary.to_string(index=False))

print("\n【年齢分布 (Table 2)】")
print(df_table2.to_string(index=False))

print("\n【薬剤使用率 (Table 3) - 一部】")
print(df_table3[["Age Group", "N", "MTX", "bDMARDs", "TNFI/ABT"]].to_string(index=False))

print("\n" + "=" * 60)
print(f"データ保存先:")
print(f"  Bronze: {BRONZE_DIR}")
print(f"  Silver: {SILVER_DIR}")
print(f"  Gold: {GOLD_DIR}")
print("=" * 60)
