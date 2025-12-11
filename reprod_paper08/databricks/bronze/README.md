# Bronze Layer: 生データ生成

## 概要

Bronze層では、NDBレセプトデータを模したダミーデータを生成します。

## ファイル構成

1. **00_setup_database.sql** - データベース作成
2. **01_create_bronze_tables.sql** - 6つのBronzeテーブル定義
3. **02_generate_bronze_data.py** - PySpark によるデータ生成

## 実行順序

```bash
# 1. データベース作成
00_setup_database.sql

# 2. テーブル定義
01_create_bronze_tables.sql

# 3. データ生成（3-5分）
02_generate_bronze_data.py
```

## 生成されるデータ

| テーブル | レコード数 | 内容 |
|---------|-----------|------|
| bronze_patients | 10,000 | 患者マスタ |
| bronze_re_receipt | ~25,000 | レセプト基本情報 |
| bronze_sy_disease | ~63,000 | 傷病名（ICD-10） |
| bronze_iy_medication | ~1,300 | 医薬品情報 |
| bronze_si_procedure | ~25,500 | 診療行為 |
| bronze_ho_insurer | 10,000 | 保険者情報 |

## 技術的なポイント

### pandas/numpy → PySpark 変換

#### 1. 確率的データ生成

**pandas（元の実装）**:
```python
age_group = np.random.choice(
    list(AGE_GROUP_RA_DISTRIBUTION.keys()),
    p=age_probs
)
```

**PySpark（Databricks実装）**:
```python
# CDFを使用した加重ランダム選択
age_group_cdf = [
    ("16-19", 0.005),
    ("20-29", 0.020),
    # ...
]

df = df.withColumn("age_group",
    F.when(F.rand() < 0.005, "16-19")
     .when(F.rand() < 0.020, "20-29")
     # ...
)
```

#### 2. ハッシュキー生成

**pandas**:
```python
hashlib.sha256(f"patient_{id}".encode()).hexdigest()[:32]
```

**PySpark**:
```python
F.substring(F.sha2(F.concat(F.lit("patient_"), F.col("patient_id")), 256), 1, 32)
```

#### 3. 年齢依存の薬剤割り当て

```python
df = df.withColumn(
    "mtx_prob",
    F.when(F.col("age") >= 85, F.lit(0.634 * 0.6))  # 高齢者で60%に減少
     .when(F.col("age") >= 80, F.lit(0.634 * 0.8))  # 80歳以上で80%
     .otherwise(F.lit(0.634))  # それ以外は標準
)
```

## データ品質チェック

```sql
-- 患者数
SELECT COUNT(*) FROM bronze_patients;  -- 10,000

-- RA候補患者
SELECT COUNT(*) FROM bronze_patients WHERE is_ra_candidate = true;  -- ~150

-- 年齢範囲
SELECT MIN(age), MAX(age) FROM bronze_patients;  -- 16, 90前後

-- 性別分布（RA候補）
SELECT sex, COUNT(*)
FROM bronze_patients
WHERE is_ra_candidate = true
GROUP BY sex;
-- sex = '2' (女性): ~114
-- sex = '1' (男性): ~36

-- ICD-10コード分布
SELECT ICD10コード, COUNT(*) as count
FROM bronze_sy_disease
WHERE ICD10コード LIKE 'M0%'
GROUP BY ICD10コード
ORDER BY count DESC;
```

## よくある問題

### 問題: データ生成が遅い

**原因**: クラスターリソース不足

**解決策**:
```python
# ノートブック先頭で shuffle partitions を調整
spark.conf.set("spark.sql.shuffle.partitions", "8")
```

### 問題: メモリ不足エラー

**原因**: クラスターメモリ不足

**解決策**:
- クラスターのメモリを増やす（i3.xlarge → i3.2xlarge）
- または、患者数を減らす（`N_TOTAL_PATIENTS = 5000`）

## 次のステップ

Bronzeデータ生成が完了したら、Silver層に進んでください：

→ `../silver/01_create_silver_tables.sql`
