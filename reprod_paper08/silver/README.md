# Silver Layer: RA患者定義適用

## 概要

Silver層では、Bronze層データにRA患者定義を適用し、分析用のクリーンなデータを作成します。

## ファイル構成

1. **01_create_silver_tables.sql** - Silverテーブル定義
2. **02_transform_ra_patients.sql** - RA患者定義適用（SQL）

## 実行順序

```bash
# 1. テーブル定義
01_create_silver_tables.sql

# 2. RA患者定義適用（1-2分）
02_transform_ra_patients.sql
```

## 生成されるデータ

| テーブル | レコード数 | 内容 |
|---------|-----------|------|
| silver_ra_patients_def3 | ~650 | RA患者マスタ（薬剤・手術フラグ付き） |
| silver_ra_definitions_summary | 4 | 定義別患者数サマリー |

## RA患者定義

### 7つの定義（論文）

| 定義 | 条件 | 期待患者数 | 有病率 |
|------|------|-----------|--------|
| Definition 0 | ICD-10のみ | ~800-900 | ~0.8-0.9% |
| Definition 1 | ICD-10 + (DMARDs ≥1mo OR CS ≥2mo) | ~750-850 | ~0.75-0.85% |
| Definition 2 | ICD-10 + DMARDs ≥1mo | ~680-720 | ~0.68-0.72% |
| **Definition 3** | **ICD-10 + DMARDs ≥2mo** | **~640-670** | **~0.64-0.67%** |
| Definition 4 | ICD-10 + DMARDs ≥6mo | ~450-500 | ~0.45-0.50% |

**本分析ではDefinition 3を採用**（感度と特異度のバランスが良い）

### Definition 3の詳細

**条件**:
1. RA関連ICD-10コードを持つ（M05.x, M06.x, M08.x）
2. DMARDs処方が2ヶ月以上

**重要なSQL**:
```sql
-- 処方月数の計算（最重要）
CREATE OR REPLACE TEMP VIEW dmard_prescription_months AS
SELECT
    iy.共通キー,
    COUNT(DISTINCT re.診療年月) AS prescription_months
FROM bronze_iy_medication iy
INNER JOIN bronze_re_receipt re ON iy.検索番号 = re.検索番号
WHERE iy.医薬品コード IN (/* DMARDs codes */)
GROUP BY iy.共通キー;

-- Definition 3: DMARDs ≥2ヶ月
CREATE OR REPLACE TEMP VIEW ra_patients_def3 AS
SELECT DISTINCT d0.共通キー
FROM ra_patients_def0 d0
INNER JOIN dmard_prescription_months dpm ON d0.共通キー = dpm.共通キー
WHERE dpm.prescription_months >= 2;
```

## 薬剤・手術フラグ

### 薬剤使用フラグ（11種類）

#### csDMARDs（従来型合成DMARD）
- MTX（メトトレキサート）
- SSZ（サラゾスルファピリジン）
- BUC（ブシラミン）
- TAC（タクロリムス）
- IGT（イグラチモド）
- LEF（レフルノミド）

#### bDMARDs（生物学的DMARD）
- TNFI（TNF阻害薬）
- IL6I（IL-6阻害薬）
- ABT（アバタセプト）

#### tsDMARDs（分子標的型合成DMARD）
- JAKi（JAK阻害薬）

#### その他
- CS（コルチコステロイド）

### 手術・検査フラグ（5種類）

- TJR（人工関節全置換術）
- ARTHROPLASTY（関節形成術）
- SYNOVECTOMY（滑膜切除術）
- ULTRASOUND（関節超音波検査）
- BMD（骨密度測定）

## データ品質チェック

```sql
-- RA患者数
SELECT COUNT(*) FROM silver_ra_patients_def3;  -- ~650

-- 有病率
SELECT
    COUNT(*) AS ra_patients,
    (SELECT COUNT(*) FROM bronze_patients) AS total_patients,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze_patients), 2) AS prevalence_pct
FROM silver_ra_patients_def3;  -- ~0.65%

-- 女性比率
SELECT
    ROUND(SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS female_pct
FROM silver_ra_patients_def3;  -- ~76%

-- 65歳以上の割合
SELECT
    ROUND(SUM(CASE WHEN age >= 65 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS elderly_pct
FROM silver_ra_patients_def3;  -- ~55-60%

-- 薬剤使用率
SELECT
    ROUND(AVG(MTX) * 100, 1) AS MTX_pct,
    ROUND(AVG(SSZ) * 100, 1) AS SSZ_pct,
    ROUND(AVG(bDMARDs) * 100, 1) AS bDMARDs_pct,
    ROUND(AVG(CS) * 100, 1) AS CS_pct
FROM silver_ra_patients_def3;
-- MTX: ~63%, SSZ: ~25%, bDMARDs: ~23%, CS: ~45%

-- 定義別患者数
SELECT * FROM silver_ra_definitions_summary ORDER BY definition;
```

## DuckDB SQL → Spark SQL 変換

### 互換性

ほとんどのSQLは**変更なし**で動作します：

| 機能 | DuckDB | Spark SQL | 互換性 |
|------|--------|-----------|--------|
| COUNT DISTINCT | ✓ | ✓ | ✓ 互換 |
| CASE WHEN | ✓ | ✓ | ✓ 互換 |
| MAX, AVG | ✓ | ✓ | ✓ 互換 |
| GROUP BY | ✓ | ✓ | ✓ 互換 |
| INNER JOIN | ✓ | ✓ | ✓ 互換 |
| COALESCE | ✓ | ✓ | ✓ 互換 |
| NULLIF | ✓ | ✓ | ✓ 互換 |

### 主な変更点

1. **View作成**:
   - DuckDB: `CREATE OR REPLACE VIEW`
   - Spark SQL: `CREATE OR REPLACE TEMP VIEW` ← セッションスコープ

2. **Percentile関数**:
   - DuckDB: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)`
   - Spark SQL: `PERCENTILE_APPROX(col, 0.5)` ← 近似値

## よくある問題

### 問題: RA患者が0人

**原因**: Bronzeデータが正しく生成されていない

**解決策**:
```sql
-- ICD-10コードを持つ患者を確認
SELECT COUNT(DISTINCT 共通キー)
FROM bronze_sy_disease
WHERE ICD10コード LIKE 'M0%';  -- ~150前後のはず

-- DMARDs処方を確認
SELECT COUNT(*)
FROM bronze_iy_medication
WHERE 医薬品コード LIKE '1199%' OR 医薬品コード LIKE '4400%';
-- ~1,300前後のはず
```

### 問題: Temp Viewが見つからない

**原因**: 別のノートブックで実行している

**解決策**: `02_transform_ra_patients.sql` を**一つのノートブック内で全て実行**

## 次のステップ

Silverデータ作成が完了したら、Gold層に進んでください：

→ `../gold/01_create_gold_tables.sql`
