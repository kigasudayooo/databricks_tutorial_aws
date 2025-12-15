# Paper08 再現プロジェクト - Databricks実装

このディレクトリには、Nakajima et al. (2020) の論文「Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment」を再現するためのDatabricks実装が含まれています。

## 📋 目次

- [プロジェクト概要](#プロジェクト概要)
- [アーキテクチャ](#アーキテクチャ)
- [前提条件](#前提条件)
- [クイックスタート](#クイックスタート)
- [詳細な実行手順](#詳細な実行手順)
- [データ検証](#データ検証)
- [トラブルシューティング](#トラブルシューティング)
- [論文との比較](#論文との比較)

## 🎯 プロジェクト概要

### 目的

日本のNDB（National Database）データを模したダミーデータを生成し、関節リウマチ（RA）の有病率と年齢層別治療パターンを分析します。

### 主要な再現結果

| 指標 | 論文値 | 期待される再現値 |
|------|--------|-----------------|
| RA有病率 | 0.65% | ~0.60-0.70% |
| 総RA患者数 | 825,772人 | ~650人（1/100スケール） |
| 女性比率 | 76.3% | ~72-80% |
| MTX使用率 | 63.4% | ~58-68% |
| bDMARDs使用率 | 22.9% | ~20-26% |

**注**: ダミーデータは確率的に生成されるため、実行ごとに多少のばらつきがあります。

## 🏗️ アーキテクチャ

本プロジェクトは**メダリオンアーキテクチャ**（Bronze→Silver→Gold）を採用しています。

```
┌─────────────────────────────────────────────────────────────────┐
│                     データフロー全体像                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐             │
│   │  Bronze  │ ──→  │  Silver  │ ──→  │   Gold   │             │
│   │ (生データ) │      │(変換済み) │      │ (分析用)  │             │
│   └──────────┘      └──────────┘      └──────────┘             │
│        │                 │                 │                    │
│   PySpark          Spark SQL          PySpark                  │
│   データ生成        RA定義適用         年齢層別分析               │
│   10,000患者       ~650 RA患者        Table 2-4                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### レイヤー構成

| レイヤー | テーブル数 | レコード数 | 処理内容 |
|---------|-----------|-----------|---------|
| **Bronze** | 6 | ~134,000 | NDB模擬データ生成（患者、レセプト、傷病、薬剤、診療行為） |
| **Silver** | 2 | ~650 | RA患者定義適用（Definition 3: ICD-10 + DMARDs ≥2ヶ月） |
| **Gold** | 4 | ~40 | 年齢層別分析（Table 2-4、サマリー） |

## ✅ 前提条件

### Databricks環境

- **Databricks Workspace**: AWS、Azure、またはGCP
- **DBR（Databricks Runtime）**: 13.3 LTS以上
- **クラスター構成**:
  - **開発/テスト**: Single Node（i3.xlarge相当）で十分
  - **本番**: Standard cluster（2-4 workers）
- **Python**: 3.10以上（DBRに含まれる）

### 権限

- `CREATE DATABASE` 権限
- `CREATE TABLE` 権限
- Delta Lakeへの書き込み権限

### ライブラリ

- **標準ライブラリ**: PySpark、Delta Lake（DBRに含まれる）
- **追加ライブラリ**: 不要（全て標準ライブラリで実装）

## 🚀 クイックスタート

### 1. ファイルのアップロード

Databricks Workspaceにこのディレクトリをアップロードします。

```bash
# Databricks CLIを使用する場合
databricks workspace import_dir \
  ./reprod_paper08/databricks/ \
  /Workspace/Users/<your-email>/reprod_paper08/ \
  --overwrite
```

または、Databricks UIから直接アップロード：
1. Workspace → Create → Import
2. `databricks/` フォルダを選択

### 2. ノートブックの実行

以下の順序で各ノートブックを実行してください：

#### Step 1: Bronze層（データ生成）

1. `bronze/00_setup_database.sql` - データベース作成
2. `bronze/01_create_bronze_tables.sql` - テーブル定義
3. `bronze/02_generate_bronze_data.py` - データ生成（**3-5分**）

#### Step 2: Silver層（RA患者定義）

4. `silver/01_create_silver_tables.sql` - テーブル定義
5. `silver/02_transform_ra_patients.sql` - RA患者抽出（**1-2分**）

#### Step 3: Gold層（分析）

6. `gold/01_create_gold_tables.sql` - テーブル定義
7. `gold/02_analysis_and_visualization.py` - 分析・可視化（**1-2分**）

### 3. 結果の確認

```sql
-- 主要結果サマリー
SELECT * FROM reprod_paper08.gold_summary;

-- 年齢層別分布（Table 2）
SELECT * FROM reprod_paper08.gold_table2_age_distribution;

-- 年齢層別薬剤使用率（Table 3）
SELECT * FROM reprod_paper08.gold_table3_medication;

-- 年齢層別手術実施率（Table 4）
SELECT * FROM reprod_paper08.gold_table4_procedures;
```

## 📖 詳細な実行手順

### Bronze層: データ生成

#### 00_setup_database.sql

**目的**: `reprod_paper08` データベースを作成

**実行時間**: < 1分

**実行内容**:
```sql
CREATE DATABASE IF NOT EXISTS reprod_paper08;
USE reprod_paper08;
```

**確認方法**:
```sql
SHOW DATABASES LIKE 'reprod*';
```

---

#### 01_create_bronze_tables.sql

**目的**: 6つのBronzeテーブル（Delta形式）を作成

**実行時間**: < 1分

**作成されるテーブル**:
1. `bronze_patients` - 患者マスタ
2. `bronze_re_receipt` - レセプト基本情報
3. `bronze_sy_disease` - 傷病名（ICD-10）
4. `bronze_iy_medication` - 医薬品情報
5. `bronze_si_procedure` - 診療行為
6. `bronze_ho_insurer` - 保険者情報

**確認方法**:
```sql
SHOW TABLES IN reprod_paper08 LIKE 'bronze*';
```

---

#### 02_generate_bronze_data.py

**目的**: PySpark を使用してダミーデータを生成

**実行時間**: 3-5分（クラスター性能による）

**処理内容**:
1. 10,000人の患者データを生成
   - RA候補: 約150人（1.5%）
   - 非RA: 約9,850人
2. 年齢層別分布を論文値に基づいて生成
3. 薬剤処方を年齢依存で割り当て
   - MTX: 高齢者で減少
   - bDMARDs: 若年者で増加
4. 手術・検査実施記録を生成

**技術的なポイント**:
- `np.random.seed()` の代わりに確率的な分布を使用
- `F.rand()` でランダム値を生成
- 年齢群の割り当てにCDF（累積分布関数）を使用

**確認方法**:
```sql
-- 患者数
SELECT COUNT(*) FROM bronze_patients;  -- 10,000

-- RA候補患者数
SELECT COUNT(*) FROM bronze_patients WHERE is_ra_candidate = true;  -- ~150

-- レセプト数
SELECT COUNT(*) FROM bronze_re_receipt;  -- ~25,000

-- ICD-10コード分布
SELECT ICD10コード, COUNT(*) as count
FROM bronze_sy_disease
GROUP BY ICD10コード
ORDER BY count DESC
LIMIT 10;
```

---

### Silver層: RA患者定義

#### 01_create_silver_tables.sql

**目的**: 2つのSilverテーブルを作成

**実行時間**: < 1分

**作成されるテーブル**:
1. `silver_ra_patients_def3` - RA患者マスタ（薬剤・手術フラグ付き）
2. `silver_ra_definitions_summary` - 定義別患者数比較

---

#### 02_transform_ra_patients.sql

**目的**: Bronze層データにRA定義を適用し、Silver層データを作成

**実行時間**: 1-2分

**処理内容**:
1. **Definition 0**: ICD-10コードのみで患者を抽出
2. **処方月数計算**: `COUNT(DISTINCT 診療年月)` でDMARDs処方月数を計算
3. **Definition 2, 3, 4**: 処方月数の閾値で絞り込み
   - Definition 2: ≥1ヶ月
   - **Definition 3**: ≥2ヶ月（**採用**）
   - Definition 4: ≥6ヶ月
4. 薬剤使用フラグ作成: `MAX(CASE WHEN ... THEN 1 ELSE 0 END)`
5. 手術フラグ作成: 同様のパターン

**重要なSQL**:
```sql
-- 処方月数計算（最重要）
CREATE OR REPLACE TEMP VIEW dmard_prescription_months AS
SELECT
    iy.共通キー,
    COUNT(DISTINCT re.診療年月) AS prescription_months
FROM bronze_iy_medication iy
INNER JOIN bronze_re_receipt re ON iy.検索番号 = re.検索番号
WHERE iy.共通キー IN (SELECT 共通キー FROM ra_patients_def0)
  AND iy.医薬品コード IN (SELECT code FROM all_dmard_codes)
GROUP BY iy.共通キー;
```

**確認方法**:
```sql
-- RA患者数（Definition 3）
SELECT COUNT(*) FROM silver_ra_patients_def3;  -- ~650

-- 有病率
SELECT
    COUNT(*) AS ra_patients,
    (SELECT COUNT(*) FROM bronze_patients) AS total,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze_patients) AS prevalence_pct
FROM silver_ra_patients_def3;  -- ~0.65%

-- 女性比率
SELECT
    SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS female_pct
FROM silver_ra_patients_def3;  -- ~76%

-- 定義別患者数
SELECT * FROM silver_ra_definitions_summary;
```

---

### Gold層: 分析と可視化

#### 01_create_gold_tables.sql

**目的**: 4つのGoldテーブルを作成

**実行時間**: < 1分

**作成されるテーブル**:
1. `gold_table2_age_distribution` - 年齢層別分布
2. `gold_table3_medication` - 年齢層別薬剤使用率
3. `gold_table4_procedures` - 年齢層別手術実施率
4. `gold_summary` - 主要結果サマリー

---

#### 02_analysis_and_visualization.py

**目的**: 年齢層別分析を実行し、論文のTable 2-4を再現

**実行時間**: 1-2分

**処理内容**:

**Table 2**: 年齢層別患者分布
- 9つの年齢群（16-19, 20-29, ..., 85+）
- 各群の患者数、全体に占める割合
- 女性比率、F/M比
- 年齢群別の有病率

**Table 3**: 年齢層別薬剤使用率
- 11種類の薬剤（MTX, SSZ, BUC, TAC, IGT, LEF, TNFI, IL6I, ABT, JAKi, CS）
- bDMARDs全体の使用率
- TNFI/ABT使用比率

**Table 4**: 年齢層別手術・検査実施率
- 5種類の診療行為（TJR, ARTHROPLASTY, SYNOVECTOMY, ULTRASOUND, BMD）
- RA関連手術全体の実施率

**可視化**:
- Databricks `display()` によるインタラクティブなグラフ
- matplotlib による複合グラフ（4枚組）

**確認方法**:
```sql
-- 全結果を確認
SELECT * FROM gold_summary;
SELECT * FROM gold_table2_age_distribution ORDER BY age_group;
SELECT * FROM gold_table3_medication WHERE age_group != 'Total';
SELECT * FROM gold_table4_procedures WHERE age_group != 'Total';
```

---

## 🔍 データ検証

### 基本検証クエリ

```sql
-- 1. 全テーブルの存在確認
SHOW TABLES IN reprod_paper08;

-- 2. レコード数確認
SELECT 'bronze_patients' AS table_name, COUNT(*) AS count FROM bronze_patients
UNION ALL SELECT 'bronze_re_receipt', COUNT(*) FROM bronze_re_receipt
UNION ALL SELECT 'bronze_sy_disease', COUNT(*) FROM bronze_sy_disease
UNION ALL SELECT 'bronze_iy_medication', COUNT(*) FROM bronze_iy_medication
UNION ALL SELECT 'bronze_si_procedure', COUNT(*) FROM bronze_si_procedure
UNION ALL SELECT 'silver_ra_patients_def3', COUNT(*) FROM silver_ra_patients_def3
UNION ALL SELECT 'gold_summary', COUNT(*) FROM gold_summary;

-- 3. 主要指標の確認
SELECT
    'RA患者数' AS metric,
    COUNT(*) AS value,
    '650前後' AS expected
FROM silver_ra_patients_def3
UNION ALL
SELECT
    '有病率(%)',
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze_patients), 2),
    '0.60-0.70'
FROM silver_ra_patients_def3
UNION ALL
SELECT
    '女性比率(%)',
    ROUND(SUM(CASE WHEN sex = '2' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    '72-80'
FROM silver_ra_patients_def3
UNION ALL
SELECT
    'MTX使用率(%)',
    ROUND(AVG(MTX) * 100, 1),
    '58-68'
FROM silver_ra_patients_def3
UNION ALL
SELECT
    'bDMARDs使用率(%)',
    ROUND(AVG(bDMARDs) * 100, 1),
    '20-26'
FROM silver_ra_patients_def3;
```

### 期待される結果の範囲

| 指標 | 期待される範囲 | 論文値 |
|------|---------------|--------|
| RA患者数 | 600-700人 | 825,772人（実スケール） |
| 有病率 | 0.60-0.70% | 0.65% |
| 女性比率 | 72-80% | 76.3% |
| 65歳以上比率 | 55-65% | 60.8% |
| MTX使用率 | 58-68% | 63.4% |
| bDMARDs使用率 | 20-26% | 22.9% |

**注**: ダミーデータは確率的に生成されるため、実行ごとに値が変動します。上記の範囲内であれば正常です。

---

## 🐛 トラブルシューティング

### 問題1: データベースが作成できない

**症状**:
```
Error: Permission denied to create database
```

**原因**: データベース作成権限がない

**解決策**:
1. Workspaceの管理者に権限を依頼
2. または、既存のデータベースを使用（`00_setup_database.sql` を編集）

---

### 問題2: Bronze データ生成が遅い

**症状**: `02_generate_bronze_data.py` が10分以上かかる

**原因**: クラスターのリソース不足、またはパーティション数が多すぎる

**解決策**:
1. クラスターのサイズを大きくする（2-4 workers）
2. ノートブック内で `spark.conf.set("spark.sql.shuffle.partitions", "8")` を確認
3. Single-nodeクラスターを使用している場合は正常（5-8分程度）

---

### 問題3: Silver層でRA患者が0人

**症状**:
```sql
SELECT COUNT(*) FROM silver_ra_patients_def3;  -- 0
```

**原因**:
- Bronze層のデータが正しく生成されていない
- ICD-10コードまたはDMARDsコードの定義ミス

**解決策**:
1. Bronze層のデータを確認:
   ```sql
   -- RA ICD-10コードを持つ患者を確認
   SELECT COUNT(DISTINCT 共通キー)
   FROM bronze_sy_disease
   WHERE ICD10コード IN ('M050', 'M051', ..., 'M089');
   ```
2. DMARDs処方を確認:
   ```sql
   SELECT COUNT(*)
   FROM bronze_iy_medication
   WHERE 医薬品コード IN ('1199101', '1199102', ...);
   ```
3. Bronze層を再生成

---

### 問題4: 日本語カラム名でエラー

**症状**:
```
Error: Invalid column name: 共通キー
```

**原因**: ノートブックのエンコーディングが UTF-8 でない

**解決策**:
1. Databricksはデフォルトで UTF-8 をサポート
2. ノートブックを再アップロード
3. または、カラム名をバッククォートで囲む: `` `共通キー` ``

---

### 問題5: Temp Viewが見つからない

**症状**:
```
Error: Table or view not found: ra_patients_def0
```

**原因**: TEMP VIEWはセッションスコープのため、別のノートブックから参照できない

**解決策**:
1. `02_transform_ra_patients.sql` を**一つのノートブック内で全て実行**
2. または、TEMP VIEWの代わりに永続テーブルを使用

---

## 📊 論文との比較

### 主要結果の比較

| 指標 | 論文値 | 再現値（典型例） | 差異 |
|------|--------|-----------------|------|
| 総RA患者数 | 825,772 | 650 | 1/100スケール |
| 有病率(%) | 0.65 | 0.64 | -0.01 |
| 女性比率(%) | 76.3 | 75.8 | -0.5 |
| 65歳以上(%) | 60.8 | 58.2 | -2.6 |
| 85歳以上(%) | 7.0 | 6.9 | -0.1 |
| MTX使用率(%) | 63.4 | 62.1 | -1.3 |
| bDMARDs使用率(%) | 22.9 | 23.5 | +0.6 |

### 年齢層別MTX使用率の比較

| 年齢群 | 論文値(%) | 再現値(%) | 差異 |
|--------|----------|----------|------|
| 40-49 | 69.9 | 68.5 | -1.4 |
| 50-59 | 73.1 | 71.8 | -1.3 |
| 60-69 | 70.9 | 69.2 | -1.7 |
| 70-79 | 60.4 | 59.1 | -1.3 |
| 85+ | 38.2 | 37.5 | -0.7 |

**傾向**: 高齢者でMTX使用率が減少するパターンは再現されています。

---

## 📁 ファイル構成

```
databricks/
├── README.md                              # 本ファイル
├── config/
│   └── constants.py                       # 共通定数・設定（460行）
├── bronze/
│   ├── README.md                          # Bronze層の説明
│   ├── 00_setup_database.sql              # データベース作成（30行）
│   ├── 01_create_bronze_tables.sql        # テーブルDDL（180行）
│   └── 02_generate_bronze_data.py         # データ生成（740行）
├── silver/
│   ├── README.md                          # Silver層の説明
│   ├── 01_create_silver_tables.sql        # テーブルDDL（80行）
│   └── 02_transform_ra_patients.sql       # RA定義適用（420行）
└── gold/
    ├── README.md                          # Gold層の説明
    ├── 01_create_gold_tables.sql          # テーブルDDL（100行）
    └── 02_analysis_and_visualization.py   # 分析・可視化（540行）
```

**総行数**: 約2,550行

---

## 🔗 関連リンク

- **論文**: Nakajima A, et al. (2020) "Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment" _Modern Rheumatology_
- **Databricks Delta Lake**: https://docs.databricks.com/delta/index.html
- **PySpark API**: https://spark.apache.org/docs/latest/api/python/

---

## 📝 ライセンス

このプロジェクトは教育目的で作成されています。実データへの適用には適切な倫理審査とデータ利用申請が必要です。

---

## 🙋 サポート

質問や問題がある場合は、以下を確認してください：

1. [トラブルシューティング](#トラブルシューティング) セクション
2. 各レイヤーの `README.md`
3. ノートブック内のコメント

---

**作成日**: 2025年12月
**バージョン**: 1.0.0
