# Gold Layer: 分析と可視化

## 概要

Gold層では、Silver層データを用いて論文のTable 2-4を再現し、年齢層別の分析結果を可視化します。

## ファイル構成

1. **01_create_gold_tables.sql** - Goldテーブル定義
2. **02_analysis_and_visualization.py** - 分析・可視化（PySpark）

## 実行順序

```bash
# 1. テーブル定義
01_create_gold_tables.sql

# 2. 分析実行（1-2分）
02_analysis_and_visualization.py
```

## 生成されるデータ

| テーブル | レコード数 | 内容 |
|---------|-----------|------|
| gold_table2_age_distribution | 10 | 年齢層別分布（9年齢群+合計） |
| gold_table3_medication | 10 | 年齢層別薬剤使用率 |
| gold_table4_procedures | 10 | 年齢層別手術実施率 |
| gold_summary | 9 | 主要結果サマリー |

## 分析内容

### Table 2: 年齢層別患者分布

**再現する項目**:
- 各年齢群のRA患者数
- 全RA患者に占める割合（%）
- 女性の割合（%）
- 女性/男性比（F/M Ratio）
- 年齢群別の有病率（%）

**年齢群**:
```
16-19, 20-29, 30-39, 40-49, 50-59, 60-69, 70-79, 80-84, 85+
```

**PySpark実装**:
```python
df_table2 = df_ra.groupBy("age_group").agg(
    F.count("*").alias("n"),
    F.sum(F.when(F.col("sex") == "2", 1).otherwise(0)).alias("n_female"),
    F.sum(F.when(F.col("sex") == "1", 1).otherwise(0)).alias("n_male")
).withColumn(
    "fm_ratio", F.col("n_female") / F.nullif(F.col("n_male"), 0)
).withColumn(
    "prevalence", F.col("n") / F.col("n_all") * 100
)
```

**期待される結果**:
- 70-79歳群で患者数が最大（~28%）
- 女性比率: 全体で約76%
- 有病率: 70-79歳でピーク（~1.5-1.7%）

---

### Table 3: 年齢層別薬剤使用率

**再現する項目**:
- csDMARDs使用率（MTX, SSZ, BUC, TAC, IGT, LEF）
- bDMARDs使用率（TNFI, IL6I, ABT）
- tsDMARDs使用率（JAKi）
- コルチコステロイド使用率（CS）
- bDMARDs全体の使用率
- TNFI/ABT使用比率

**PySpark実装**:
```python
df_table3 = df_ra.groupBy("age_group").agg(
    F.round(F.avg("MTX") * 100, 1).alias("MTX"),
    F.round(F.avg("SSZ") * 100, 1).alias("SSZ"),
    F.round(F.avg("bDMARDs") * 100, 1).alias("bDMARDs"),
    # ...
).withColumn(
    "TNFI_ABT_ratio",
    F.round(F.col("TNFI") / F.nullif(F.col("ABT"), 0), 1)
)
```

**期待される傾向**:
- **MTX**: 50-59歳でピーク、高齢者で減少
  - 50-59歳: ~73%
  - 85歳以上: ~38%
- **bDMARDs**: 若年者で高い
  - 16-19歳: ~50%
  - 85歳以上: ~14%
- **TNFI/ABT比**: 年齢とともに減少
  - 若年層: 高比率（TNFI優勢）
  - 高齢層: 低比率（ABT増加）

---

### Table 4: 年齢層別手術・検査実施率

**再現する項目**:
- TJR（人工関節全置換術）実施率
- ARTHROPLASTY（関節形成術）実施率
- SYNOVECTOMY（滑膜切除術）実施率
- ULTRASOUND（関節超音波検査）実施率
- BMD（骨密度測定）実施率
- RA関連手術全体の実施率

**PySpark実装**:
```python
df_table4 = df_ra.groupBy("age_group").agg(
    F.round(F.avg("TJR") * 100, 2).alias("TJR"),
    F.round(F.avg("ARTHROPLASTY") * 100, 2).alias("ARTHROPLASTY"),
    F.round(F.avg("BMD") * 100, 2).alias("BMD"),
    # ...
)
```

**期待される結果**:
- TJR: ~0.9-1.0%
- ULTRASOUND: ~18%
- BMD: 高齢者で増加（年齢依存）

---

### Summary: 主要結果サマリー

**再現する項目**:
- 総RA患者数
- 有病率
- 女性比率
- 65歳以上の割合
- 85歳以上の割合
- MTX使用率
- bDMARDs使用率
- ステロイド使用率
- RA手術実施率

**期待される結果**:

| 指標 | 論文値 | 再現値（目安） |
|------|--------|---------------|
| RA患者数 | 825,772 | ~650 |
| 有病率(%) | 0.65 | 0.60-0.70 |
| 女性比率(%) | 76.3 | 72-80 |
| 65歳以上(%) | 60.8 | 55-65 |
| MTX使用率(%) | 63.4 | 58-68 |
| bDMARDs使用率(%) | 22.9 | 20-26 |

---

## 可視化

### 1. Databricks displayによる可視化

```python
# インタラクティブなグラフ
display(df_table2.select("age_group", "n", "prevalence"))

# Databricks UIでグラフタイプを選択:
# - Bar Chart: 年齢層別患者数
# - Line Chart: 有病率の推移
# - Scatter: F/M比の変化
```

### 2. matplotlibによるカスタムグラフ

```python
import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# 1. 年齢層別患者数
axes[0, 0].bar(age_groups, patient_counts)

# 2. MTX使用率（再現 vs 論文）
axes[0, 1].bar(reproduced_mtx, paper_mtx)

# 3. bDMARDs使用率
axes[1, 0].bar(reproduced_bdmard, paper_bdmard)

# 4. TNFI/ABT比率
axes[1, 1].plot(age_groups, tnfi_abt_ratio)
```

---

## データ品質チェック

```sql
-- 1. 全Goldテーブルの存在確認
SHOW TABLES IN reprod_paper08 LIKE 'gold*';

-- 2. Table 2の確認
SELECT * FROM gold_table2_age_distribution
ORDER BY
    CASE age_group
        WHEN '16-19' THEN 1 WHEN '20-29' THEN 2 WHEN '30-39' THEN 3
        WHEN '40-49' THEN 4 WHEN '50-59' THEN 5 WHEN '60-69' THEN 6
        WHEN '70-79' THEN 7 WHEN '80-84' THEN 8 WHEN '85+' THEN 9
        WHEN 'Total' THEN 10
    END;

-- 3. Table 3の確認（MTX使用率の年齢変化）
SELECT age_group, n, MTX, bDMARDs
FROM gold_table3_medication
WHERE age_group != 'Total'
ORDER BY age_group;

-- 4. サマリーの確認
SELECT * FROM gold_summary;

-- 5. 論文値との比較
SELECT
    metric,
    reproduced,
    paper,
    CAST(REPLACE(reproduced, ',', '') AS DOUBLE) AS reprod_val,
    CAST(REPLACE(paper, ',', '') AS DOUBLE) AS paper_val,
    ROUND(
        (CAST(REPLACE(reproduced, ',', '') AS DOUBLE) -
         CAST(REPLACE(paper, ',', '') AS DOUBLE)) /
        CAST(REPLACE(paper, ',', '') AS DOUBLE) * 100, 1
    ) AS diff_pct
FROM gold_summary
WHERE metric LIKE '%(%'  -- パーセント指標のみ
;
```

---

## よくある問題

### 問題: Table 2で年齢群の順序がおかしい

**原因**: 文字列ソートになっている

**解決策**: CASE文で順序を指定
```python
df_table2 = df_table2.orderBy(
    F.when(F.col("age_group") == "16-19", 1)
     .when(F.col("age_group") == "20-29", 2)
     # ...
     .otherwise(10)
)
```

### 問題: グラフが表示されない

**原因**: matplotlib の設定

**解決策**:
```python
# Databricks display を使用
display(fig)

# または
plt.show()  # ただしDatabricksでは display(fig) を推奨
```

### 問題: 論文値と大きく異なる

**症状**: 有病率が0.3%や1.2%など、0.65%から大きく外れる

**原因**: Bronzeデータの生成に問題がある

**解決策**:
1. Bronzeデータを再生成
2. `RA_CANDIDATE_RATIO` を調整（`config/constants.py`）
3. Silverの定義別患者数を確認

---

## 論文との比較ポイント

### 再現できている傾向

✅ **有病率の年齢変化**:
- 70-79歳でピーク
- 若年層と超高齢層で低い

✅ **MTX使用率の年齢変化**:
- 50-59歳でピーク
- 高齢者で減少（腎機能考慮）

✅ **bDMARDs使用率の年齢変化**:
- 若年層で高い
- 高齢層で減少

✅ **TNFI/ABT比の変化**:
- 若年層でTNFI優勢
- 高齢層でABT増加（安全性考慮）

### 確率的な変動

⚠️ 以下は実行ごとにばらつきます（±5-10%程度）:
- 個別の薬剤使用率
- 手術実施率
- 年齢群別の患者数

これはダミーデータが確率的に生成されるためです。

---

## 次のステップ

Gold層の分析が完了したら：

1. **結果の確認**: `SELECT * FROM gold_summary`
2. **可視化の確認**: ノートブック内のグラフ
3. **論文との比較**: 主要指標が範囲内にあるか確認
4. **レポート作成**: 必要に応じて結果をエクスポート

---

## 論文の主要な発見（再現確認）

1. ✅ RA有病率は0.65%
2. ✅ 女性が76.3%を占める
3. ✅ 65歳以上が60.8%
4. ✅ 70-79歳群で最高の有病率1.63%
5. ✅ MTX使用率は年齢とともに減少
6. ✅ bDMARDs使用率は若年で高い
7. ✅ TNFI/ABT比は年齢とともに減少
