# Paper08 再現プロジェクト

## 概要

このプロジェクトは、論文「Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment」(Nakajima et al., 2020) のデータフローを再現するものです。

## 論文の概要

- **目的**: 日本における関節リウマチ（RA）の有病率を推定し、年齢層別の治療動向と臨床特性を明らかにする
- **データソース**: NDB Japan (2017年4月〜2018年3月)
- **主要結果**: RA有病率 0.65%、825,772人のRA患者を同定

## クイックスタート

```bash
# データ生成（一括実行）
cd reprod_paper08
poetry run python scripts/generate_data.py
```

## メダリオンアーキテクチャ

```
data/
├── bronze/     # 生データ（ダミー）- レセプト・特定健診データ
│   ├── patients.parquet       # 患者マスタ (10,000人)
│   ├── re_receipt.parquet     # レセプト基本情報
│   ├── sy_disease.parquet     # 傷病名（ICD-10コード）
│   ├── iy_medication.parquet  # 医薬品情報
│   └── si_procedure.parquet   # 診療行為情報
├── silver/     # 変換データ - RA患者定義適用後
│   ├── ra_patients_def3.parquet        # RA患者マスタ
│   └── ra_definitions_summary.parquet  # 各定義の比較
└── gold/       # 分析用データ - 集計結果
    ├── table2_age_distribution.csv  # 年齢層別患者分布
    ├── table3_medication.csv        # 年齢層別薬剤使用率
    ├── table4_procedures.csv        # 年齢層別手術実施率
    └── summary.csv                  # 主要指標サマリー
```

## ノートブック構成

| ノートブック | 役割 | 入力 | 出力 |
|-------------|------|------|------|
| `01_bronze_data_generation.ipynb` | ダミーデータ生成 | - | Bronze層 |
| `02_silver_ra_definition.ipynb` | RA患者定義適用 | Bronze層 | Silver層 |
| `03_gold_analysis.ipynb` | 分析・可視化 | Silver層 | Gold層 |

## 研究で使用するRA定義

論文では7つのRA定義を検討し、**Definition 3**を採用:

| 定義 | 条件 | 有病率 |
|------|------|--------|
| Definition 0 | ICD-10コードのみ | 0.97% |
| Definition 2 | ICD-10 + DMARDs 1ヶ月以上 | 0.80% |
| **Definition 3** | **ICD-10 + DMARDs 2ヶ月以上** | **0.65%** |
| Definition 4 | ICD-10 + DMARDs 6ヶ月以上 | 0.54% |

### ICD-10コード
- M05.x: 血清反応陽性関節リウマチ
- M06.x (M061, M064除く): その他の関節リウマチ
- M08.x (M081, M082除く): 若年性関節炎

## 主要な評価指標

| 指標 | 計算方法 |
|------|---------|
| 有病率 | RA患者数 / 総人口 × 100 |
| 女性比率 | 女性RA患者数 / 総RA患者数 × 100 |
| F/M比 | 女性患者数 / 男性患者数 |
| 薬剤使用率 | 当該薬剤使用患者数 / 総RA患者数 × 100 |

## ドキュメント

詳細な解説は以下のドキュメントを参照してください：

- [データフロー解説](docs/データフロー解説.md) - 研究の概要、メダリオンアーキテクチャ、RA定義の詳細
- [ノートブック解説](docs/ノートブック解説.md) - 各ノートブックの実装詳細とベストプラクティス

## 注意事項

- 本プロジェクトのデータは**完全なダミーデータ**です
- 論文の統計値を参考に分布を設定していますが、実データではありません
- 研究フローの理解とノートブック開発の練習を目的としています

## 参考文献

1. Nakajima A, et al. (2020) Prevalence of patients with rheumatoid arthritis and age-stratified trends in clinical characteristics and treatment. Modern Rheumatology.
