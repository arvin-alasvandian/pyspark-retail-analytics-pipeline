# Resume Bullets (pick 2–3)

- Built an end‑to‑end **Big Data** analytics pipeline in **PySpark** on a multi‑feature retail dataset: data cleaning, EDA/visualisation, **Spark SQL**, and **MLlib** models (Decision Tree, Logistic Regression, Linear Regression).
- Implemented median imputation (treating zeros as missing), engineered features with `StringIndexer`/`OneHotEncoder`, and evaluated models using Accuracy, F1, AUROC, RMSE, and R²; saved artefacts (plots & metrics) for reproducibility.
- Structured a reproducible repository (scripts, checkpoints, outputs) with clear documentation and task mapping; designed for quick local runs with Python + Java.

---

## Tailored Bullets by Role

### Data Engineer
- Built a reproducible **PySpark** pipeline with Parquet checkpoints (`df1/df2/df3`), **Spark SQL**, and scripted steps (Makefile + Docker) for consistent runs across machines.
- Implemented robust data quality handling at scale (zeros→missing, median imputation) and encoded categoricals with `StringIndexer`/`OneHotEncoder`; assembled features via `VectorAssembler` for MLlib.
- Automated artefact outputs (figures, metrics JSON) to standard paths, supporting CI/CD-style verification and quick reviews.

### Data Analyst
- Performed EDA with summary statistics, quartiles, histogram and boxplot to characterise spend and purchase behaviour; explored linear association (Pearson correlation) between spending and purchase amount.
- Wrote a focused **Spark SQL** query for cohort slicing (`Age < 50` and `SpendingScore > 100`) and exported results for reporting.
- Communicated results with concise visuals and a short methodology/results write‑up suitable for stakeholders.

### Data Scientist / ML Engineer (optional)
- Trained and evaluated MLlib **Decision Tree** and **Logistic Regression** classifiers (Accuracy, F1, AUROC) and a **Linear Regression** model (RMSE, R²), with saved metrics for comparison.
- Used proper feature engineering and clear train/test splits; ensured numeric casting and invalid‑category handling for reliable pipelines.
- Kept the repo lightweight and reproducible so experiments can be rerun or extended quickly.
