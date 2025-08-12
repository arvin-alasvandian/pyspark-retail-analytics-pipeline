# PySpark Retail Analytics Pipeline

![CI](https://github.com/arvin-alasvandian/pyspark-retail-analytics-pipeline/actions/workflows/pyspark-ci.yml/badge.svg)
![Lint](https://github.com/arvin-alasvandian/pyspark-retail-analytics-pipeline/actions/workflows/lint.yml/badge.svg)

An end-to-end **PySpark** pipeline on a retail purchases dataset: data cleaning and imputation, EDA with Spark + plots, a focused Spark SQL query, and three MLlib models (Decision Tree, Logistic Regression, Linear Regression). The repo is reproducible and CI-ready (GitHub Actions builds figures + metrics on every push).

---

## Tech stack
- **PySpark** (DataFrame API, SQL, MLlib)
- **Matplotlib** (small plots after `.toPandas()` sampling)
- **GitHub Actions** (CI pipelines + artifacts)
- **Docker** (optional local run)
- **pre-commit** (Black, isort, Flake8) — optional

## Dataset (from coursework brief)
`customer_purchases.csv` with columns:
- `CustomerID`, `Age`, `Gender`, `AnnualIncome`, `SpendingScore`,
  `PurchaseCategory`, `TotalPurchases`, `PurchaseAmount`, `Outcome`

> Note: in this project, zeros are treated as **missing** for the columns specified by the brief and handled accordingly.

## Repository structure
```
.
├─ data/
│  └─ customer_purchases.csv
├─ src/
│  ├─ 01_data_prep.py          # df1/df2/df3 creation, median impute, row filtering
│  ├─ 02_eda_sql.py            # stats, plots, correlation, Spark SQL query
│  ├─ 03_models.py             # Decision Tree, Logistic Regression, Linear Regression
│  └─ 04_autofill_report.py    # (optional) auto-fill brief findings from outputs
├─ outputs/                    # metrics.json, SQL CSV (created at run time)
├─ figures/                    # PNG plots (created at run time)
├─ .github/workflows/          # CI: pyspark-ci.yml, lint.yml
├─ requirements.txt            # runtime deps
├─ dev-requirements.txt        # pre-commit deps (optional)
├─ Dockerfile                  # optional container run
├─ Makefile                    # make prep | make eda | make models
├─ LICENSE
└─ README.md
```

## How to run (local)
Prereqs: **Python 3.11+** and **Java 17 (JRE/JDK)**.

```bash
pip install -r requirements.txt

# Step 1–3 from the brief
python src/01_data_prep.py
python src/02_eda_sql.py
python src/03_models.py

# Optional: create a paste-ready findings file from outputs
python src/04_autofill_report.py
```

Outputs after a successful run:
- `figures/` → `purchase_amount_hist.png`, `total_purchases_boxplot.png`, `purchase_vs_spending_scatter.png`
- `outputs/` → `metrics.json` and a CSV exported from the SQL query

## How to run (Docker, optional)
```bash
docker build -t retail-pyspark .
docker run --rm -it -v "%cd%":/app retail-pyspark bash -lc   "python src/01_data_prep.py && python src/02_eda_sql.py && python src/03_models.py"
```

## CI (GitHub Actions)
- On every push, **PySpark CI** installs Java+Python, runs all scripts, and uploads **figures** + **outputs** as artifacts.
- Linting is available via **Lint (pre-commit)**. To match CI locally:
  ```bash
  pip install -r dev-requirements.txt
  pre-commit run --all-files
  ```

## How the brief maps to this repo
1. **Task 1** — Load CSV, print schema/count → `01_data_prep.py` → `data/parquet/df1`
2. **Task 2** — Replace zeros with medians (`SpendingScore`, `TotalPurchases`) → `df2`
3. **Task 3** — Drop rows with zeros in (`Age`, `AnnualIncome`, `PurchaseAmount`) → `df3`
4. **Task 4** — Summary stats + histogram for `PurchaseAmount` → `02_eda_sql.py` → figure + console
5. **Task 5** — Quartiles + boxplot for `TotalPurchases` → `02_eda_sql.py` → figure + console
6. **Task 6** — Scatter + Pearson correlation (`PurchaseAmount` vs `SpendingScore`) → `02_eda_sql.py`
7. **Task 7** — Spark SQL (`Age < 50`, `SpendingScore > 100`) → `02_eda_sql.py` → CSV under `outputs/`
8. **Task 8** — Decision Tree classifier (`Outcome`) → `03_models.py`
9. **Task 9** — Logistic Regression classifier (`Outcome`) → `03_models.py`
10. **Task 10** — Linear Regression (`PurchaseAmount ~ AnnualIncome`) → `03_models.py`

## Methodology (short)
- Treat zeros as missing where the brief requires; impute medians (computed excluding zeros) for `SpendingScore` and `TotalPurchases`.
- Remove rows with zero values in `Age`, `AnnualIncome`, and `PurchaseAmount` to form a clean **df3**.
- Perform EDA: compute summary stats, plot distributions, and assess correlation.
- Register `df3` as a temp view and execute the focused SQL.
- Build ML pipelines: encode categoricals (`StringIndexer`→`OneHotEncoder`), assemble features, train/evaluate models.

## Results (from a CI run)
Classification (higher is better):
- **Decision Tree** — Accuracy **0.925**, F1 **0.927**, AUROC **0.963** (excellent ROC quality)
- **Logistic Regression** — Accuracy **0.850**, F1 **0.846**, AUROC **0.922** (excellent ROC quality)

Regression (lower RMSE better, higher R² better):
- **Linear Regression** — RMSE **144.6**, R² **0.016** (low explained variance on this target with a single predictor)

> Note: Metrics may vary slightly run-to-run depending on random splits and environment. Exact values are saved to `outputs/metrics.json`.

## Notes & assumptions
- Zeros are treated as missing according to the brief; medians are computed **excluding zeros**.
- Small samples are collected to Pandas only for plotting; all heavy processing stays in Spark.
- Quartiles and medians use `percentile_approx` which is standard in Spark for efficiency on large data.

## License
MIT — for learning and portfolio demonstration.
