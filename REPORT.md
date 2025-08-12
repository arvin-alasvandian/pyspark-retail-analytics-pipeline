# Methodology & Results (short)

#### Methodology & Results (short)

##### Methodology
In this coursework, I used **PySpark** to build a reproducible pipeline that follows the brief exactly.
1. **Data loading (Task 1).** I loaded `customer_purchases.csv` into a PySpark DataFrame and checked the schema and row count to understand data types and data quality.
2. **Data cleaning (Tasks 2–3).** Following the brief, I treated the value **0** as missing for relevant columns. I imputed the median (calculated **excluding zeros**) for `SpendingScore` and `TotalPurchases` to create the 2nd DataFrame, and then removed rows with zeros in `Age`, `AnnualIncome`, or `PurchaseAmount` to create the 3rd DataFrame for analysis and modelling.
3. **Exploratory analysis (Tasks 4–6).** I computed summary statistics for `PurchaseAmount` and produced a histogram. I calculated quartiles for `TotalPurchases` and used a boxplot to assess spread and potential outliers. I also explored the relationship between `PurchaseAmount` and `SpendingScore` with a scatter plot and calculated the **Pearson correlation**.
4. **Spark SQL (Task 7).** I registered a temporary view on the cleaned DataFrame and ran the required SQL to list customers where `Age < 50` and `SpendingScore > 100`.
5. **Feature preparation and modelling (Tasks 8–10).** I encoded `Gender` and `PurchaseCategory` with `StringIndexer`/`OneHotEncoder`, assembled features with `VectorAssembler`, and trained (i) a **Decision Tree** and (ii) **Logistic Regression** to predict `Outcome`. I also trained a **Linear Regression** model to predict `PurchaseAmount` from `AnnualIncome`.
6. **Evaluation.** For classification, I reported **Accuracy**, **F1**, and **AUROC**; for regression, I reported **RMSE** and **R²**. Plots are saved under `figures/`, SQL output under `outputs/`, and metrics in `outputs/metrics.json`.

##### Results (high level)
- **Data quality change:** The number of rows removed is printed as `|df2| − |df3|`, showing how much missingness was addressed.
- **`PurchaseAmount` distribution:** Histogram and summary statistics (min, max, mean, median, variance, std. dev.) give a clear view of central tendency and spread.
- **`TotalPurchases` quartiles & boxplot:** Q1/Median/Q3 and the boxplot indicate dispersion and potential outliers.
- **Association of `PurchaseAmount` and `SpendingScore`:** Scatter plot shows the pattern; **Pearson correlation** quantifies linear association.
- **Model performance:** Decision Tree and Logistic Regression metrics (Accuracy, F1, AUROC) and Linear Regression metrics (RMSE, R²) are logged and saved to `outputs/metrics.json`.

## Findings (paste-ready)

## Findings (paste into your report after running the code)

> Replace the bracketed placeholders with your actual numbers from the console and `outputs/metrics.json`.

- **Rows removed (Task 4):** We removed **[N]** rows when moving from `df2` to `df3`, indicating the proportion of entries with missing (zero) values in the required fields.
- **`PurchaseAmount` summary (Task 4):** min = **[min]**, max = **[max]**, mean = **[mean]**, median = **[median]**, variance = **[variance]**, std. dev. = **[stddev]**. The histogram suggests the distribution is **[roughly symmetric / right-skewed / left-skewed]** with **[few/many]** extreme values.
- **`TotalPurchases` quartiles (Task 5):** Q1 = **[q1]**, Median = **[q2]**, Q3 = **[q3]**. The boxplot indicates **[tight/moderate/wide]** dispersion and **[presence/absence]** of outliers.
- **Correlation (Task 6):** The Pearson correlation between `PurchaseAmount` and `SpendingScore` is **[corr]**, which indicates a **[weak/moderate/strong] [positive/negative]** linear relationship.
- **Decision Tree performance (Task 8):** Accuracy = **[acc_dt]**, F1 = **[f1_dt]**, AUROC = **[auroc_dt]**.
- **Logistic Regression performance (Task 9):** Accuracy = **[acc_lr]**, F1 = **[f1_lr]**, AUROC = **[auroc_lr]**.
- **Linear Regression performance (Task 10):** RMSE = **[rmse]**, R² = **[r2]**. Interpretation: the model explains **[x%]** of the variance in `PurchaseAmount`, where `x = 100 × R²`.

> You can find the metrics in `outputs/metrics.json` after running `src/03_models.py`. Plots are saved under `figures/` to support the narrative.
