# GitHub Setup — Copy/Paste Guide

## A) Web Upload (no terminal)
1. Create a new public repo on GitHub.
2. Unzip this project and upload **all files/folders** (do not upload the zip). Click **Commit**.

## B) GitHub Desktop (GUI)
1. **File → New repository from existing files** → select the unzipped project folder.
2. Click **Publish repository**.

## C) Terminal (recommended)
```bash
# inside the project folder
python -m pip install -r requirements.txt
python src/01_data_prep.py
python src/02_eda_sql.py
python src/03_models.py
python src/04_autofill_report.py  # optional

git init
git add .
git commit -m "Init: PySpark big-data portfolio project"
git branch -M main
git remote add origin https://github.com/arvin-alasvandian/pyspark-retail-analytics-pipeline.git
git push -u origin main
```

## D) GitHub CLI (if you have `gh`)
```bash
gh repo create pyspark-retail-analytics-pipeline --public --source=. --remote=origin --push   --description "End-to-end PySpark pipeline: data cleaning, EDA, Spark SQL, and MLlib models with saved figures/metrics."

# Add topics for discoverability
gh repo edit arvin-alasvandian/pyspark-retail-analytics-pipeline --add-topic pyspark --add-topic spark --add-topic big-data   --add-topic mllib --add-topic data-science --add-topic portfolio --add-topic retail-analytics
```
