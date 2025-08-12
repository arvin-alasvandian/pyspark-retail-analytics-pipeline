
.PHONY: init prep eda models clean

init:
	python -m pip install -r requirements.txt

prep:
	python src/01_data_prep.py

eda:
	python src/02_eda_sql.py

models:
	python src/03_models.py

clean:
	rm -rf data/parquet figures/*.png outputs/*
