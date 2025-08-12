# Simple image with Python + Java to run PySpark locally
FROM python:3.11-slim

# Install Java (required by PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default help
CMD ["bash", "-lc", "echo 'Use: python src/01_data_prep.py && python src/02_eda_sql.py && python src/03_models.py'"]
