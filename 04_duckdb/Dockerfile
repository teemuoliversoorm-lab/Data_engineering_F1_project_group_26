FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY refresh_duckdb.py .
COPY run_queries.py .
COPY compare_engines.py .

CMD ["python", "refresh_duckdb.py"]