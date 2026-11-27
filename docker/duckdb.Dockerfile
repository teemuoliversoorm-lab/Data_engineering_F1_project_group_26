FROM python:3.11-slim
WORKDIR /app
COPY 04_duckdb/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY 04_duckdb/refresh_duckdb.py .
COPY 04_duckdb/compare_engines.py .
# Baked in at build time so no host-path bind mount is needed to run the
# comparison -- works the same whether launched via `docker compose run` or
# via Airflow's DockerOperator (Docker-outside-of-Docker, see 00_dags/).
COPY 03_queries queries
