FROM python:3.11-slim
RUN pip install --no-cache-dir dbt-core==1.9.10 dbt-postgres==1.9.0
WORKDIR /project/dbt
ENTRYPOINT ["dbt"]
COPY dbt /project/dbt
