FROM python:3.11-slim
RUN pip install --no-cache-dir dbt-core==1.9.10 dbt-postgres==1.9.0
WORKDIR /project/dbt
ENTRYPOINT ["dbt"]
# 02_dbt/ on disk (numbered for pipeline-order browsing) -> /project/dbt in the container.
COPY 02_dbt /project/dbt
