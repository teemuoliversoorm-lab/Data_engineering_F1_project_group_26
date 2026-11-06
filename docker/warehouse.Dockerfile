FROM postgres:17.7
COPY 01_ingestion/sql/bronze_setup.sql /docker-entrypoint-initdb.d/01-bronze.sql
