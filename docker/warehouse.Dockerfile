FROM postgres:17.7
COPY SQL/bronze_setup.sql /docker-entrypoint-initdb.d/01-bronze.sql
