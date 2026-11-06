FROM apache/airflow:2.11.1-python3.11
COPY requirements.txt /tmp/f1-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.11.1" -r /tmp/f1-requirements.txt
ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
# Repo folders are numbered (00_dags, 01_ingestion, ...) so a reader can see
# the pipeline order at a glance. Inside the container they land at plain
# names so `import ingestion...` keeps working and Airflow's DAGS_FOLDER is
# a normal path.
COPY --chown=airflow:root 00_dags /opt/airflow/project/dags
COPY --chown=airflow:root 01_ingestion /opt/airflow/project/ingestion
COPY --chown=airflow:root tests /opt/airflow/project/tests
