FROM apache/airflow:2.11.1-python3.11
COPY requirements.txt /tmp/f1-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.11.1" -r /tmp/f1-requirements.txt
ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
COPY --chown=airflow:root . /opt/airflow/project
