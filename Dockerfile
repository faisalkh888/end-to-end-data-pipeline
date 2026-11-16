FROM apache/airflow:2.10.4-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.4" -r /requirements.txt psycopg2-binary
WORKDIR /opt/airflow
COPY app /opt/airflow/app
COPY .streamlit /opt/airflow/.streamlit
