FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt update && \
    apt install -y gcc g++ pkg-config && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements-force.txt
COPY providers.txt /providers.txt

RUN pip install -r /providers.txt && \
    pip install -r /requirements-force.txt

RUN sleep 1

RUN pip install duckdb && \
    pip install airflow-provider-duckdb

ENTRYPOINT ["bash", "/entrypoint"]

EXPOSE 8080 5555 8793
