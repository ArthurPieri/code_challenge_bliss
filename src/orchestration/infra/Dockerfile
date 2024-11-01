FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt update && \
    apt install -y gcc g++ pkg-config && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /requirements-force.txt

RUN pip install -r /requirements-force.txt

# uninstall azure - bug airflow
RUN pip uninstall apache-airflow-providers-microsoft-azure --yes

ENTRYPOINT ["bash", "/entrypoint"]

EXPOSE 8080 5555 8793
