import os
from datetime import timedelta

from airflow import DAG
from airflow.models.taskinstance import TaskInstance as ti
from etl.json_to_duckdb import JsonToDuck

default_args = {
    "owner": "airflow",
    "email": ["arthur@arthurpieri.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "max_active_tasks": 1,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "dagrun_timeout": timedelta(minutes=10),
    "wait_for_downstream": True,
    "depends_on_past": True,
    "provide_context": True,
    "on_failure_callback": None,
}

jd = JsonToDuck()
c_dir = os.getcwd()
if not c_dir.endswith("code_challenge_bliss"):
    c_dir = os.path.dirname(c_dir)

with DAG(
    "Json to Parquet",
    default_args=default_args,
    schedule_interval=None,
    tags=["Pipeline"],
) as dag:

    @dag.task()
    def run_extract(task_instance: ti):
        temp_table = jd.extract(
            source_is_encrypted=False,
            source_file=f"{c_dir}/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            source_table_name="payments",
        )
        task_instance.xcom_push(key="temp_table_name", value=temp_table)

    @dag.task()
    def run_load(task_instance: ti):
        temp_table = task_instance.xcom_pull(
            task_ids="run_extract", key="temp_table_name"
        )
        jd.load(
            append=True,
            dest_file=f"{c_dir}/payments.parquet",
            dest_table_name="payments",
            dest_is_encrypted=False,
            temp_table_name=temp_table,
        )

    @dag.task()
    def end():
        jd.__del__()


run_extract() >> run_load() >> end()
