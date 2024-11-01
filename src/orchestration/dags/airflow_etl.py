import os
from datetime import timedelta

from airflow import DAG
# from airflow.models.taskinstance import TaskInstance as ti
from utils.json_to_duckdb import JsonToDuck

## 2
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
source_dir = os.getcwd()
while not source_dir.endswith("airflow"):
    source_dir = os.path.dirname(source_dir)
dest_dir = os.path.join(source_dir, "dest_data")
source_dir = os.path.join(source_dir, "source_data")

with DAG(
    "json_to_parquet",
    default_args=default_args,
    schedule_interval=None,
    tags=["Pipeline"],
) as dag:

    @dag.task()
    def run():
        temp_table = jd.extract(
            source_is_encrypted=False,
            source_file=f"{source_dir}/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            source_table_name="payments",
        )
        print(temp_table)

        jd.load(
            append=True,
            dest_file=f"{dest_dir}/payments.parquet",
            dest_table_name="payments",
            dest_is_encrypted=False,
            temp_table_name=temp_table,
        )

        print(jd._get_statistics())

    @dag.task()
    def end():
        jd.__del__()


run() >> end()
