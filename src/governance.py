from utils.json_to_duckdb import JsonToDuck
import os

if __name__ == "__main__":
    jd = JsonToDuck()
    source_dir = os.getcwd()
    while not source_dir.endswith("code_challenge_bliss"):
        source_dir = os.path.dirname(source_dir)
    dest_dir = os.path.join(source_dir, "dest_data")
    source_dir = os.path.join(source_dir, "source_data")

    temp = jd.extract(
        source_is_encrypted=True,
        source_file=f"{source_dir}/payments.crypt.parquet",
        select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
        source_table_name="payments_test",
        source_password="123456"
    )
    jd.load(
        append=True,
        dest_file=f"{dest_dir}/payments.decrypt.parquet",
        dest_table_name="payments_test_crypt_temp",
        dest_is_encrypted=False,
        dest_password="123456",
        temp_table_name=temp,
    )
    print(jd._get_statistics())

