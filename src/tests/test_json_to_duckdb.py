import os

import pytest

from ..utils.json_to_duckdb import JsonToDuck


@pytest.fixture(scope="module")
def obj():
    yield JsonToDuck()


source_dir = os.getcwd()
while not source_dir.endswith("code_challenge_bliss"):
    source_dir = os.path.dirname(source_dir)


source_dir = os.path.join(source_dir, "src/tests")


class TestJsonToDuckdb:
    """
    Test pipeline class: JsonToDuck
    """

    def _create_file(self, obj):
        temp = obj.extract(
            source_is_encrypted=False,
            source_file=f"{source_dir}/fixtures/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            source_table_name="payments_test",
        )
        obj.load(
            append=False,
            dest_file=f"{source_dir}/fixtures/payments.crypt.parquet",
            dest_table_name="payments_test",
            dest_is_encrypted=True,
            dest_password="123456",
            temp_table_name=temp,
        )

    def test_assert_fail_extract(self, obj):
        try:
            # Missing password arg
            obj.extract(
                source_is_encrypted=True,
                source_file="fixtures/payments.json",
                select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
                source_table_name="payments_test",
            )
        except Exception as exc:
            assert isinstance(exc, AssertionError)

    def test_extract(self, obj):
        """Test extract"""
        temp_table = obj.extract(
            source_is_encrypted=False,
            source_file=f"{source_dir}/fixtures/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            source_table_name="payments_test_",
        )
        assert temp_table == "payments_test__temp"

    def test_extract_encrypted(self, obj):
        """
        This test fails inexplicably sometimes.
        """
        self._create_file(obj)

        temp_table = obj.extract(
            source_is_encrypted=True,
            source_password="123456",
            source_file=f"{source_dir}/fixtures/payments.crypt.parquet",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            source_table_name="payments_test",
        )
        assert temp_table == "payments_test_crypt_temp"

    def test_assert_fail_load(self, obj):
        try:
            # Missing table_name arg
            obj.load(append=False, filename="file.load", temp_table_name="jose")
        except Exception as exc:
            assert isinstance(exc, AssertionError)

    def test_load(self, obj):
        dest_file = f"{source_dir}/fixtures/test_load.parquet"
        obj.load(
            append=False,
            dest_file=dest_file,
            dest_table_name="payments_test_temp",
            temp_table_name="payments_test_temp",
            dest_is_encrypted=False,
        )

        with open(dest_file, "rb") as f:
            data = f.read()
        assert data

    def test_load_encrypted(self, obj):
        dest_file = f"{source_dir}/fixtures/test_load.crypt.parquet"
        obj.load(
            append=False,
            dest_file=dest_file,
            dest_table_name="payments_test",
            temp_table_name="payments_test_temp",
            dest_is_encrypted=True,
            dest_password="123456",
        )

        with open(dest_file, "rb") as f:
            data = f.read()
        assert data
