import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from ..etl.json_to_duckdb import JsonToDuck


@pytest.fixture(scope="module")
def obj():
    yield JsonToDuck()


class TestJsonToDuckdb:
    """
    Test pipeline class: JsonToDuck
    """

    def test_assert_fail_extract(self, obj):
        try:
            # Missing password arg
            obj.extract(
                is_encrypted=True,
                source_file="fixtures/payments.json",
                select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
                table_name="payments_test",
            )
        except Exception as exc:
            assert isinstance(exc, KeyError)

    def test_extract(self, obj):
        """Test extract"""
        c_dir = os.getcwd()
        if not c_dir.endswith("code_challenge_bliss"):
            c_dir = os.path.dirname(c_dir)
        if "tests" not in c_dir:
            c_dir = os.path.join(c_dir, "tests")

        temp_table = obj.extract(
            is_encrypted=False,
            source_file=f"{c_dir}/fixtures/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            table_name="payments_test",
        )
        assert temp_table == "payments_test_temp"

    def test_extract_encrypted(self, obj):
        c_dir = os.getcwd()
        if not c_dir.endswith("code_challenge_bliss"):
            c_dir = os.path.dirname(c_dir)
        if "tests" not in c_dir:
            c_dir = os.path.join(c_dir, "tests")

        temp_table = obj.extract(
            is_encrypted=True,
            password="123456",
            source_file=f"{c_dir}/fixtures/payments.json",
            select_query="SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
            table_name="payments_test",
        )
        assert temp_table == "payments_test_temp"

    def test_assert_fail_load(self, obj):
        try:
            # Missing table_name arg
            obj.load(append=False, filename="file.load", temp_table_name="jose")
        except Exception as exc:
            assert isinstance(exc, AssertionError)

    def test_load(self, obj): ...

    def test_load_encrypted(self, obj): ...

    def test_run(self, obj): ...
