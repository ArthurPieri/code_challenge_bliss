import pytest


@pytest.fixture
def fixture_new_data():
    return {
        "payment_id": "101",
        "transaction_id": "101",
        "amount_paid": 20.0,
        "date_paid": "2025-06-21",
    }
