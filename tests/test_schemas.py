"""Tests for Pydantic request/response validation."""

import pytest
from pydantic import ValidationError

# Import path assumes running from project root
import sys
sys.path.insert(0, ".")

from model_service.app.schemas import (
    Transaction,
    PredictionRequest,
    PredictResponse,
    PastPrediction,
)


VALID_TRANSACTION = {
    "amount": 100.0,
    "account_age_days": 365,
    "shipping_distance_km": 10.0,
    "total_transactions_user": 20,
    "avg_amount_user": 80.0,
    "transaction_hour": 14,
    "transaction_day": 2,
    "promo_used": 0,
    "avs_match": 1,
    "three_ds_flag": 1,
    "cvv_result": 1,
    "country": "US",
    "bin_country": "US",
    "merchant_category": "electronics",
    "channel": "web",
}


def test_valid_transaction():
    t = Transaction(**VALID_TRANSACTION)
    assert t.amount == 100.0
    assert t.country == "US"


def test_transaction_missing_field():
    incomplete = {k: v for k, v in VALID_TRANSACTION.items() if k != "amount"}
    with pytest.raises(ValidationError):
        Transaction(**incomplete)


def test_transaction_wrong_type():
    bad = {**VALID_TRANSACTION, "amount": "not_a_number"}
    with pytest.raises(ValidationError):
        Transaction(**bad)


def test_prediction_request_valid():
    req = PredictionRequest(features=[VALID_TRANSACTION])
    assert len(req.features) == 1
    assert req.source == "webapp"


def test_prediction_request_with_source():
    req = PredictionRequest(features=[VALID_TRANSACTION], source="scheduled")
    assert req.source == "scheduled"


def test_prediction_request_empty_features():
    req = PredictionRequest(features=[])
    assert len(req.features) == 0


def test_prediction_request_batch():
    req = PredictionRequest(features=[VALID_TRANSACTION, VALID_TRANSACTION])
    assert len(req.features) == 2


def test_predict_response_valid():
    resp = PredictResponse(count=1, results=[{"prediction": 1, "probability": 0.85}])
    assert resp.count == 1
    assert resp.results[0].prediction == 1


def test_past_prediction_valid():
    pp = PastPrediction(
        id=1,
        prediction=0,
        probability=0.2,
        source="webapp",
        created_at="2024-01-01T00:00:00",
    )
    assert pp.source == "webapp"
