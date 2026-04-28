"""
Tests unitaires pour le module transform.
Verifie les regles qualite critiques du pipeline pharma.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transform import (
    _clean_text_columns,
    _parse_dates,
    _deduplicate,
    _categorize_product_type,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "product_ndc": ["12345-678", "12345-678", "99999-001", "00001-000"],
            "generic_name": ["  paracetamol  ", "paracetamol", "ibuprofen", None],
            "brand_name": ["Tylenol", "Tylenol", "Advil", None],
            "labeler_name": ["Lab A", "Lab A", "Lab B", None],
            "dosage_form": ["TABLET", "TABLET", "CAPSULE", "SOLUTION"],
            "product_type": [
                "HUMAN OTC DRUG",
                "HUMAN OTC DRUG",
                "HUMAN PRESCRIPTION DRUG",
                "VACCINE",
            ],
            "marketing_category": ["OTC monograph final", "OTC monograph final", "NDA", "BLA"],
            "marketing_start_date": ["20100315", "20100315", "20180601", "19981201"],
            "finished": [True, True, True, False],
            "route": ["ORAL", "ORAL", "ORAL", "INTRAMUSCULAR"],
            "pharm_class": [None, None, "Anti-Inflammatory", None],
            "active_ingredient": ["ACETAMINOPHEN", "ACETAMINOPHEN", "IBUPROFEN", None],
            "strength": ["500 mg/1", "500 mg/1", "200 mg/1", None],
        }
    )


class TestCleanTextColumns:
    def test_strips_whitespace(self, sample_df):
        result = _clean_text_columns(sample_df)
        assert result["generic_name"].iloc[0] == "PARACETAMOL"

    def test_names_uppercase(self, sample_df):
        result = _clean_text_columns(sample_df)
        non_null = result["generic_name"].dropna()
        assert non_null.str.isupper().all()

    def test_none_becomes_nan(self, sample_df):
        result = _clean_text_columns(sample_df)
        assert pd.isna(result["generic_name"].iloc[3])


class TestParseDates:
    def test_creates_annee_column(self, sample_df):
        result = _parse_dates(sample_df)
        assert "annee_mise_marche" in result.columns

    def test_year_extracted_correctly(self, sample_df):
        result = _parse_dates(sample_df)
        assert int(result["annee_mise_marche"].iloc[0]) == 2010
        assert int(result["annee_mise_marche"].iloc[2]) == 2018

    def test_invalid_date_becomes_nat(self):
        df = pd.DataFrame({"marketing_start_date": ["99999999", "20200101"]})
        result = _parse_dates(df)
        assert pd.isna(result["annee_mise_marche"].iloc[0])


class TestDeduplicate:
    def test_removes_duplicates(self, sample_df):
        result, nb = _deduplicate(sample_df)
        assert nb == 1
        assert len(result) == 3

    def test_first_occurrence_kept(self, sample_df):
        result, _ = _deduplicate(sample_df)
        assert len(result[result["product_ndc"] == "12345-678"]) == 1


class TestCategorizeProductType:
    def test_otc_mapping(self, sample_df):
        result = _categorize_product_type(sample_df)
        assert result["product_type_cat"].iloc[0] == "OTC"

    def test_prescription_mapping(self, sample_df):
        result = _categorize_product_type(sample_df)
        assert result["product_type_cat"].iloc[2] == "Prescription"

    def test_vaccine_mapping(self, sample_df):
        result = _categorize_product_type(sample_df)
        assert result["product_type_cat"].iloc[3] == "Vaccin"

    def test_unknown_becomes_autre(self):
        df = pd.DataFrame({"product_type": ["UNKNOWN TYPE"]})
        result = _categorize_product_type(df)
        assert result["product_type_cat"].iloc[0] == "Autre"
