"""
Transform module -- nettoyage, standardisation et feature engineering des donnees medicaments.
Applique les regles qualite essentielles pour le secteur pharmaceutique.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TRANSFORMED_OUTPUT = Path(__file__).parent.parent / "data" / "transformed" / "medicaments_clean.csv"


def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise les colonnes texte : strip, None -> NaN, noms en majuscules."""
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"None": np.nan, "nan": np.nan, "": np.nan})

    # Conventions pharma : noms des medicaments en majuscules
    for col in ["generic_name", "brand_name", "active_ingredient"]:
        if col in df.columns:
            df[col] = df[col].str.upper()

    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit marketing_start_date (format YYYYMMDD) en date ISO
    et extrait l'annee de mise sur le marche.
    """
    if "marketing_start_date" not in df.columns:
        return df
    df["marketing_start_date"] = pd.to_datetime(
        df["marketing_start_date"], format="%Y%m%d", errors="coerce"
    )
    df["annee_mise_marche"] = df["marketing_start_date"].dt.year.astype("Int64")
    df["marketing_start_date"] = df["marketing_start_date"].dt.date
    return df


def _deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Supprime les doublons sur product_ndc (identifiant unique FDA)."""
    before = len(df)
    if "product_ndc" in df.columns:
        df = df.drop_duplicates(subset=["product_ndc"], keep="first")
    else:
        df = df.drop_duplicates()
    nb_removed = before - len(df)
    return df, nb_removed


def _categorize_product_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simplifie les types de produits en categories standardisees.
    Feature engineering utile pour des analyses segment par segment.
    """
    if "product_type" not in df.columns:
        return df
    mapping = {
        "HUMAN PRESCRIPTION DRUG": "Prescription",
        "HUMAN OTC DRUG": "OTC",
        "PLASMA DERIVATIVE": "Derive Plasmatique",
        "NON-STANDARDIZED ALLERGENIC": "Allergenique",
        "VACCINE": "Vaccin",
        "CELLULAR THERAPY": "Therapie Cellulaire",
    }
    df["product_type_cat"] = df["product_type"].map(mapping).fillna("Autre")
    return df


def _validate_quality(df: pd.DataFrame) -> float:
    """Logge un rapport qualite et retourne le taux de completude global."""
    total = len(df)
    for col in df.columns:
        pct_missing = df[col].isna().sum() / total * 100
        if pct_missing > 5:
            logger.info(f"   Attention  {col} : {pct_missing:.1f}%% valeurs manquantes")
    completude = (1 - df.isna().sum().sum() / (total * len(df.columns))) * 100
    logger.info(f"   Taux de completude global : {completude:.1f}%%")
    return completude


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie et enrichit le DataFrame medicaments.

    Transformations appliquees :
    - Nettoyage et standardisation des textes
    - Parsing et feature engineering sur les dates
    - Deduplication sur l'identifiant unique (product_ndc)
    - Categorisation du type de produit
    - Rapport de qualite des donnees

    Args:
        df: DataFrame brut issu de l'extraction.

    Returns:
        DataFrame transforme, pret pour le chargement.
    """
    logger.info("Demarrage de la transformation...")
    initial_count = len(df)

    df = _clean_text_columns(df)
    df = _parse_dates(df)
    df, nb_doublons = _deduplicate(df)
    df = _categorize_product_type(df)

    # Supprimer les lignes sans identifiant (inutilisables)
    if "product_ndc" in df.columns:
        before = len(df)
        df = df.dropna(subset=["product_ndc"])
        dropped = before - len(df)
        if dropped:
            logger.info(f"   Supprime {dropped} lignes sans product_ndc")

    df = df.reset_index(drop=True)

    logger.info(f"   Doublons supprimes     : {nb_doublons}")
    logger.info(f"   Enregistrements entree : {initial_count}")
    logger.info(f"   Enregistrements sortie : {len(df)}")

    _validate_quality(df)

    # Sauvegarde intermediaire (permet de relancer uniquement Load si besoin)
    TRANSFORMED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(TRANSFORMED_OUTPUT, index=False, encoding="utf-8-sig")
    logger.info(f"Transformation terminee -> {TRANSFORMED_OUTPUT}")

    return df


if __name__ == "__main__":
    raw_path = Path(__file__).parent.parent / "data" / "raw" / "medicaments_raw.csv"
    df_raw = pd.read_csv(raw_path, encoding="utf-8-sig")
    df_clean = transform(df_raw)
    print(df_clean.dtypes)
    print(df_clean.head(3))
