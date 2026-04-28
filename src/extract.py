"""
Extract module -- recupere les donnees medicaments depuis l'API OpenFDA (NDC).
Source : https://open.fda.gov/apis/drug/ndc/
API publique, pas de cle requise pour moins de 1000 req/jour.
"""

import requests
import pandas as pd
import logging
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_URL = "https://api.fda.gov/drug/ndc.json"
RAW_OUTPUT = Path(__file__).parent.parent / "data" / "raw" / "medicaments_raw.csv"

BATCH_SIZE = 100
MAX_RECORDS = 2000


def fetch_batch(skip: int, limit: int = BATCH_SIZE) -> dict:
    """Recupere un lot de medicaments depuis l'API OpenFDA."""
    params = {"limit": limit, "skip": skip}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _flatten_record(record: dict) -> dict:
    """Aplatit les champs imbriques (listes) en valeurs simples."""
    flat = {}
    flat["product_ndc"] = record.get("product_ndc")
    flat["generic_name"] = record.get("generic_name")
    flat["brand_name"] = record.get("brand_name")
    flat["labeler_name"] = record.get("labeler_name")
    flat["dosage_form"] = record.get("dosage_form")
    flat["product_type"] = record.get("product_type")
    flat["marketing_category"] = record.get("marketing_category")
    flat["marketing_start_date"] = record.get("marketing_start_date")
    flat["finished"] = record.get("finished")

    # Voies d'administration (liste -> chaine separee par ';')
    routes = record.get("route", [])
    flat["route"] = "; ".join(routes) if routes else None

    # Classes pharmacologiques
    pharm = record.get("pharm_class", [])
    flat["pharm_class"] = "; ".join(pharm[:2]) if pharm else None  # garder les 2 premieres

    # Ingredients actifs : nom + dosage du premier ingredient
    ingredients = record.get("active_ingredients", [])
    if ingredients:
        flat["active_ingredient"] = ingredients[0].get("name")
        flat["strength"] = ingredients[0].get("strength")
    else:
        flat["active_ingredient"] = None
        flat["strength"] = None

    return flat


def extract(max_records: int = MAX_RECORDS) -> pd.DataFrame:
    """
    Extrait les medicaments via l'API OpenFDA (Drug NDC).

    Returns:
        DataFrame des medicaments bruts.
    """
    logger.info("Demarrage de l'extraction OpenFDA...")

    records = []
    skip = 0

    # Premier appel pour connaitre le total disponible
    try:
        first_batch = fetch_batch(skip=0, limit=1)
        total_available = first_batch.get("meta", {}).get("results", {}).get("total", 0)
        total_to_fetch = min(total_available, max_records)
        logger.info(f"   API disponible : {total_available:,} medicaments -- on en recupere {total_to_fetch:,}")
    except requests.exceptions.RequestException as e:
        logger.error(f"   Impossible de joindre l'API : {e}")
        raise

    while skip < total_to_fetch:
        try:
            batch_limit = min(BATCH_SIZE, total_to_fetch - skip)
            batch = fetch_batch(skip=skip, limit=batch_limit)
            batch_records = batch.get("results", [])
            if not batch_records:
                break
            records.extend([_flatten_record(r) for r in batch_records])
            skip += len(batch_records)
            logger.info(f"   Recupere {skip}/{total_to_fetch} enregistrements...")
            time.sleep(0.15)  # politesse envers l'API (limite : 240 req/min sans cle)
        except requests.exceptions.RequestException as e:
            logger.warning(f"   Erreur reseau a l'offset {skip} : {e} -- nouvelle tentative dans 5s")
            time.sleep(5)
            continue

    df = pd.DataFrame(records)
    logger.info(f"Extraction terminee : {len(df)} enregistrements, {len(df.columns)} colonnes")

    # Sauvegarde des donnees brutes (audit trail)
    RAW_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RAW_OUTPUT, index=False, encoding="utf-8-sig")
    logger.info(f"Donnees brutes sauvegardees -> {RAW_OUTPUT}")

    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())
    print(f"\nColonnes disponibles : {list(df.columns)}")
