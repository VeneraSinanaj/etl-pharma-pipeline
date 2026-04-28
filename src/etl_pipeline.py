"""
Orchestrateur ETL — exécute les trois étapes Extract → Transform → Load
et affiche un rapport de synthèse final.

Usage :
    python src/etl_pipeline.py
    python src/etl_pipeline.py --max-records 500   # pour un test rapide
"""

import argparse
import logging
import time
from pathlib import Path

from extract import extract
from transform import transform
from load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BANNER = """
+======================================================+
|       ETL Pipeline -- Base Publique des Medicaments  |
|       Source : ANSM / data.gouv.fr                   |
+======================================================+
"""


def run_pipeline(max_records: int = 2000) -> dict:
    """
    Lance le pipeline ETL complet.

    Args:
        max_records: Nombre maximum d'enregistrements à extraire.

    Returns:
        Dictionnaire des statistiques finales.
    """
    print(BANNER)
    t_start = time.time()

    # ── EXTRACT ──────────────────────────────────────────────
    logger.info("━━━ ÉTAPE 1/3 : EXTRACT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    df_raw = extract(max_records=max_records)
    t_extract = time.time() - t0

    # ── TRANSFORM ────────────────────────────────────────────
    logger.info("━━━ ÉTAPE 2/3 : TRANSFORM ━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    df_clean = transform(df_raw)
    t_transform = time.time() - t0

    # ── LOAD ─────────────────────────────────────────────────
    logger.info("━━━ ÉTAPE 3/3 : LOAD ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    t0 = time.time()
    stats = load(df_clean)
    t_load = time.time() - t0

    t_total = time.time() - t_start

    # ── RAPPORT FINAL ─────────────────────────────────────────
    nb_doublons = len(df_raw) - len(df_clean)
    pct_completude = (
        1 - df_clean.isna().sum().sum() / (len(df_clean) * len(df_clean.columns))
    ) * 100

    rapport = f"""
+======================================================+
|            RAPPORT D'EXECUTION ETL                   |
+======================================================+
|  [OK] Extraction  : {len(df_raw):>6,} enregistrements recuperes  |
|  [->] Doublons   : {nb_doublons:>6,} supprimes                   |
|  [DB] Chargement : {stats['total']:>6,} enregistrements en base   |
|  [%%] Completude : {pct_completude:>5.1f}%% de donnees completes      |
|  [Rx] Prescription : {stats['prescription']:>4,} / OTC : {stats['otc']:>4,}          |
+------------------------------------------------------+
|  Extract   : {t_extract:>5.1f}s                                  |
|  Transform : {t_transform:>5.1f}s                                  |
|  Load      : {t_load:>5.1f}s                                  |
|  Total     : {t_total:>5.1f}s                                  |
+======================================================+
"""
    print(rapport)

    return {
        "nb_extraits": len(df_raw),
        "nb_doublons": nb_doublons,
        "nb_charges": stats["total"],
        "completude_pct": round(pct_completude, 1),
        "temps_total_s": round(t_total, 1),
        **stats,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pipeline — Médicaments ANSM")
    parser.add_argument(
        "--max-records",
        type=int,
        default=2000,
        help="Nombre max d'enregistrements à extraire (défaut : 2000)",
    )
    args = parser.parse_args()
    run_pipeline(max_records=args.max_records)
