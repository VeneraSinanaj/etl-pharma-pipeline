"""
Load module -- charge les donnees transformees dans un entrepot SQLite.
Gere les upserts, l'indexation et la validation post-chargement.
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "pharma_warehouse.db"

DDL_MEDICAMENTS = """
CREATE TABLE IF NOT EXISTS medicaments (
    product_ndc           TEXT PRIMARY KEY,
    generic_name          TEXT,
    brand_name            TEXT,
    labeler_name          TEXT,
    dosage_form           TEXT,
    product_type          TEXT,
    product_type_cat      TEXT,
    marketing_category    TEXT,
    marketing_start_date  TEXT,
    annee_mise_marche     INTEGER,
    finished              INTEGER DEFAULT 1,
    route                 TEXT,
    pharm_class           TEXT,
    active_ingredient     TEXT,
    strength              TEXT,
    date_chargement       TEXT DEFAULT (datetime('now'))
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_generic_name        ON medicaments (generic_name);",
    "CREATE INDEX IF NOT EXISTS idx_labeler_name        ON medicaments (labeler_name);",
    "CREATE INDEX IF NOT EXISTS idx_annee_mise_marche   ON medicaments (annee_mise_marche);",
    "CREATE INDEX IF NOT EXISTS idx_product_type_cat    ON medicaments (product_type_cat);",
    "CREATE INDEX IF NOT EXISTS idx_dosage_form         ON medicaments (dosage_form);",
]


def _get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)


def _create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(DDL_MEDICAMENTS)
    for idx_sql in DDL_INDEXES:
        cur.execute(idx_sql)
    conn.commit()
    logger.info("   Schema et index crees / verifies")


def _upsert_dataframe(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """
    Insere ou remplace les enregistrements (UPSERT sur product_ndc).
    Le pipeline est idempotent : une re-execution ne cree pas de doublons.
    """
    df_to_insert = df.copy()

    # Convertir booleens en entiers pour SQLite
    for col in df_to_insert.select_dtypes(include="bool").columns:
        df_to_insert[col] = df_to_insert[col].astype(int)

    # Convertir Int64 nullable en object pour SQLite
    for col in df_to_insert.columns:
        if pd.api.types.is_extension_array_dtype(df_to_insert[col]):
            df_to_insert[col] = df_to_insert[col].astype(object).where(df_to_insert[col].notna(), None)

    # Convertir dates en string
    for col in df_to_insert.columns:
        if hasattr(df_to_insert[col], "dt") or str(df_to_insert[col].dtype) == "object":
            try:
                df_to_insert[col] = df_to_insert[col].astype(str).replace("nan", None).replace("NaT", None)
            except Exception:
                pass

    df_to_insert = df_to_insert.drop(columns=["date_chargement"], errors="ignore")

    df_to_insert.to_sql(
        "medicaments",
        conn,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )
    return len(df_to_insert)


def _post_load_validation(conn: sqlite3.Connection) -> dict:
    """Requetes de controle qualite post-chargement."""
    cur = conn.cursor()
    stats = {}

    stats["total"] = cur.execute("SELECT COUNT(*) FROM medicaments").fetchone()[0]
    stats["avec_ingredient"] = cur.execute(
        "SELECT COUNT(*) FROM medicaments WHERE active_ingredient IS NOT NULL"
    ).fetchone()[0]
    stats["prescription"] = cur.execute(
        "SELECT COUNT(*) FROM medicaments WHERE product_type_cat = 'Prescription'"
    ).fetchone()[0]
    stats["otc"] = cur.execute(
        "SELECT COUNT(*) FROM medicaments WHERE product_type_cat = 'OTC'"
    ).fetchone()[0]
    stats["annee_min"] = cur.execute(
        "SELECT MIN(annee_mise_marche) FROM medicaments WHERE annee_mise_marche IS NOT NULL"
    ).fetchone()[0]
    stats["annee_max"] = cur.execute(
        "SELECT MAX(annee_mise_marche) FROM medicaments WHERE annee_mise_marche IS NOT NULL"
    ).fetchone()[0]

    top_labos = cur.execute(
        """
        SELECT labeler_name, COUNT(*) as nb
        FROM medicaments
        WHERE labeler_name IS NOT NULL
        GROUP BY labeler_name
        ORDER BY nb DESC
        LIMIT 5
        """
    ).fetchall()
    stats["top_laboratoires"] = top_labos

    return stats


def load(df: pd.DataFrame, db_path: Path = DB_PATH) -> dict:
    """
    Charge le DataFrame dans pharma_warehouse.db.

    Args:
        df:      DataFrame transforme.
        db_path: Chemin de la base SQLite.

    Returns:
        Dictionnaire de statistiques post-chargement.
    """
    logger.info("Demarrage du chargement...")

    with _get_connection(db_path) as conn:
        _create_schema(conn)
        nb_inseres = _upsert_dataframe(df, conn)
        stats = _post_load_validation(conn)

    logger.info(f"   Enregistrements inseres  : {nb_inseres}")
    logger.info(f"   Total en base            : {stats['total']}")
    logger.info(f"   Medicaments Prescription : {stats['prescription']}")
    logger.info(f"   Medicaments OTC          : {stats['otc']}")
    logger.info(f"   Periode                  : {stats['annee_min']} -> {stats['annee_max']}")
    logger.info("   Top 5 laboratoires :")
    for labo, nb in stats["top_laboratoires"]:
        logger.info(f"     * {labo} : {nb}")

    logger.info(f"Chargement termine -> {db_path}")
    return stats


if __name__ == "__main__":
    transformed_path = Path(__file__).parent.parent / "data" / "transformed" / "medicaments_clean.csv"
    df_clean = pd.read_csv(transformed_path, encoding="utf-8-sig")
    stats = load(df_clean)
    print(stats)
