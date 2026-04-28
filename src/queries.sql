-- ============================================================
-- Requetes analytiques -- Base Medicaments FDA (OpenFDA NDC)
-- Base : pharma_warehouse.db
-- ============================================================

-- 1. Vue d'ensemble du catalogue
SELECT
    COUNT(*)                                                    AS total_medicaments,
    COUNT(DISTINCT labeler_name)                               AS nb_laboratoires,
    COUNT(DISTINCT dosage_form)                                AS nb_formes,
    SUM(CASE WHEN product_type_cat = 'Prescription' THEN 1 ELSE 0 END) AS prescription,
    SUM(CASE WHEN product_type_cat = 'OTC'          THEN 1 ELSE 0 END) AS otc,
    MIN(annee_mise_marche)                                     AS premiere_mise_marche,
    MAX(annee_mise_marche)                                     AS derniere_mise_marche
FROM medicaments;


-- 2. Top 10 laboratoires par nombre de references
SELECT
    labeler_name,
    COUNT(*) AS nb_medicaments,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM medicaments), 2) AS part_pct
FROM medicaments
WHERE labeler_name IS NOT NULL AND labeler_name != 'None'
GROUP BY labeler_name
ORDER BY nb_medicaments DESC
LIMIT 10;


-- 3. Repartition Prescription vs OTC par decennie
SELECT
    (annee_mise_marche / 10) * 10          AS decennie,
    SUM(CASE WHEN product_type_cat = 'Prescription' THEN 1 ELSE 0 END) AS prescription,
    SUM(CASE WHEN product_type_cat = 'OTC'          THEN 1 ELSE 0 END) AS otc,
    COUNT(*)                               AS total
FROM medicaments
WHERE annee_mise_marche IS NOT NULL
GROUP BY decennie
ORDER BY decennie;


-- 4. Formes pharmaceutiques les plus courantes
SELECT
    dosage_form,
    COUNT(*) AS nb,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM medicaments WHERE dosage_form IS NOT NULL), 1) AS pct
FROM medicaments
WHERE dosage_form IS NOT NULL AND dosage_form != 'None'
GROUP BY dosage_form
ORDER BY nb DESC
LIMIT 15;


-- 5. Classes pharmacologiques les plus representees
SELECT
    pharm_class,
    COUNT(*) AS nb
FROM medicaments
WHERE pharm_class IS NOT NULL AND pharm_class != 'None'
GROUP BY pharm_class
ORDER BY nb DESC
LIMIT 10;


-- 6. Ingredients actifs les plus references (top 20)
SELECT
    active_ingredient,
    COUNT(*) AS nb_references,
    COUNT(DISTINCT labeler_name) AS nb_laboratoires
FROM medicaments
WHERE active_ingredient IS NOT NULL AND active_ingredient != 'NONE'
GROUP BY active_ingredient
ORDER BY nb_references DESC
LIMIT 20;


-- 7. Controle qualite -- taux de completude par colonne
SELECT 'generic_name'       AS colonne, ROUND(100.0 * SUM(CASE WHEN generic_name IS NOT NULL AND generic_name != 'None' THEN 1 END) / COUNT(*), 1) AS completude_pct FROM medicaments
UNION ALL
SELECT 'brand_name',         ROUND(100.0 * SUM(CASE WHEN brand_name        IS NOT NULL AND brand_name != 'None'        THEN 1 END) / COUNT(*), 1) FROM medicaments
UNION ALL
SELECT 'active_ingredient',  ROUND(100.0 * SUM(CASE WHEN active_ingredient IS NOT NULL AND active_ingredient != 'None' THEN 1 END) / COUNT(*), 1) FROM medicaments
UNION ALL
SELECT 'dosage_form',        ROUND(100.0 * SUM(CASE WHEN dosage_form       IS NOT NULL AND dosage_form != 'None'       THEN 1 END) / COUNT(*), 1) FROM medicaments
UNION ALL
SELECT 'labeler_name',       ROUND(100.0 * SUM(CASE WHEN labeler_name      IS NOT NULL AND labeler_name != 'None'      THEN 1 END) / COUNT(*), 1) FROM medicaments
ORDER BY completude_pct DESC;
