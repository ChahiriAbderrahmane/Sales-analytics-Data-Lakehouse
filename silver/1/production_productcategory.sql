-- 2. production_productcategory — SCD TYPE 2
--    Clé       : ProductCategoryID
--    Surveillé : Name

CREATE TABLE IF NOT EXISTS silver.production_productcategory (
    product_category_id     INT,
    name                    STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productcategory'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- ÉTAPE 0 : Classer les lignes bronze
CREATE OR REPLACE TEMP VIEW production_productcategory_ranked AS
SELECT
    CAST(ProductCategoryID  AS INT)       AS product_category_id,
    CAST(Name               AS STRING)    AS name,
    CAST(rowguid            AS STRING)    AS rowguid,
    CAST(ModifiedDate       AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductCategoryID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductCategoryID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_productcategory
WHERE ProductCategoryID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- Matérialisé en TEMP VIEW pour éviter les sous-requêtes dans le MERGE
-- (limitation Delta open source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.product_category_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productcategory_ranked b
JOIN silver.production_productcategory t
    ON  b.product_category_id = t.product_category_id
    AND t.is_current = TRUE
WHERE b.name <> t.name
GROUP BY b.product_category_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.production_productcategory AS t
USING ids_to_close AS s
ON  t.product_category_id = s.product_category_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.production_productcategory
SELECT
    b.product_category_id,
    b.name,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM (
    -- On ranke chronologiquement (ASC) pour calculer le précédent état
    SELECT 
        *,
        LAG(name)     OVER (PARTITION BY product_category_id ORDER BY modified_date ASC) AS prev_name,
        LAG(product_category_id)   OVER (PARTITION BY product_category_id ORDER BY modified_date ASC) AS prev_product_category_id
    FROM production_productcategory_ranked
) b
JOIN ids_to_close s ON b.product_category_id = s.product_category_id
WHERE 
    -- On insère seulement si cette ligne diffère de la précédente (dans le batch ou de silver)
    (b.name                <> COALESCE(b.prev_name,                '')     OR
     b.product_category_id <> COALESCE(b.prev_product_category_id, 0))
    -- OU si c'est la première ligne du batch et qu'elle diffère de silver (déjà couvert par ids_to_close)
;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.production_productcategory
SELECT
    b.product_category_id,
    b.name,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_productcategory_ranked b
LEFT JOIN silver.production_productcategory t
    ON b.product_category_id = t.product_category_id
WHERE t.product_category_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.production_productcategory
ZORDER BY (product_category_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productcategory RETAIN 168 HOURS;