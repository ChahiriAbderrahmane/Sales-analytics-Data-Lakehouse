-- 3. production_productsubcategory — SCD TYPE 2
--    Clé       : ProductSubcategoryID
--    Surveillé : Name, ProductCategoryID

CREATE TABLE IF NOT EXISTS silver.production_productsubcategory (
    product_subcategory_id  INT,
    product_category_id     INT,
    name                    STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productsubcategory'
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
CREATE OR REPLACE TEMP VIEW production_productsubcategory_ranked AS
SELECT
    CAST(ProductSubcategoryID   AS INT)       AS product_subcategory_id,
    CAST(ProductCategoryID      AS INT)       AS product_category_id,
    CAST(Name                   AS STRING)    AS name,
    CAST(rowguid                AS STRING)    AS rowguid,
    CAST(ModifiedDate           AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductSubcategoryID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductSubcategoryID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_productsubcategory
WHERE ProductSubcategoryID IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.product_subcategory_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productsubcategory_ranked b
JOIN silver.production_productsubcategory t
    ON  b.product_subcategory_id = t.product_subcategory_id
    AND t.is_current = TRUE
WHERE b.name                <> t.name
   OR b.product_category_id <> t.product_category_id
GROUP BY b.product_subcategory_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.production_productsubcategory AS t
USING ids_to_close AS s
ON  t.product_subcategory_id = s.product_subcategory_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.production_productsubcategory
SELECT
    b.product_subcategory_id,
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
        LAG(name)     OVER (PARTITION BY product_subcategory_id ORDER BY modified_date ASC) AS prev_name,
        LAG(product_category_id)   OVER (PARTITION BY product_subcategory_id ORDER BY modified_date ASC) AS prev_product_category_id
    FROM production_productsubcategory_ranked
) b
JOIN ids_to_close s ON b.product_subcategory_id = s.product_subcategory_id
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
INSERT INTO silver.production_productsubcategory
SELECT
    b.product_subcategory_id,
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
FROM production_productsubcategory_ranked b
LEFT JOIN silver.production_productsubcategory t
    ON b.product_subcategory_id = t.product_subcategory_id
WHERE t.product_subcategory_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.production_productsubcategory
ZORDER BY (product_category_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productsubcategory RETAIN 48 HOURS;
