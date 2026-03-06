-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_productproductphoto  (SCD TYPE 2)
-- Clé       : ProductID + ProductPhotoID
-- Surveillé : Primary
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_productproductphoto (
    product_id              INT,
    product_photo_id        INT,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productproductphoto'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 0 : Classer les lignes bronze
-- Colonnes réelles après normalisation HiveQL
-- Primary absent de bronze — table réduite à clé + modifieddate
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW production_productproductphoto_ranked AS
SELECT
    CAST(productid       AS INT)       AS product_id,
    CAST(productphotoid  AS INT)       AS product_photo_id,
    CAST(modifieddate    AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY productid, productphotoid
        ORDER BY modifieddate DESC
    ) AS rn,
    LEAD(CAST(modifieddate AS TIMESTAMP)) OVER (
        PARTITION BY productid, productphotoid
        ORDER BY modifieddate DESC
    ) AS next_version_date
FROM bronze.production_productproductphoto
WHERE productid      IS NOT NULL
  AND productphotoid IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : ids_to_close — toujours vide (aucune colonne surveillée)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW productproductphoto_ids_to_close AS
SELECT
    b.product_id,
    b.product_photo_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productproductphoto_ranked b
JOIN silver.production_productproductphoto t
    ON  b.product_id       = t.product_id
    AND b.product_photo_id = t.product_photo_id
    AND t.is_current       = TRUE
WHERE 1 = 0
GROUP BY b.product_id, b.product_photo_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer — inopérante, conservée pour cohérence
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_productproductphoto AS t
USING productproductphoto_ids_to_close AS s
ON  t.product_id       = s.product_id
AND t.product_photo_id = s.product_photo_id
AND t.is_current       = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Inopérante, conservée pour cohérence
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productproductphoto
SELECT
    b.product_id,
    b.product_photo_id,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM (
    SELECT
        *,
        LAG(product_id) OVER (
            PARTITION BY product_id, product_photo_id ORDER BY modified_date ASC
        ) AS prev_product_id
    FROM production_productproductphoto_ranked
) b
JOIN productproductphoto_ids_to_close s
    ON  b.product_id       = s.product_id
    AND b.product_photo_id = s.product_photo_id
WHERE b.product_id <> COALESCE(b.prev_product_id, 0);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux liens (étape principale)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productproductphoto
SELECT
    b.product_id,
    b.product_photo_id,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_productproductphoto_ranked b
LEFT JOIN silver.production_productproductphoto t
    ON  b.product_id       = t.product_id
    AND b.product_photo_id = t.product_photo_id
WHERE t.product_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_productproductphoto
ZORDER BY (product_id, product_photo_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productproductphoto RETAIN 168 HOURS;