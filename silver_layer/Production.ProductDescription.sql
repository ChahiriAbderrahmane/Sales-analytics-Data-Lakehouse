-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_productdescription  (SCD TYPE 2)
-- Clé       : ProductDescriptionID
-- Surveillé : Description
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_productdescription (
    product_description_id  INT,
    description             STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productdescription'
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
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW production_productdescription_ranked AS
SELECT
    CAST(ProductDescriptionID  AS INT)       AS product_description_id,
    CAST(Description           AS STRING)    AS description,
    CAST(rowguid               AS STRING)    AS rowguid,
    CAST(ModifiedDate          AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductDescriptionID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductDescriptionID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_productdescription
WHERE ProductDescriptionID IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les IDs à fermer
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW productdescription_ids_to_close AS
SELECT
    b.product_description_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productdescription_ranked b
JOIN silver.production_productdescription t
    ON  b.product_description_id = t.product_description_id
    AND t.is_current             = TRUE
WHERE b.description <> t.description
GROUP BY b.product_description_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_productdescription AS t
USING productdescription_ids_to_close AS s
ON  t.product_description_id = s.product_description_id
AND t.is_current             = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productdescription
SELECT
    b.product_description_id,
    b.description,
    b.rowguid,
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
        LAG(description) OVER (
            PARTITION BY product_description_id ORDER BY modified_date ASC
        ) AS prev_description,
        LAG(product_description_id) OVER (
            PARTITION BY product_description_id ORDER BY modified_date ASC
        ) AS prev_product_description_id
    FROM production_productdescription_ranked
) b
JOIN productdescription_ids_to_close s
    ON b.product_description_id = s.product_description_id
WHERE
    b.description          <> COALESCE(b.prev_description,           '')  OR
    b.product_description_id <> COALESCE(b.prev_product_description_id, 0);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productdescription
SELECT
    b.product_description_id,
    b.description,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_productdescription_ranked b
LEFT JOIN silver.production_productdescription t
    ON b.product_description_id = t.product_description_id
WHERE t.product_description_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_productdescription
ZORDER BY (product_description_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productdescription RETAIN 168 HOURS;