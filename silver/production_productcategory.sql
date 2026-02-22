-- ███████████████████████████████████████████████████████████████████████████
-- 2. production_productcategory — SCD TYPE 2
--    Clé       : ProductCategoryID
--    Surveillé : Name
-- ███████████████████████████████████████████████████████████████████████████

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

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.production_productcategory AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM production_productcategory_ranked b
        WHERE b.product_category_id = t.product_category_id
    )
WHERE t.is_current = TRUE
  AND t.product_category_id IN (SELECT DISTINCT product_category_id FROM production_productcategory_ranked)
  AND EXISTS (
      SELECT 1 FROM production_productcategory_ranked b
      WHERE b.product_category_id = t.product_category_id AND b.rn = 1
        AND b.name <> t.name
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.production_productcategory
SELECT
    b.product_category_id, b.name, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_productcategory_ranked b
WHERE b.product_category_id IN (
    SELECT DISTINCT b2.product_category_id
    FROM production_productcategory_ranked b2
    JOIN silver.production_productcategory t
        ON b2.product_category_id = t.product_category_id AND t.is_current = FALSE
    WHERE b2.rn = 1 AND b2.name <> t.name
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.production_productcategory
SELECT
    b.product_category_id, b.name, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_productcategory_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.production_productcategory t
    WHERE t.product_category_id = b.product_category_id
);

OPTIMIZE silver.production_productcategory ZORDER BY (product_category_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productcategory  RETAIN 2 HOURS;