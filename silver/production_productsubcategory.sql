-- ███████████████████████████████████████████████████████████████████████████
-- 3. production_productsubcategory — SCD TYPE 2
--    Clé       : ProductSubcategoryID
--    Surveillé : Name, ProductCategoryID
-- ███████████████████████████████████████████████████████████████████████████

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
UPDATE silver.production_productsubcategory AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM production_productsubcategory_ranked b
        WHERE b.product_subcategory_id = t.product_subcategory_id
    )
WHERE t.is_current = TRUE
  AND t.product_subcategory_id IN (SELECT DISTINCT product_subcategory_id FROM production_productsubcategory_ranked)
  AND EXISTS (
      SELECT 1 FROM production_productsubcategory_ranked b
      WHERE b.product_subcategory_id = t.product_subcategory_id AND b.rn = 1
        AND (
            b.name                <> t.name                OR
            b.product_category_id <> t.product_category_id
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.production_productsubcategory
SELECT
    b.product_subcategory_id, b.product_category_id, b.name, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_productsubcategory_ranked b
WHERE b.product_subcategory_id IN (
    SELECT DISTINCT b2.product_subcategory_id
    FROM production_productsubcategory_ranked b2
    JOIN silver.production_productsubcategory t
        ON b2.product_subcategory_id = t.product_subcategory_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.name                <> t.name                OR
          b2.product_category_id <> t.product_category_id
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.production_productsubcategory
SELECT
    b.product_subcategory_id, b.product_category_id, b.name, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_productsubcategory_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.production_productsubcategory t
    WHERE t.product_subcategory_id = b.product_subcategory_id
);

OPTIMIZE silver.production_productsubcategory ZORDER BY (product_category_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productsubcategory  RETAIN 2 HOURS;