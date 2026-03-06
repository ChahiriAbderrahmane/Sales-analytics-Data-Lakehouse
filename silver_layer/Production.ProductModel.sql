-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_productmodel
-- SCD2 : Name, CatalogDescription
-- Append + dernière valeur : Instructions (rn=1 systématiquement)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_productmodel (
    product_model_id        INT,
    name                    STRING,
    catalog_description     STRING,
    instructions            STRING,   -- toujours la valeur la plus récente au moment de l'insert
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productmodel'
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
CREATE OR REPLACE TEMP VIEW production_productmodel_ranked AS
SELECT
    CAST(ProductModelID       AS INT)       AS product_model_id,
    CAST(Name                 AS STRING)    AS name,
    CAST(CatalogDescription   AS STRING)    AS catalog_description,
    CAST(Instructions         AS STRING)    AS instructions,
    CAST(rowguid              AS STRING)    AS rowguid,
    CAST(ModifiedDate         AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductModelID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductModelID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_productmodel
WHERE ProductModelID IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 0b : Dernière valeur instructions par product_model_id (rn = 1)
-- Réutilisée dans étape 3 et 4 pour éviter la duplication
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW productmodel_latest_instructions AS
SELECT
    product_model_id,
    instructions
FROM production_productmodel_ranked
WHERE rn = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les lignes à fermer (SCD2 uniquement)
-- Comparaison sur Name et CatalogDescription — Instructions ignoré
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW productmodel_ids_to_close AS
SELECT
    b.product_model_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productmodel_ranked b
JOIN silver.production_productmodel t
    ON  b.product_model_id = t.product_model_id
    AND t.is_current       = TRUE
WHERE
    b.name <> t.name
    OR
    regexp_replace(TRIM(COALESCE(b.catalog_description, '')), '>\\s+<', '><')
    <>
    regexp_replace(TRIM(COALESCE(t.catalog_description, '')), '>\\s+<', '><')
GROUP BY b.product_model_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_productmodel AS t
USING productmodel_ids_to_close AS s
ON  t.product_model_id = s.product_model_id
AND t.is_current       = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- instructions = dernière valeur bronze via productmodel_latest_instructions
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productmodel
SELECT
    b.product_model_id,
    b.name,
    b.catalog_description,
    li.instructions,                                -- dernière valeur (rn=1)
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
        LAG(name) OVER (
            PARTITION BY product_model_id ORDER BY modified_date ASC
        ) AS prev_name,
        LAG(catalog_description) OVER (
            PARTITION BY product_model_id ORDER BY modified_date ASC
        ) AS prev_catalog_description,
        LAG(product_model_id) OVER (
            PARTITION BY product_model_id ORDER BY modified_date ASC
        ) AS prev_product_model_id
    FROM production_productmodel_ranked
) b
JOIN productmodel_latest_instructions li
    ON b.product_model_id = li.product_model_id
JOIN productmodel_ids_to_close s
    ON b.product_model_id = s.product_model_id
WHERE
    b.name <> COALESCE(b.prev_name, '')
    OR
    regexp_replace(TRIM(COALESCE(b.catalog_description,      '')), '>\\s+<', '><')
    <>
    regexp_replace(TRIM(COALESCE(b.prev_catalog_description, '')), '>\\s+<', '><')
    OR
    b.product_model_id <> COALESCE(b.prev_product_model_id, 0);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- instructions = dernière valeur bronze via productmodel_latest_instructions
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productmodel
SELECT
    b.product_model_id,
    b.name,
    b.catalog_description,
    li.instructions,                                -- dernière valeur (rn=1)
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_productmodel_ranked b
JOIN productmodel_latest_instructions li
    ON b.product_model_id = li.product_model_id
LEFT JOIN silver.production_productmodel t
    ON b.product_model_id = t.product_model_id
WHERE t.product_model_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_productmodel
ZORDER BY (product_model_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productmodel RETAIN 168 HOURS;