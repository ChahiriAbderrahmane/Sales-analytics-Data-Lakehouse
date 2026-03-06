-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_productmodelproductdescriptionculture  (SCD TYPE 2)
-- Clé       : ProductModelID + ProductDescriptionID + CultureID
-- Surveillé : aucune colonne métier — SCD2 structurel (apparition/disparition)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_productmodelproductdescriptionculture (
    product_model_id            INT,
    product_description_id      INT,
    culture_id                  STRING,
    modified_date               TIMESTAMP,
    _ingestion_timestamp        TIMESTAMP,
    is_current                  BOOLEAN,
    end_date                    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productmodelproductdescriptionculture'
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
CREATE OR REPLACE TEMP VIEW production_pmpdc_ranked AS
SELECT
    CAST(productmodelid        AS INT)       AS product_model_id,
    CAST(productdescriptionid  AS INT)       AS product_description_id,
    CAST(culture               AS STRING)    AS culture_id,
    CAST(modifieddate          AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY productmodelid, productdescriptionid, culture
        ORDER BY modifieddate DESC
    ) AS rn,
    LEAD(CAST(modifieddate AS TIMESTAMP)) OVER (
        PARTITION BY productmodelid, productdescriptionid, culture
        ORDER BY modifieddate DESC
    ) AS next_version_date
FROM bronze.production_productmodelproductdescriptionculture
WHERE productmodelid        IS NOT NULL
  AND productdescriptionid  IS NOT NULL
  AND culture               IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les IDs à fermer
-- Aucune colonne métier surveillée → cette vue sera toujours vide
-- Conservée pour respecter le pattern et anticiper d'éventuels enrichissements
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW pmpdc_ids_to_close AS
SELECT
    b.product_model_id,
    b.product_description_id,
    b.culture_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_pmpdc_ranked b
JOIN silver.production_productmodelproductdescriptionculture t
    ON  b.product_model_id          = t.product_model_id
    AND b.product_description_id    = t.product_description_id
    AND b.culture_id                = t.culture_id
    AND t.is_current                = TRUE
WHERE 1 = 0  -- aucune colonne surveillée : jamais de fermeture par modification
GROUP BY b.product_model_id, b.product_description_id, b.culture_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- Ne s'exécutera jamais tant qu'aucune colonne métier n'est surveillée
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_productmodelproductdescriptionculture AS t
USING pmpdc_ids_to_close AS s
ON  t.product_model_id        = s.product_model_id
AND t.product_description_id  = s.product_description_id
AND t.culture_id              = s.culture_id
AND t.is_current              = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- Inopérante ici car pmpdc_ids_to_close est toujours vide
-- Conservée pour cohérence du pattern
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productmodelproductdescriptionculture
SELECT
    b.product_model_id,
    b.product_description_id,
    b.culture_id,
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
        LAG(product_model_id) OVER (
            PARTITION BY product_model_id, product_description_id, culture_id
            ORDER BY modified_date ASC
        ) AS prev_product_model_id
    FROM production_pmpdc_ranked
) b
JOIN pmpdc_ids_to_close s
    ON  b.product_model_id        = s.product_model_id
    AND b.product_description_id  = s.product_description_id
    AND b.culture_id              = s.culture_id
WHERE
    b.product_model_id <> COALESCE(b.prev_product_model_id, 0);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux liens (jamais vus dans silver)
-- C'est l'étape principale pour cette table
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productmodelproductdescriptionculture
SELECT
    b.product_model_id,
    b.product_description_id,
    b.culture_id,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_pmpdc_ranked b
LEFT JOIN silver.production_productmodelproductdescriptionculture t
    ON  b.product_model_id        = t.product_model_id
    AND b.product_description_id  = t.product_description_id
    AND b.culture_id              = t.culture_id
WHERE t.product_model_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_productmodelproductdescriptionculture
ZORDER BY (product_model_id, product_description_id, culture_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productmodelproductdescriptionculture RETAIN 168 HOURS;