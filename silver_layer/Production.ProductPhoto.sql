-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_productphoto
-- Clé       : ProductPhotoID
-- Surveillé : aucun — append + dernière valeur
-- Exclus    : ThumbNailPhoto, LargePhoto (binaires trop lourds)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_productphoto (
    product_photo_id            INT,
    thumbnail_photo_file_name   STRING,
    large_photo_file_name       STRING,
    modified_date               TIMESTAMP,
    _ingestion_timestamp        TIMESTAMP,
    is_current                  BOOLEAN,
    end_date                    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_productphoto'
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
-- ThumbNailPhoto et LargePhoto intentionnellement exclus
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW production_productphoto_ranked AS
SELECT
    CAST(ProductPhotoID             AS INT)       AS product_photo_id,
    CAST(ThumbnailPhotoFileName     AS STRING)    AS thumbnail_photo_file_name,
    CAST(LargePhotoFileName         AS STRING)    AS large_photo_file_name,
    CAST(ModifiedDate               AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductPhotoID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductPhotoID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_productphoto
WHERE ProductPhotoID IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les IDs à fermer
-- WHERE 1 = 0 — aucune colonne surveillée, jamais de fermeture
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW productphoto_ids_to_close AS
SELECT
    b.product_photo_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_productphoto_ranked b
JOIN silver.production_productphoto t
    ON  b.product_photo_id = t.product_photo_id
    AND t.is_current       = TRUE
WHERE 1 = 0
GROUP BY b.product_photo_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives — inopérante, conservée pour cohérence
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_productphoto AS t
USING productphoto_ids_to_close AS s
ON  t.product_photo_id = s.product_photo_id
AND t.is_current       = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions — inopérante, conservée pour cohérence
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productphoto
SELECT
    b.product_photo_id,
    b.thumbnail_photo_file_name,
    b.large_photo_file_name,
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
        LAG(product_photo_id) OVER (
            PARTITION BY product_photo_id ORDER BY modified_date ASC
        ) AS prev_product_photo_id
    FROM production_productphoto_ranked
) b
JOIN productphoto_ids_to_close s
    ON b.product_photo_id = s.product_photo_id
WHERE
    b.product_photo_id <> COALESCE(b.prev_product_photo_id, 0);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- C'est l'étape principale pour cette table
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_productphoto
SELECT
    b.product_photo_id,
    b.thumbnail_photo_file_name,
    b.large_photo_file_name,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_productphoto_ranked b
LEFT JOIN silver.production_productphoto t
    ON b.product_photo_id = t.product_photo_id
WHERE t.product_photo_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_productphoto
ZORDER BY (product_photo_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_productphoto RETAIN 168 HOURS;