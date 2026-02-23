-- 2. sales_salesreason — SCD TYPE 2
--    Clé       : SalesReasonID
--    Surveillé : Name, ReasonType

CREATE TABLE IF NOT EXISTS silver.sales_salesreason (
    sales_reason_id         INT,
    name                    STRING,
    reason_type             STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesreason'
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
CREATE OR REPLACE TEMP VIEW sales_salesreason_ranked AS
SELECT
    CAST(SalesReasonID  AS INT)       AS sales_reason_id,
    CAST(Name           AS STRING)    AS name,
    CAST(ReasonType     AS STRING)    AS reason_type,
    CAST(ModifiedDate   AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY SalesReasonID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY SalesReasonID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.sales_salesreason
WHERE SalesReasonID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- IS DISTINCT FROM remplace <> pour gérer correctement les valeurs NULL
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.sales_reason_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM sales_salesreason_ranked b
JOIN silver.sales_salesreason t
    ON  b.sales_reason_id = t.sales_reason_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.name        IS DISTINCT FROM t.name
   OR b.reason_type IS DISTINCT FROM t.reason_type
GROUP BY b.sales_reason_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.sales_salesreason AS t
USING ids_to_close AS s
ON  t.sales_reason_id = s.sales_reason_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_salesreason
SELECT
    b.sales_reason_id,
    b.name,
    b.reason_type,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_salesreason_ranked b
JOIN ids_to_close s
    ON b.sales_reason_id = s.sales_reason_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_salesreason
SELECT
    b.sales_reason_id,
    b.name,
    b.reason_type,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_salesreason_ranked b
LEFT JOIN silver.sales_salesreason t
    ON b.sales_reason_id = t.sales_reason_id
WHERE t.sales_reason_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.sales_salesreason
ZORDER BY (sales_reason_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesreason RETAIN 168 HOURS;