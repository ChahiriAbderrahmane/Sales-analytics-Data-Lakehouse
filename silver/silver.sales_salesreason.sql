-- ███████████████████████████████████████████████████████████████████████████
-- 2. sales_salesreason — SCD TYPE 2
--    Clé       : SalesReasonID
--    Surveillé : Name, ReasonType
-- ███████████████████████████████████████████████████████████████████████████

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

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.sales_salesreason AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM sales_salesreason_ranked b
        WHERE b.sales_reason_id = t.sales_reason_id
    )
WHERE t.is_current = TRUE
  AND t.sales_reason_id IN (SELECT DISTINCT sales_reason_id FROM sales_salesreason_ranked)
  AND EXISTS (
      SELECT 1 FROM sales_salesreason_ranked b
      WHERE b.sales_reason_id = t.sales_reason_id AND b.rn = 1
        AND (
            b.name        <> t.name        OR
            b.reason_type <> t.reason_type
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.sales_salesreason
SELECT
    b.sales_reason_id, b.name, b.reason_type, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_salesreason_ranked b
WHERE b.sales_reason_id IN (
    SELECT DISTINCT b2.sales_reason_id
    FROM sales_salesreason_ranked b2
    JOIN silver.sales_salesreason t
        ON b2.sales_reason_id = t.sales_reason_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.name        <> t.name        OR
          b2.reason_type <> t.reason_type
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.sales_salesreason
SELECT
    b.sales_reason_id, b.name, b.reason_type, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_salesreason_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.sales_salesreason t
    WHERE t.sales_reason_id = b.sales_reason_id
);

OPTIMIZE silver.sales_salesreason ZORDER BY (sales_reason_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesreason  RETAIN 2 HOURS;