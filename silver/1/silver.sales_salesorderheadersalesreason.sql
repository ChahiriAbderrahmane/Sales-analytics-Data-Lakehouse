-- 1. sales_salesorderheadersalesreason — INSERT UNIQUEMENT
--    Clé : SalesOrderID + SalesReasonID (clé composite)
--    Logique : table de liaison transactionnelle, une association créée
--              ne change jamais, on insère les nouvelles uniquement

CREATE TABLE IF NOT EXISTS silver.sales_salesorderheadersalesreason (
    sales_order_id          INT,
    sales_reason_id         INT,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesorderheadersalesreason'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

INSERT INTO silver.sales_salesorderheadersalesreason
SELECT
    CAST(SalesOrderID   AS INT)       AS sales_order_id,
    CAST(SalesReasonID  AS INT)       AS sales_reason_id,
    CAST(ModifiedDate   AS TIMESTAMP) AS modified_date,
    current_timestamp()               AS _ingestion_timestamp
FROM bronze.sales_salesorderheadersalesreason b
WHERE b.SalesOrderID  IS NOT NULL
  AND b.SalesReasonID IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.sales_salesorderheadersalesreason t
      WHERE t.sales_order_id  = b.SalesOrderID
        AND t.sales_reason_id = b.SalesReasonID
  );

OPTIMIZE silver.sales_salesorderheadersalesreason ZORDER BY (sales_order_id);
 

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesorderheadersalesreason  RETAIN 2 HOURS;