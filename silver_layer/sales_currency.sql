-- 4. sales_currency — INSERT UNIQUEMENT
--    Clé : CurrencyCode (STRING)
--    Logique : une devise ne change pas, on insère les nouveaux codes seulement


CREATE TABLE IF NOT EXISTS silver.sales_currency (
    currency_code           STRING,
    name                    STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_currency'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);
 
INSERT INTO silver.sales_currency
SELECT
    CAST(CurrencyCode   AS STRING)    AS currency_code,
    CAST(Name           AS STRING)    AS name,
    CAST(ModifiedDate   AS TIMESTAMP) AS modified_date,
    current_timestamp()               AS _ingestion_timestamp
FROM bronze.sales_currency b
WHERE b.CurrencyCode IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.sales_currency t
      WHERE t.currency_code = b.CurrencyCode
  );

OPTIMIZE silver.sales_currency ZORDER BY (currency_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_currency  RETAIN 2 HOURS;
