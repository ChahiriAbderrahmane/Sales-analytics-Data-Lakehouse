CREATE TABLE IF NOT EXISTS gold.dim_currency(
    currency_code          STRING    NOT NULL,
    currency_name                   STRING,
    ingestion_timestamp    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_currency'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

INSERT INTO gold.dim_currency
SELECT
    currency_code as currency_code,
    name as currency_name,
    current_timestamp() AS ingestion_timestamp
FROM silver.sales_currency
WHERE currency_code IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM gold.dim_currency t
      WHERE t.currency_code = silver.sales_currency.currency_code
  );
