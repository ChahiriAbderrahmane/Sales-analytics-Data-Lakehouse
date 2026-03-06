CREATE TABLE IF NOT EXISTS gold.dim_sales_reason (
    sales_reason_id        INT         NOT NULL,  -- business key
    sales_reason_name                    STRING,
    sales_reason_type             STRING,
    ingestion_timestamp     TIMESTAMP,
    sales_reason_state      BOOLEAN
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_sales_reason'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);
 
INSERT INTO gold.dim_sales_reason
SELECT
    CAST(sales_reason_id AS INT) AS sales_reason_id,
    CAST(name AS STRING) AS sales_reason_name,
    CAST(reason_type AS STRING) AS sales_reason_type,
    current_timestamp() AS ingestion_timestamp,
    is_current AS sales_reason_state
FROM silver.sales_salesreason 
WHERE sales_reason_id IS NOT NULL;