CREATE TABLE IF NOT EXISTS gold.dim_sales_territory (
    sales_territory_id     INT         NOT NULL,  -- business key
    sales_territory_name   STRING,
    sales_country_region_code    STRING,
    sales_group_name             STRING,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_sales_territory'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);
 
INSERT INTO gold.dim_sales_territory
SELECT
    CAST(territory_id AS INT) AS sales_territory_id, 
    CAST(name AS STRING) AS sales_territory_name,
    CAST(country_region_code AS STRING) AS sales_country_region_code,
    CAST(territory_group AS STRING) AS sales_group_name,
    current_timestamp() AS ingestion_timestamp
FROM silver.sales_salesterritory t1 
WHERE territory_id IS NOT NULL
        AND t1.is_current = true
        AND NOT EXISTS (
            SELECT 1 FROM gold.dim_sales_territory t2
            WHERE t2.sales_territory_id = t1.territory_id
        );