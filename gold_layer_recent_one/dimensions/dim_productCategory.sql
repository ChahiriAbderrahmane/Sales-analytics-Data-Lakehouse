CREATE TABLE IF NOT EXISTS gold.dim_product_category (
    ProductCategoryKey     INT         NOT NULL,  -- business key
    ProductCategoryName    STRING,
    IngestionTimestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_product_category'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',   
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

INSERT INTO gold.dim_product_category
SELECT
    CAST(product_category_id AS INT) AS ProductCategoryKey,
    CAST(name AS STRING) AS ProductCategoryName,
    current_timestamp() AS IngestionTimestamp
FROM silver.production_productcategory t1
WHERE product_category_id IS NOT NULL
  AND t1.is_current = true
  AND NOT EXISTS (
      SELECT 1 FROM gold.dim_product_category t
      WHERE t.ProductCategoryKey = t1.product_category_id
  );

product_category_id