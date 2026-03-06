CREATE TABLE IF NOT EXISTS gold.dim_product_sub_category (
    ProductSubCategoryKey  INT         NOT NULL,  -- business key
    ProductSubCategoryName STRING,
    ProductCategoryKey     INT,
    IngestionTimestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_product_sub_category'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',    
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

INSERT INTO gold.dim_product_sub_category
SELECT 
    CAST(product_subcategory_id AS INT) AS ProductSubCategoryKey,
    CAST(name AS STRING) AS ProductSubCategoryName,
    CAST(product_category_id AS INT) AS ProductCategoryKey,
    current_timestamp() AS IngestionTimestamp 
FROM silver.production_productsubcategory t1
WHERE product_subcategory_id IS NOT NULL
  AND t1.is_current = true
  AND NOT EXISTS (
      SELECT 1 FROM gold.dim_product_sub_category t
      WHERE t.ProductSubCategoryKey = t1.product_subcategory_id
  );

  product_subcategory_id