-- ============================================================
-- 2. dim_product_history (versions fermées)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.dim_product_history (
    ProductKey              INT             NOT NULL,
    ProductAlternateKey    STRING,
    ProductSubcategoryKey   INT,
    SizeUnitMeasureCode     STRING,
    WeightUnitMeasureCode   STRING,
    ProductName             STRING,
    StandardCost            DECIMAL(19,4),
    FinishedGoodsFlag       BOOLEAN,
    Color                   STRING,
    SafetyStockLevel        SMALLINT,
    ReorderPoint            SMALLINT,
    ListPrice               DECIMAL(19,4),
    Size                    STRING,
    Weight                  DOUBLE,
    DaysToManufacture       INT,
    ProductLine             STRING,
    Class                   STRING,
    Style                   STRING,
    ModelName               STRING,
    LargePhoto              BINARY,
    StartDate               TIMESTAMP,
    EndDate                 TIMESTAMP,
    Status                  STRING,
    valid_from              TIMESTAMP       NOT NULL,
    valid_to                TIMESTAMP       NOT NULL,
    closed_timestamp        TIMESTAMP       NOT NULL,
    ingestion_timestamp     TIMESTAMP,
    year_valid_to           INT
)
USING DELTA
PARTITIONED BY (year_valid_to)
LOCATION '/user/hadoop/sales_data_mart/gold/dim_product_history'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

-- Ce script je l'utilise dans dim_product.sql. Il est destiné à être communn à les deux!

-- CREATE OR REPLACE TEMP VIEW silver_product_enriched AS
-- SELECT
--     p.product_id                    AS ProductKey,
--     p.product_number                AS ProductNumber,
--     p.product_subcategory_id        AS ProductSubcategoryKey,
--     p.size_unit_measure_code        AS SizeUnitMeasureCode,
--     p.weight_unit_measure_code      AS WeightUnitMeasureCode,
--     p.name                          AS ProductName,
--     p.standard_cost                 AS StandardCost,
--     p.finished_goods_flag           AS FinishedGoodsFlag,
--     p.color                         AS Color,
--     p.safety_stock_level            AS SafetyStockLevel,
--     p.reorder_point                 AS ReorderPoint,
--     p.list_price                    AS ListPrice,
--     p.size                          AS Size,
--     p.weight                        AS Weight,
--     p.days_to_manufacture           AS DaysToManufacture,
--     p.product_line                  AS ProductLine,
--     p.class                         AS Class,
--     p.style                         AS Style,
--     pm.name                         AS ModelName,
--     ph.large_photo_file_name        AS LargePhoto,
--     p.sell_start_date               AS StartDate,
--     p.sell_end_date                 AS EndDate,
--     p.discontinued_date             AS Status,
--     p.modified_date                 AS valid_from,
--     p.is_current,
--     p._ingestion_timestamp
-- FROM silver.production_product p
-- LEFT JOIN silver.production_productmodel pm
--     ON p.product_model_id = pm.product_model_id       -- ✅ snake_case
--     AND pm.is_current = TRUE
-- LEFT JOIN silver.production_productproductphoto ppp
--     ON p.product_id = ppp.product_id                  -- ✅ snake_case
--     AND ppp.is_current = TRUE
-- LEFT JOIN silver.production_productphoto ph
--     ON ppp.product_photo_id = ph.product_photo_id;    -- ✅ snake_case


INSERT INTO gold.dim_product_history
SELECT
    ProductKey,
    ProductNumber           AS ProductAlternateKey,
    ProductSubcategoryKey,
    SizeUnitMeasureCode,
    WeightUnitMeasureCode,
    ProductName,
    StandardCost,
    FinishedGoodsFlag,
    Color,
    SafetyStockLevel,
    ReorderPoint,
    ListPrice,
    Size,
    Weight,
    DaysToManufacture,
    ProductLine,
    Class,
    Style,
    ModelName,
    LargePhoto,
    StartDate,
    EndDate,
    Status,
    COALESCE(valid_from, _ingestion_timestamp)  AS valid_from, 
    current_timestamp()         AS valid_to,
    current_timestamp()         AS closed_timestamp,
    current_timestamp()         AS ingestion_timestamp,
    YEAR(current_timestamp())   AS year_valid_to 
FROM silver_product_enriched
WHERE is_current = FALSE;


OPTIMIZE gold.dim_product_history ZORDER BY (ProductKey, valid_from);