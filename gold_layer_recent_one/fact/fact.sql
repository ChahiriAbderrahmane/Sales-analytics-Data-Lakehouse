-- ============================================================
-- 1. CREATE TABLE fact_internet_sales
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.fact_internet_sales (
    ProductKey              INT,
    OrderDateKey            INT,
    DueDateKey              INT,
    ShipDateKey             INT,
    CustomerKey             INT,
    PromotionKey            INT,
    CurrencyKey             INT,
    SalesTerritoryKey       INT,
    SalesOrderNumber        STRING      NOT NULL,
    SalesOrderLineNumber    TINYINT     NOT NULL,
    RevisionNumber          TINYINT,
    OrderQuantity           SMALLINT,
    UnitPrice               DECIMAL(19,4),
    ExtendedAmount          DECIMAL(19,4),
    UnitPriceDiscountPct    DOUBLE,
    DiscountAmount          DOUBLE,
    ProductStandardCost     DECIMAL(19,4),
    TotalProductCost        DECIMAL(19,4),
    SalesAmount             DECIMAL(19,4),
    TaxAmt                  DECIMAL(19,4),
    Freight                 DECIMAL(19,4),
    CarrierTrackingNumber   STRING,
    CustomerPONumber        STRING,
    OrderDate               TIMESTAMP,
    DueDate                 TIMESTAMP,
    ShipDate                TIMESTAMP,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
-- Partitionner par année de commande pour le partition pruning
PARTITIONED BY (OrderDateKey)
LOCATION '/user/hadoop/sales_data_mart/gold/fact_internet_sales'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

-- 1. Matérialisation physique du CDF (Évite le bug Catalyst)
DROP TABLE IF EXISTS gold.temp_fact_cdf;

CREATE TABLE gold.temp_fact_cdf USING DELTA AS 
SELECT * FROM table_changes('silver.sales_salesorderheader', 1)
WHERE _change_type IN ('insert', 'update_postimage');

-- 2. CACHE de la table enrichie
CACHE TABLE fact_internet_sales_staging AS
SELECT
    sod.product_id                                          AS ProductKey,
    CAST(DATE_FORMAT(soh.order_date,  'yyyyMMdd') AS INT)   AS OrderDateKey,
    CAST(DATE_FORMAT(soh.due_date,    'yyyyMMdd') AS INT)   AS DueDateKey,
    CAST(DATE_FORMAT(soh.ship_date,   'yyyyMMdd') AS INT)   AS ShipDateKey,
    soh.customer_id                                         AS CustomerKey,
    sod.special_offer_id                                    AS PromotionKey,
    soh.currency_rate_id                                    AS CurrencyKey,
    soh.territory_id                                        AS SalesTerritoryKey,
    soh.sales_order_number                                  AS SalesOrderNumber,
    sod.sales_order_detail_id                               AS SalesOrderLineNumber,
    sod.revision_number                                     AS RevisionNumber,
    sod.order_qty                                           AS OrderQuantity,
    sod.unit_price                                          AS UnitPrice,
    sod.line_total                                          AS ExtendedAmount,
    sod.unit_price_discount                                 AS UnitPriceDiscountPct,
    ROUND(sod.unit_price * sod.unit_price_discount * sod.order_qty, 4) AS DiscountAmount,
    pp.standard_cost                                        AS ProductStandardCost,
    ROUND(pp.standard_cost * sod.order_qty, 4)              AS TotalProductCost,
    ROUND(sod.unit_price * (1 - sod.unit_price_discount) * sod.order_qty, 4) AS SalesAmount,
    ROUND(soh.tax_amt * (sod.line_total / soh.sub_total), 4) AS TaxAmt,
    ROUND(soh.freight * (sod.line_total / soh.sub_total), 4) AS Freight,
    sod.carrier_tracking_number                             AS CarrierTrackingNumber,
    soh.purchase_order_number                               AS CustomerPONumber,
    soh.order_date                                          AS OrderDate,
    soh.due_date                                            AS DueDate,
    soh.ship_date                                           AS ShipDate,
    current_timestamp()                                     AS ingestion_timestamp
FROM gold.temp_fact_cdf chg
INNER JOIN silver.sales_salesorderheader soh 
    ON chg.sales_order_id = soh.sales_order_id
INNER JOIN silver.sales_salesorderdetail sod 
    ON soh.sales_order_id = sod.sales_order_id
LEFT JOIN silver.production_product pp 
    ON sod.product_id = pp.product_id AND pp.is_current = TRUE
WHERE soh.online_order_flag = TRUE AND soh.is_current = TRUE;

-- 3. MERGE (Ton code d'origine était parfait ici)
MERGE INTO gold.fact_internet_sales tgt
USING fact_internet_sales_staging src
ON  tgt.SalesOrderNumber     = src.SalesOrderNumber
AND tgt.SalesOrderLineNumber = src.SalesOrderLineNumber
WHEN MATCHED THEN UPDATE SET * -- Optionnel : UPDATE SET * met à jour toutes les colonnes automatiquement pour simplifier le code
WHEN NOT MATCHED THEN INSERT *;

-- 4. UPDATE Pipeline Control (Utilisation de la variable)
MERGE INTO gold.pipeline_control tgt
USING (SELECT 'silver.sales_salesorderheader' AS table_name) src
ON tgt.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET 
    tgt.last_version = CAST(${hivevar:CURRENT_VERSION} AS BIGINT), 
    tgt.last_run_timestamp = current_timestamp();

-- 5. CLEANUP & OPTIMIZE (Sans la colonne de partition dans le ZORDER)
UNCACHE TABLE fact_internet_sales_staging;
DROP TABLE IF EXISTS gold.temp_fact_cdf;
OPTIMIZE gold.fact_internet_sales ZORDER BY (CustomerKey, ProductKey);

