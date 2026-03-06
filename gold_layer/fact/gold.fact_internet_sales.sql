CREATE TABLE gold.fact_internet_sales (
    ProductKey              INT,
    OrderDateKey            INT,
    DueDateKey              INT,
    ShipDateKey             INT,
    CustomerKey             INT,
    PromotionKey            INT,
    CurrencyKey             STRING,  
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

-- ============================================================
-- ÉTAPE 1 : (Matérialisation du CDF)
-- ============================================================
DROP TABLE IF EXISTS gold.temp_fact_cdf_staging;

-- Faut l'updater manuellement à chaque fois avant le lancement de ce script
   -- kinda tricky

CREATE TABLE gold.temp_fact_cdf_staging USING DELTA AS 
SELECT * FROM table_changes('silver.sales_salesorderheader', 1)
WHERE _change_type IN ('insert', 'update_postimage');

-- ============================================================
-- ÉTAPE 2 : CACHE DE LA TABLE DE FAITS
-- ============================================================
CACHE TABLE fact_internet_sales_staging AS
SELECT
    dp.ProductKey,
    CAST(DATE_FORMAT(soh.order_date,  'yyyyMMdd') AS INT)   AS OrderDateKey,
    CAST(DATE_FORMAT(soh.due_date,    'yyyyMMdd') AS INT)   AS DueDateKey,
    CAST(DATE_FORMAT(soh.ship_date,   'yyyyMMdd') AS INT)   AS ShipDateKey,
    dc.CustomerKey,
    dpr.PromotionKey,
    dcu.currency_code                                       AS CurrencyKey,  
    dst.sales_territory_id                                  AS SalesTerritoryKey, 
    soh.sales_order_number                                  AS SalesOrderNumber,
    CAST(sod.sales_order_detail_id AS TINYINT)              AS SalesOrderLineNumber,
    soh.revision_number                                     AS RevisionNumber,  
    sod.order_qty                                           AS OrderQuantity,
    sod.unit_price                                          AS UnitPrice,
    sod.line_total                                          AS ExtendedAmount,
    sod.unit_price_discount                                 AS UnitPriceDiscountPct,
    ROUND(sod.unit_price * sod.unit_price_discount
          * sod.order_qty, 4)                               AS DiscountAmount,
    dp.StandardCost                                         AS ProductStandardCost,
    ROUND(dp.StandardCost * sod.order_qty, 4)               AS TotalProductCost,
    ROUND(sod.unit_price * (1 - sod.unit_price_discount)
          * sod.order_qty, 4)                               AS SalesAmount,
    ROUND(soh.tax_amt
          * (sod.line_total / soh.sub_total), 4)            AS TaxAmt,
    ROUND(soh.freight
          * (sod.line_total / soh.sub_total), 4)            AS Freight,
    sod.carrier_tracking_number                             AS CarrierTrackingNumber,
    soh.purchase_order_number                               AS CustomerPONumber,
    soh.order_date                                          AS OrderDate,
    soh.due_date                                            AS DueDate,
    soh.ship_date                                           AS ShipDate

FROM silver.sales_salesorderheader soh

INNER JOIN (
    SELECT DISTINCT sales_order_id
    FROM gold.temp_fact_cdf_staging
    WHERE online_order_flag = TRUE
) chg ON soh.sales_order_id = chg.sales_order_id

INNER JOIN silver.sales_salesorderdetail sod
    ON soh.sales_order_id = sod.sales_order_id

LEFT JOIN gold.dim_product dp
    ON sod.product_id = dp.ProductKey

LEFT JOIN gold.dim_customer dc
    ON soh.customer_id = dc.CustomerKey
    AND dc.is_current = TRUE

LEFT JOIN gold.dim_promotion dpr
    ON sod.special_offer_id = dpr.PromotionKey

LEFT JOIN silver.sales_currencyrate scr
    ON soh.currency_rate_id = scr.currency_rate_id

LEFT JOIN gold.dim_currency dcu
    ON scr.to_currency_code = dcu.currency_code  

INNER JOIN gold.dim_sales_territory dst
    ON soh.territory_id = dst.sales_territory_id  

WHERE soh.online_order_flag = TRUE
  AND soh.is_current = TRUE
  AND soh.sub_total > 0;

-- Insertion des données dans la fact_table avec logique de MERGE (UPDATE/INSERT)

MERGE INTO gold.fact_internet_sales tgt
USING fact_internet_sales_staging src
ON  tgt.SalesOrderNumber     = src.SalesOrderNumber
AND tgt.SalesOrderLineNumber = src.SalesOrderLineNumber

WHEN MATCHED THEN UPDATE SET
    tgt.ProductKey              = src.ProductKey,
    tgt.OrderDateKey            = src.OrderDateKey,
    tgt.DueDateKey              = src.DueDateKey,
    tgt.ShipDateKey             = src.ShipDateKey,
    tgt.CustomerKey             = src.CustomerKey,
    tgt.PromotionKey            = src.PromotionKey,
    tgt.CurrencyKey             = src.CurrencyKey,
    tgt.SalesTerritoryKey       = src.SalesTerritoryKey,
    tgt.RevisionNumber          = src.RevisionNumber,
    tgt.OrderQuantity           = src.OrderQuantity,
    tgt.UnitPrice               = src.UnitPrice,
    tgt.ExtendedAmount          = src.ExtendedAmount,
    tgt.UnitPriceDiscountPct    = src.UnitPriceDiscountPct,
    tgt.DiscountAmount          = src.DiscountAmount,
    tgt.ProductStandardCost     = src.ProductStandardCost,
    tgt.TotalProductCost        = src.TotalProductCost,
    tgt.SalesAmount             = src.SalesAmount,
    tgt.TaxAmt                  = src.TaxAmt,
    tgt.Freight                 = src.Freight,
    tgt.CarrierTrackingNumber   = src.CarrierTrackingNumber,
    tgt.CustomerPONumber        = src.CustomerPONumber,
    tgt.OrderDate               = src.OrderDate,
    tgt.DueDate                 = src.DueDate,
    tgt.ShipDate                = src.ShipDate,
    tgt.ingestion_timestamp     = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    ProductKey, OrderDateKey, DueDateKey, ShipDateKey,
    CustomerKey, PromotionKey, CurrencyKey, SalesTerritoryKey,
    SalesOrderNumber, SalesOrderLineNumber,
    RevisionNumber, OrderQuantity, UnitPrice, ExtendedAmount,
    UnitPriceDiscountPct, DiscountAmount,
    ProductStandardCost, TotalProductCost,
    SalesAmount, TaxAmt, Freight,
    CarrierTrackingNumber, CustomerPONumber,
    OrderDate, DueDate, ShipDate,
    ingestion_timestamp
) VALUES (
    src.ProductKey, src.OrderDateKey, src.DueDateKey, src.ShipDateKey,
    src.CustomerKey, src.PromotionKey, src.CurrencyKey, src.SalesTerritoryKey,
    src.SalesOrderNumber, src.SalesOrderLineNumber,
    src.RevisionNumber, src.OrderQuantity, src.UnitPrice, src.ExtendedAmount,
    src.UnitPriceDiscountPct, src.DiscountAmount,
    src.ProductStandardCost, src.TotalProductCost,
    src.SalesAmount, src.TaxAmt, src.Freight,
    src.CarrierTrackingNumber, src.CustomerPONumber,
    src.OrderDate, src.DueDate, src.ShipDate,
    current_timestamp()
);

-- ============================================================
-- ÉTAPE 4 : NETTOYER
-- ============================================================
UNCACHE TABLE fact_internet_sales_staging;
DROP TABLE IF EXISTS gold.temp_fact_cdf_staging;

-- Optimisation (ZORDER sur les clés les plus requêtées)
OPTIMIZE gold.fact_internet_sales ZORDER BY (CustomerKey, ProductKey);