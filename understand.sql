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


-- ============================================================
-- 2. Initialisation pipeline_control pour la fact
-- ============================================================
INSERT INTO gold.pipeline_control
SELECT
    'silver.sales_salesorderheader'     AS table_name,
    0                                   AS last_version,
    current_timestamp()                 AS last_run_timestamp
WHERE NOT EXISTS (
    SELECT 1 FROM gold.pipeline_control
    WHERE table_name = 'silver.sales_salesorderheader'
);


-- ============================================================
-- 3. CACHE du résultat enrichi via CDF
--    Seules les commandes nouvelles/modifiées depuis le dernier run
-- ============================================================
CACHE TABLE fact_internet_sales_staging AS
WITH cdf_changes AS (
    -- Récupérer uniquement les SalesOrderID qui ont changé
    SELECT DISTINCT sales_order_id
    FROM table_changes(
        'silver.sales_salesorderheader',
        (SELECT last_version + 1
         FROM gold.pipeline_control
         WHERE table_name = 'silver.sales_salesorderheader')
    )
    WHERE _change_type IN ('insert', 'update_postimage')
      AND online_order_flag = TRUE   -- Internet Sales uniquement
)
SELECT
    -- FK vers les dimensions
    sod.product_id                                          AS ProductKey,

    -- DateKey = YYYYMMDD (INT) pour jointure avec DimDate
    CAST(DATE_FORMAT(soh.order_date,  'yyyyMMdd') AS INT)   AS OrderDateKey,
    CAST(DATE_FORMAT(soh.due_date,    'yyyyMMdd') AS INT)   AS DueDateKey,
    CAST(DATE_FORMAT(soh.ship_date,   'yyyyMMdd') AS INT)   AS ShipDateKey,

    soh.customer_id                                         AS CustomerKey,
    sod.special_offer_id                                    AS PromotionKey,
    soh.currency_rate_id                                    AS CurrencyKey,
    soh.territory_id                                        AS SalesTerritoryKey,

    -- PK composite
    soh.sales_order_number                                  AS SalesOrderNumber,
    sod.sales_order_detail_id                               AS SalesOrderLineNumber,

    -- Mesures
    sod.revision_number                                     AS RevisionNumber,
    sod.order_qty                                           AS OrderQuantity,
    sod.unit_price                                          AS UnitPrice,
    sod.line_total                                          AS ExtendedAmount,
    sod.unit_price_discount                                 AS UnitPriceDiscountPct,

    -- DiscountAmount calculé
    ROUND(sod.unit_price * sod.unit_price_discount * sod.order_qty, 4)
                                                            AS DiscountAmount,

    -- StandardCost depuis silver.production_product
    pp.standard_cost                                        AS ProductStandardCost,

    -- TotalProductCost = StandardCost × OrderQty
    ROUND(pp.standard_cost * sod.order_qty, 4)              AS TotalProductCost,

    -- SalesAmount = UnitPrice × (1 - Discount) × Qty
    ROUND(sod.unit_price * (1 - sod.unit_price_discount) * sod.order_qty, 4)
                                                            AS SalesAmount,

    -- Répartition TaxAmt et Freight au prorata de la ligne
    ROUND(soh.tax_amt * (sod.line_total / soh.sub_total), 4)
                                                            AS TaxAmt,
    ROUND(soh.freight * (sod.line_total / soh.sub_total), 4)
                                                            AS Freight,

    sod.carrier_tracking_number                             AS CarrierTrackingNumber,
    soh.purchase_order_number                               AS CustomerPONumber,
    soh.order_date                                          AS OrderDate,
    soh.due_date                                            AS DueDate,
    soh.ship_date                                           AS ShipDate

FROM silver.sales_salesorderheader soh
-- JOIN uniquement sur les commandes changées → pas de full scan
INNER JOIN cdf_changes chg
    ON soh.sales_order_id = chg.sales_order_id
-- Lignes de détail
INNER JOIN silver.sales_salesorderdetail sod
    ON soh.sales_order_id = sod.sales_order_id
-- StandardCost du produit
LEFT JOIN silver.production_product pp
    ON sod.product_id = pp.product_id
    AND pp.is_current = TRUE
WHERE soh.online_order_flag = TRUE
  AND soh.is_current = TRUE;


-- ============================================================
-- 4. MERGE dans fact_internet_sales
--    PK composite : SalesOrderNumber + SalesOrderLineNumber
-- ============================================================
MERGE INTO gold.fact_internet_sales tgt
USING fact_internet_sales_staging src
ON  tgt.SalesOrderNumber     = src.SalesOrderNumber
AND tgt.SalesOrderLineNumber = src.SalesOrderLineNumber

-- Ligne existante modifiée (ex: révision, annulation)
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

-- Nouvelle ligne → insertion
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
-- 5. Mettre à jour pipeline_control
-- ============================================================
MERGE INTO gold.pipeline_control tgt
USING (
    SELECT
        'silver.sales_salesorderheader'     AS table_name,
        (SELECT MAX(version)
         FROM (DESCRIBE HISTORY silver.sales_salesorderheader))
                                            AS last_version,
        current_timestamp()                 AS last_run_timestamp
) src
ON tgt.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET
    tgt.last_version        = src.last_version,
    tgt.last_run_timestamp  = src.last_run_timestamp;


-- ============================================================
-- 6. Libérer le cache et optimiser
-- ============================================================
UNCACHE TABLE fact_internet_sales_staging;

-- ZORDER sur les colonnes les plus filtrées dans les requêtes analytiques
OPTIMIZE gold.fact_internet_sales
ZORDER BY (CustomerKey, ProductKey, OrderDateKey);