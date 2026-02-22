-- ███████████████████████████████████████████████████████████████████████████
-- 1. sales_salesorderdetail — INSERT UNIQUEMENT (pas de SCD)
--    Clé : SalesOrderID + SalesOrderDetailID
--    Logique : une ligne achetée ne change jamais, on insère et c'est tout
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.sales_salesorderdetail (
    sales_order_id              INT,
    sales_order_detail_id       INT,
    carrier_tracking_number     STRING,
    order_qty                   SMALLINT,
    product_id                  INT,
    special_offer_id            INT,
    unit_price                  DECIMAL(19,4),
    unit_price_discount         DECIMAL(19,4),
    line_total                  DECIMAL(19,4),
    rowguid                     STRING,
    modified_date               TIMESTAMP,
    _ingestion_timestamp        TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesorderdetail'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- Insérer uniquement les lignes qui n'existent pas encore (clé composite)
INSERT INTO silver.sales_salesorderdetail
SELECT
    CAST(SalesOrderID           AS INT)            AS sales_order_id,
    CAST(SalesOrderDetailID     AS INT)            AS sales_order_detail_id,
    CAST(CarrierTrackingNumber  AS STRING)         AS carrier_tracking_number,
    CAST(OrderQty               AS SMALLINT)       AS order_qty,
    CAST(ProductID              AS INT)            AS product_id,
    CAST(SpecialOfferID         AS INT)            AS special_offer_id,
    ABS(CAST(UnitPrice          AS DECIMAL(19,4))) AS unit_price,
    ABS(CAST(UnitPriceDiscount  AS DECIMAL(19,4))) AS unit_price_discount,
    ABS(CAST(LineTotal          AS DECIMAL(19,4))) AS line_total,
    CAST(rowguid                AS STRING)         AS rowguid,
    CAST(ModifiedDate           AS TIMESTAMP)      AS modified_date,
    current_timestamp()                            AS _ingestion_timestamp
FROM bronze.sales_salesorderdetail b
WHERE b.SalesOrderID IS NOT NULL
  AND b.SalesOrderDetailID IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM silver.sales_salesorderdetail t
      WHERE t.sales_order_id       = b.SalesOrderID
        AND t.sales_order_detail_id = b.SalesOrderDetailID
  );

OPTIMIZE silver.sales_salesorderdetail ZORDER BY (sales_order_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesorderdetail  RETAIN 2 HOURS;