-- I hope if they want to change a promotion, they will create a new promotion instead of updating the existing one. 
-- If they update the existing promotion, I will just update the current version in this dimension table.


-- I'm tired bro lol


CREATE TABLE IF NOT EXISTS gold.dim_promotion (
    PromotionKey  INT         NOT NULL,  -- business key
    PromotionDescription   STRING,
    DiscountPct           FLOAT,
    PromotionType   STRING,
    PromotionCategory STRING,
    StartDate             DATE,
    EndDate               DATE,
    MinQty                INT,
    MaxQty                INT,
    _ingestion_timestamp   TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_promotion'
TBLPROPERTIES ( 
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);
 


INSERT INTO gold.dim_promotion
SELECT
    special_offer_id AS PromotionKey,
    description AS PromotionDescription,
    discount_pct AS DiscountPct,
    type AS PromotionType,
    category AS PromotionCategory,
    CAST(start_date AS DATE) AS StartDate,
    CAST(end_date_offer AS DATE) AS EndDate,
    min_qty AS MinQty,
    max_qty AS MaxQty,
    _ingestion_timestamp
FROM silver.sales_specialoffer
WHERE special_offer_id IS NOT NULL
        AND is_current = true
        AND NOT EXISTS (
            SELECT 1 FROM gold.dim_promotion t2
            WHERE t2.PromotionKey = silver.sales_specialoffer.special_offer_id
        );