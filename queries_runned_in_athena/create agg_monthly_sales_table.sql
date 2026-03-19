CREATE external TABLE IF NOT EXISTS gold_serving.agg_monthly_sales(
    OrderYear               INT,
    OrderMonth              INT,
    CountryRegionCode       STRING,
    TerritoryGroup          STRING,
    ProductLine            STRING,
    TotalOrders            INT,
    TotalQuantitySold      INT,
    TotalSalesAmount       DECIMAL(19,2),
    TotalProductCost       DECIMAL(19,2),
    GrossProfit            DECIMAL(19,2),
    GrossMarginPct         DECIMAL(5,2),
    TotalTax               DECIMAL(19,2),
    TotalFreight           DECIMAL(19,2),
    TotalDiscountGiven     DOUBLE,
    last_refresh_timestamp TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://sales-data-mart-internet-sales-process/gold-layer-aggregate-tables/agg_monthly_sales/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
