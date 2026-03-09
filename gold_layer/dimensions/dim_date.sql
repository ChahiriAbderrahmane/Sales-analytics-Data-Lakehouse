CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk         INT          NOT NULL,   -- yyyyMMdd, ex: 20240315
    date            DATE         NOT NULL,
    year            INT,
    quarter         INT,
    quarter_name    STRING,
    month           INT,
    month_name      STRING,
    day             INT,
    day_of_week     INT,
    day_name        STRING,
    week_of_year    INT,
    is_weekday      BOOLEAN,
    is_weekend      BOOLEAN,
    is_last_day_of_month BOOLEAN,
    -- Colonnes fiscales (exercice commençant en juillet)
    fiscal_year     INT,
    fiscal_quarter  INT,
    fiscal_month    INT
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_date'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
);

CREATE OR REPLACE TABLE gold.dim_date
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_date'
AS
SELECT
    CAST(date_format(d.date, 'yyyyMMdd') AS INT)        AS date_sk,
    d.date,
    YEAR(d.date)                                         AS year,
    QUARTER(d.date)                                      AS quarter,
    CONCAT('Q', QUARTER(d.date))                         AS quarter_name,
    MONTH(d.date)                                        AS month,
    date_format(d.date, 'MMMM')                         AS month_name,
    DAY(d.date)                                          AS day,
    DAYOFWEEK(d.date)                                    AS day_of_week,
    date_format(d.date, 'EEEE')                         AS day_name,
    WEEKOFYEAR(d.date)                                   AS week_of_year,
    CASE WHEN DAYOFWEEK(d.date) BETWEEN 2 AND 6
         THEN TRUE ELSE FALSE END                        AS is_weekday,
    CASE WHEN DAYOFWEEK(d.date) IN (1, 7)
         THEN TRUE ELSE FALSE END                        AS is_weekend,
    CASE WHEN d.date = LAST_DAY(d.date)
         THEN TRUE ELSE FALSE END                        AS is_last_day_of_month,
    -- Fiscal year : juillet = début (Adventure Works convention)
    CASE WHEN MONTH(d.date) >= 7
         THEN YEAR(d.date) + 1
         ELSE YEAR(d.date) END                           AS fiscal_year,
    CASE WHEN MONTH(d.date) IN (7,8,9)   THEN 1
         WHEN MONTH(d.date) IN (10,11,12) THEN 2
         WHEN MONTH(d.date) IN (1,2,3)   THEN 3
         ELSE 4 END                                      AS fiscal_quarter,
    CASE WHEN MONTH(d.date) >= 7
         THEN MONTH(d.date) - 6
         ELSE MONTH(d.date) + 6 END                     AS fiscal_month
FROM (
    SELECT EXPLODE(
        SEQUENCE(TO_DATE('2010-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)
    ) AS date
) d;