CREATE TABLE IF NOT EXISTS gold.dim_date (
    DateKey             INT         NOT NULL,
    Date                DATE,
    Day                 INT,
    Month               INT,
    Year                INT,
    Quarter             INT,
    DayOfWeek           STRING,
    MonthName           STRING,
    QuarterName         STRING,
    YearName            STRING,
    IsWeekend           BOOLEAN,
    IngestionTimestamp  TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_date'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

INSERT INTO gold.dim_date
SELECT
    CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT)  AS DateKey,
    calendar_date                                         AS Date,
    DAY(calendar_date)                                    AS Day,
    MONTH(calendar_date)                                  AS Month,
    YEAR(calendar_date)                                   AS Year,
    QUARTER(calendar_date)                                AS Quarter,
    DATE_FORMAT(calendar_date, 'EEEE')                   AS DayOfWeek,
    DATE_FORMAT(calendar_date, 'MMMM')                   AS MonthName,
    CONCAT('Q', QUARTER(calendar_date))                   AS QuarterName,
    CAST(YEAR(calendar_date) AS STRING)                   AS YearName,
    CASE WHEN DAYOFWEEK(calendar_date) IN (1,7)
         THEN TRUE ELSE FALSE END                         AS IsWeekend,
    current_timestamp()                                   AS IngestionTimestamp
FROM (
    SELECT EXPLODE(
        SEQUENCE(DATE '2010-01-01', DATE '2016-12-31', INTERVAL 1 DAY)
    ) AS calendar_date
) dates;