-- ███████████████████████████████████████████████████████████████████████████
-- 5. sales_currencyrate — INSERT UNIQUEMENT (transactionnel)
--    Clé : CurrencyRateID
--    Logique : un taux de change est un snapshot quotidien, il ne change pas
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.sales_currencyrate (
    currency_rate_id        INT,
    currency_rate_date      TIMESTAMP,
    from_currency_code      STRING,
    to_currency_code        STRING,
    average_rate            DECIMAL(19,4),
    end_of_day_rate         DECIMAL(19,4),
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_currencyrate'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

INSERT INTO silver.sales_currencyrate
SELECT
    CAST(CurrencyRateID     AS INT)            AS currency_rate_id,
    CAST(CurrencyRateDate   AS TIMESTAMP)      AS currency_rate_date,
    CAST(FromCurrencyCode   AS STRING)         AS from_currency_code,
    CAST(ToCurrencyCode     AS STRING)         AS to_currency_code,
    ABS(CAST(AverageRate    AS DECIMAL(19,4))) AS average_rate,
    ABS(CAST(EndOfDayRate   AS DECIMAL(19,4))) AS end_of_day_rate,
    CAST(ModifiedDate       AS TIMESTAMP)      AS modified_date,
    current_timestamp()                        AS _ingestion_timestamp
FROM bronze.sales_currencyrate b
WHERE b.CurrencyRateID IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.sales_currencyrate t
      WHERE t.currency_rate_id = b.CurrencyRateID
  );

OPTIMIZE silver.sales_currencyrate ZORDER BY (currency_rate_date, from_currency_code, to_currency_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_currencyrate  RETAIN 2 HOURS;