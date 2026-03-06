CREATE TABLE IF NOT EXISTS gold.dim_geography (
    GeographyKey           INT         NOT NULL,  -- business key
    City                    STRING,
    StateProvinceCode       STRING,
    StateProvinceName       STRING,
    CountryRegionCode       STRING,
    CountryRegionName       STRING,
    PostalCode             STRING,
    SalesTerritoryKey      INT,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_geography'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);

INSERT INTO gold.dim_geography
SELECT
    addr.address_id                 AS GeographyKey,
    addr.city                       AS City,
    sp.state_province_code          AS StateProvinceCode,
    sp.name                         AS StateProvinceName,
    cr.country_region_code          AS CountryRegionCode,
    cr.name                         AS CountryRegionName,
    addr.postal_code                AS PostalCode,
    sp.territory_id                 AS SalesTerritoryKey,
    current_timestamp()             AS ingestion_timestamp
FROM silver.person_address addr          
JOIN silver.person_stateprovince sp
    ON addr.state_province_id = sp.state_province_id
    AND sp.is_current = TRUE
JOIN silver.person_countryregion cr
    ON sp.country_region_code = cr.country_region_code
    AND cr.is_current = TRUE
WHERE addr.address_id IS NOT NULL
  AND sp.state_province_code IS NOT NULL
  AND cr.country_region_code IS NOT NULL
  AND addr.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM gold.dim_geography g
      WHERE g.GeographyKey = addr.address_id
  );