-- ███████████████████████████████████████████████████████████████████████████
-- 5. person_countryregion — SCD TYPE 2
--    Clé       : CountryRegionCode (STRING)
--    Surveillé : Name
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.person_countryregion (
    country_region_code     STRING,
    name                    STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_countryregion'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- ÉTAPE 0 : Classer les lignes bronze
CREATE OR REPLACE TEMP VIEW person_countryregion_ranked AS
SELECT
    CAST(CountryRegionCode  AS STRING)    AS country_region_code,
    CAST(Name               AS STRING)    AS name,
    CAST(ModifiedDate       AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY CountryRegionCode
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY CountryRegionCode
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_countryregion
WHERE CountryRegionCode IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.person_countryregion AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM person_countryregion_ranked b
        WHERE b.country_region_code = t.country_region_code
    )
WHERE t.is_current = TRUE
  AND t.country_region_code IN (SELECT DISTINCT country_region_code FROM person_countryregion_ranked)
  AND EXISTS (
      SELECT 1 FROM person_countryregion_ranked b
      WHERE b.country_region_code = t.country_region_code AND b.rn = 1
        AND b.name <> t.name
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les codes modifiés
INSERT INTO silver.person_countryregion
SELECT
    b.country_region_code,
    b.name,
    b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_countryregion_ranked b
WHERE b.country_region_code IN (
    SELECT DISTINCT b2.country_region_code
    FROM person_countryregion_ranked b2
    JOIN silver.person_countryregion t ON b2.country_region_code = t.country_region_code AND t.is_current = FALSE
    WHERE b2.rn = 1 AND b2.name <> t.name
);

-- ÉTAPE 3 : Insérer les nouveaux codes
INSERT INTO silver.person_countryregion
SELECT
    b.country_region_code,
    b.name,
    b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_countryregion_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.person_countryregion t
    WHERE t.country_region_code = b.country_region_code
);

OPTIMIZE silver.person_countryregion ZORDER BY (country_region_code);


SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_countryregion    RETAIN 2 HOURS;