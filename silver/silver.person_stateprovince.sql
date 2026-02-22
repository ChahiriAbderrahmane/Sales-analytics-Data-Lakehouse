-- ███████████████████████████████████████████████████████████████████████████
-- 4. person_stateprovince — SCD TYPE 2
--    Clé       : StateProvinceID
--    Surveillé : Name, CountryRegionCode, TerritoryID, IsOnlyStateProvinceFlag
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.person_stateprovince (
    state_province_id           INT,
    state_province_code         STRING,
    country_region_code         STRING,
    is_only_state_province_flag BOOLEAN,
    name                        STRING,
    territory_id                INT,
    rowguid                     STRING,
    modified_date               TIMESTAMP,
    _ingestion_timestamp        TIMESTAMP,
    is_current                  BOOLEAN,
    end_date                    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_stateprovince'
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
CREATE OR REPLACE TEMP VIEW person_stateprovince_ranked AS
SELECT
    CAST(StateProvinceID            AS INT)       AS state_province_id,
    CAST(StateProvinceCode          AS STRING)    AS state_province_code,
    CAST(CountryRegionCode          AS STRING)    AS country_region_code,
    CAST(IsOnlyStateProvinceFlag    AS BOOLEAN)   AS is_only_state_province_flag,
    CAST(Name                       AS STRING)    AS name,
    CAST(TerritoryID                AS INT)       AS territory_id,
    CAST(rowguid                    AS STRING)    AS rowguid,
    CAST(ModifiedDate               AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY StateProvinceID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY StateProvinceID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_stateprovince
WHERE StateProvinceID IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.person_stateprovince AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM person_stateprovince_ranked b
        WHERE b.state_province_id = t.state_province_id
    )
WHERE t.is_current = TRUE
  AND t.state_province_id IN (SELECT DISTINCT state_province_id FROM person_stateprovince_ranked)
  AND EXISTS (
      SELECT 1 FROM person_stateprovince_ranked b
      WHERE b.state_province_id = t.state_province_id AND b.rn = 1
        AND (
            b.name                        <> t.name                        OR
            b.country_region_code         <> t.country_region_code         OR
            b.territory_id                <> t.territory_id                OR
            b.is_only_state_province_flag <> t.is_only_state_province_flag
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.person_stateprovince
SELECT
    b.state_province_id, b.state_province_code, b.country_region_code,
    b.is_only_state_province_flag, b.name, b.territory_id,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_stateprovince_ranked b
WHERE b.state_province_id IN (
    SELECT DISTINCT b2.state_province_id
    FROM person_stateprovince_ranked b2
    JOIN silver.person_stateprovince t ON b2.state_province_id = t.state_province_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.name                        <> t.name                        OR
          b2.country_region_code         <> t.country_region_code         OR
          b2.territory_id                <> t.territory_id                OR
          b2.is_only_state_province_flag <> t.is_only_state_province_flag
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.person_stateprovince
SELECT
    b.state_province_id, b.state_province_code, b.country_region_code,
    b.is_only_state_province_flag, b.name, b.territory_id,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_stateprovince_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.person_stateprovince t
    WHERE t.state_province_id = b.state_province_id
);

OPTIMIZE silver.person_stateprovince ZORDER BY (country_region_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_stateprovince    RETAIN 2 HOURS;