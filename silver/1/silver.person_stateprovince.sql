-- 4. person_stateprovince — SCD TYPE 2
--    Clé       : StateProvinceID
--    Surveillé : Name, CountryRegionCode, TerritoryID, IsOnlyStateProvinceFlag

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

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- Matérialisé en TEMP VIEW pour éviter les sous-requêtes dans le MERGE
-- (limitation Delta open source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.state_province_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_stateprovince_ranked b
JOIN silver.person_stateprovince t
    ON  b.state_province_id = t.state_province_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.name                        <> t.name
   OR b.country_region_code         <> t.country_region_code
   OR b.territory_id                <> t.territory_id
   OR b.is_only_state_province_flag <> t.is_only_state_province_flag
GROUP BY b.state_province_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.person_stateprovince AS t
USING ids_to_close AS s
ON  t.state_province_id = s.state_province_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_stateprovince
SELECT
    b.state_province_id,
    b.state_province_code,
    b.country_region_code,
    b.is_only_state_province_flag,
    b.name,
    b.territory_id,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_stateprovince_ranked b
JOIN ids_to_close s
    ON b.state_province_id = s.state_province_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_stateprovince
SELECT
    b.state_province_id,
    b.state_province_code,
    b.country_region_code,
    b.is_only_state_province_flag,
    b.name,
    b.territory_id,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_stateprovince_ranked b
LEFT JOIN silver.person_stateprovince t
    ON b.state_province_id = t.state_province_id
WHERE t.state_province_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.person_stateprovince
ZORDER BY (country_region_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_stateprovince RETAIN 168 HOURS;