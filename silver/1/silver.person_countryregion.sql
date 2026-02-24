-- 5. person_countryregion — SCD TYPE 2
--    Clé       : CountryRegionCode (STRING)
--    Surveillé : Name

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

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les codes à fermer
-- Matérialisé en TEMP VIEW pour éviter les sous-requêtes dans le MERGE
-- (limitation Delta open source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.country_region_code,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_countryregion_ranked b
JOIN silver.person_countryregion t
    ON  b.country_region_code = t.country_region_code
    AND t.is_current = TRUE
WHERE b.name <> t.name
GROUP BY b.country_region_code;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.person_countryregion AS t
USING ids_to_close AS s
ON  t.country_region_code = s.country_region_code
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les codes modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_countryregion
SELECT
    b.country_region_code,
    b.name,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM (
    -- On ranke chronologiquement (ASC) pour calculer le précédent état
    SELECT 
        *,
        LAG(name)     OVER (PARTITION BY country_region_code ORDER BY modified_date ASC) AS prev_name
    FROM person_countryregion_ranked
) b
JOIN ids_to_close s ON b.country_region_code = s.country_region_code
WHERE 
    -- On insère seulement si cette ligne diffère de la précédente (dans le batch ou de silver)
    (b.name <> COALESCE(b.prev_name, 0))
    -- OU si c'est la première ligne du batch et qu'elle diffère de silver (déjà couvert par ids_to_close)
;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux codes (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_countryregion
SELECT
    b.country_region_code,
    b.name,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_countryregion_ranked b
LEFT JOIN silver.person_countryregion t
    ON b.country_region_code = t.country_region_code
WHERE t.country_region_code IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.person_countryregion
ZORDER BY (country_region_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_countryregion RETAIN 168 HOURS;