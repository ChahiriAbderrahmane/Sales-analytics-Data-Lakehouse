-- 3. sales_salesterritory — SCD TYPE 2
--    Clé       : TerritoryID
--    Surveillé : Name, CountryRegionCode, Group
--    Note      : SalesYTD/CostYTD sont des agrégats recalculés automatiquement
--                → non surveillés car ils changent à chaque transaction

CREATE TABLE IF NOT EXISTS silver.sales_salesterritory (
    territory_id            INT,
    name                    STRING,
    country_region_code     STRING,
    territory_group         STRING,
    sales_ytd               DECIMAL(19,4),
    sales_last_year         DECIMAL(19,4),
    cost_ytd                DECIMAL(19,4),
    cost_last_year          DECIMAL(19,4),
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesterritory'
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
-- Note : `Group` est un mot réservé Spark SQL → on utilise des backticks
CREATE OR REPLACE TEMP VIEW sales_salesterritory_ranked AS
SELECT
    CAST(TerritoryID        AS INT)            AS territory_id,
    CAST(Name               AS STRING)         AS name,
    CAST(CountryRegionCode  AS STRING)         AS country_region_code,
    CAST(`Group`            AS STRING)         AS territory_group,
    ABS(CAST(SalesYTD       AS DECIMAL(19,4))) AS sales_ytd,
    ABS(CAST(SalesLastYear  AS DECIMAL(19,4))) AS sales_last_year,
    ABS(CAST(CostYTD        AS DECIMAL(19,4))) AS cost_ytd,
    ABS(CAST(CostLastYear   AS DECIMAL(19,4))) AS cost_last_year,
    CAST(rowguid            AS STRING)         AS rowguid,
    CAST(ModifiedDate       AS TIMESTAMP)      AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY TerritoryID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY TerritoryID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.sales_salesterritory
WHERE TerritoryID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- IS DISTINCT FROM remplace <> pour gérer correctement les valeurs NULL
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.territory_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM sales_salesterritory_ranked b
JOIN silver.sales_salesterritory t
    ON  b.territory_id = t.territory_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.name                IS DISTINCT FROM t.name
   OR b.country_region_code IS DISTINCT FROM t.country_region_code
   OR b.territory_group     IS DISTINCT FROM t.territory_group
GROUP BY b.territory_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.sales_salesterritory AS t
USING ids_to_close AS s
ON  t.territory_id = s.territory_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_salesterritory
SELECT
    b.territory_id,
    b.name,
    b.country_region_code,
    b.territory_group,
    b.sales_ytd,
    b.sales_last_year,
    b.cost_ytd,
    b.cost_last_year,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_salesterritory_ranked b
JOIN ids_to_close s
    ON b.territory_id = s.territory_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_salesterritory
SELECT
    b.territory_id,
    b.name,
    b.country_region_code,
    b.territory_group,
    b.sales_ytd,
    b.sales_last_year,
    b.cost_ytd,
    b.cost_last_year,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_salesterritory_ranked b
LEFT JOIN silver.sales_salesterritory t
    ON b.territory_id = t.territory_id
WHERE t.territory_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.sales_salesterritory
ZORDER BY (country_region_code, territory_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesterritory RETAIN 168 HOURS;