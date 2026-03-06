-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.production_unitmeasure  (SCD TYPE 2)
-- Clé       : UnitMeasureCode
-- Surveillé : Name
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.production_unitmeasure (
    unit_measure_code       STRING,
    name                    STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_unitmeasure'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 0 : Classer les lignes bronze
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW production_unitmeasure_ranked AS
SELECT
    CAST(UnitMeasureCode  AS STRING)    AS unit_measure_code,
    CAST(Name             AS STRING)    AS name,
    CAST(ModifiedDate     AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY UnitMeasureCode
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY UnitMeasureCode
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_unitmeasure
WHERE UnitMeasureCode IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les IDs à fermer
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW unitmeasure_ids_to_close AS
SELECT
    b.unit_measure_code,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM production_unitmeasure_ranked b
JOIN silver.production_unitmeasure t
    ON  b.unit_measure_code = t.unit_measure_code
    AND t.is_current        = TRUE
WHERE b.name <> t.name
GROUP BY b.unit_measure_code;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.production_unitmeasure AS t
USING unitmeasure_ids_to_close AS s
ON  t.unit_measure_code = s.unit_measure_code
AND t.is_current        = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_unitmeasure
SELECT
    b.unit_measure_code,
    b.name,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM (
    SELECT
        *,
        LAG(name) OVER (
            PARTITION BY unit_measure_code ORDER BY modified_date ASC
        ) AS prev_name,
        LAG(unit_measure_code) OVER (
            PARTITION BY unit_measure_code ORDER BY modified_date ASC
        ) AS prev_unit_measure_code
    FROM production_unitmeasure_ranked
) b
JOIN unitmeasure_ids_to_close s
    ON b.unit_measure_code = s.unit_measure_code
WHERE
    b.name              <> COALESCE(b.prev_name,              '')  OR
    b.unit_measure_code <> COALESCE(b.prev_unit_measure_code, '');

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.production_unitmeasure
SELECT
    b.unit_measure_code,
    b.name,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM production_unitmeasure_ranked b
LEFT JOIN silver.production_unitmeasure t
    ON b.unit_measure_code = t.unit_measure_code
WHERE t.unit_measure_code IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.production_unitmeasure
ZORDER BY (unit_measure_code);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.production_unitmeasure RETAIN 168 HOURS;