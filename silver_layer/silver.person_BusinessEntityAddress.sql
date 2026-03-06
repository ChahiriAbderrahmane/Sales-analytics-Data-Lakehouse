-- SCD TYPE 2 — silver.person_businessentityaddress
-- Clé       : (business_entity_id, address_type_id)
-- Surveillé : address_id

CREATE TABLE IF NOT EXISTS silver.person_businessentityaddress (
    business_entity_id      INT,
    address_id              INT,
    address_type_id         INT,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_businessentityaddress'
TBLPROPERTIES (
    'delta.minReaderVersion'             = '1',
    'delta.minWriterVersion'             = '2',
    'delta.enableChangeDataFeed'         = 'true',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true',
    'delta.enableDeletionVectors'        = 'true',
    'delta.columnMapping.mode'           = 'name'
);

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 0 : Classer les lignes bronze
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW person_businessentityaddress_ranked AS
SELECT
    CAST(business_entity_id   AS INT)       AS business_entity_id,
    CAST(address_id          AS INT)       AS address_id,
    CAST(address_type_id      AS INT)       AS address_type_id,
    CAST(rowguid            AS STRING)    AS rowguid,
    CAST(modified_date       AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY business_entity_id, address_type_id
        ORDER BY modified_date DESC
    ) AS rn,
    LEAD(CAST(modified_date AS TIMESTAMP)) OVER (
        PARTITION BY business_entity_id, address_type_id
        ORDER BY modified_date DESC
    ) AS next_version_date
FROM bronze.person_businessentityaddress
WHERE business_entity_id IS NOT NULL
  AND address_type_id    IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les clés composites à fermer
-- IS DISTINCT FROM remplace <> pour gérer correctement les valeurs NULL
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.business_entity_id,
    b.address_type_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_businessentityaddress_ranked b
JOIN silver.person_businessentityaddress t
    ON  b.business_entity_id = t.business_entity_id
    AND b.address_type_id    = t.address_type_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.address_id IS DISTINCT FROM t.address_id
GROUP BY b.business_entity_id, b.address_type_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.person_businessentityaddress AS t
USING ids_to_close AS s
ON  t.business_entity_id = s.business_entity_id
AND t.address_type_id    = s.address_type_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les clés modifiées
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_businessentityaddress
SELECT
    b.business_entity_id,
    b.address_id,
    b.address_type_id,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_businessentityaddress_ranked b
JOIN ids_to_close s
    ON  b.business_entity_id = s.business_entity_id
    AND b.address_type_id    = s.address_type_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouvelles clés (jamais vues dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_businessentityaddress
SELECT
    b.business_entity_id,
    b.address_id,
    b.address_type_id,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_businessentityaddress_ranked b
LEFT JOIN silver.person_businessentityaddress t
    ON  b.business_entity_id = t.business_entity_id
    AND b.address_type_id    = t.address_type_id
WHERE t.business_entity_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.person_businessentityaddress
ZORDER BY (business_entity_id, address_type_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_businessentityaddress RETAIN 168 HOURS;