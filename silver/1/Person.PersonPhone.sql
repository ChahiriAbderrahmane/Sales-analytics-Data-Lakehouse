-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.person_personphone  (SCD TYPE 2)
-- Clé       : BusinessEntityID + PhoneNumberTypeID
-- Surveillé : PhoneNumber
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.person_personphone (
    business_entity_id      INT,
    phone_number            STRING,
    phone_number_type_id    INT,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_personphone'
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
CREATE OR REPLACE TEMP VIEW person_personphone_ranked AS
SELECT
    CAST(BusinessEntityID    AS INT)       AS business_entity_id,
    CAST(PhoneNumber         AS STRING)    AS phone_number,
    CAST(PhoneNumberTypeID   AS INT)       AS phone_number_type_id,
    CAST(ModifiedDate        AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY BusinessEntityID, PhoneNumberTypeID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY BusinessEntityID, PhoneNumberTypeID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_personphone
WHERE BusinessEntityID  IS NOT NULL
  AND PhoneNumberTypeID IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les lignes actives à fermer
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW personphone_ids_to_close AS
SELECT
    b.business_entity_id,
    b.phone_number_type_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_personphone_ranked b
JOIN silver.person_personphone t
    ON  b.business_entity_id   = t.business_entity_id
    AND b.phone_number_type_id = t.phone_number_type_id
    AND t.is_current           = TRUE
WHERE b.phone_number <> t.phone_number
GROUP BY b.business_entity_id, b.phone_number_type_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.person_personphone AS t
USING personphone_ids_to_close AS s
ON  t.business_entity_id   = s.business_entity_id
AND t.phone_number_type_id = s.phone_number_type_id
AND t.is_current           = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.person_personphone
SELECT
    b.business_entity_id,
    b.phone_number,
    b.phone_number_type_id,
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
        LAG(phone_number) OVER (
            PARTITION BY business_entity_id, phone_number_type_id
            ORDER BY modified_date ASC
        ) AS prev_phone_number,
        LAG(phone_number_type_id) OVER (
            PARTITION BY business_entity_id, phone_number_type_id
            ORDER BY modified_date ASC
        ) AS prev_phone_number_type_id
    FROM person_personphone_ranked
) b
JOIN personphone_ids_to_close s
    ON  b.business_entity_id   = s.business_entity_id
    AND b.phone_number_type_id = s.phone_number_type_id
WHERE
    (b.phone_number         <> COALESCE(b.prev_phone_number,          '')  OR
     b.phone_number_type_id <> COALESCE(b.prev_phone_number_type_id,   0));

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.person_personphone
SELECT
    b.business_entity_id,
    b.phone_number,
    b.phone_number_type_id,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_personphone_ranked b
LEFT JOIN silver.person_personphone t
    ON  b.business_entity_id   = t.business_entity_id
    AND b.phone_number_type_id = t.phone_number_type_id
WHERE t.business_entity_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.person_personphone
ZORDER BY (business_entity_id, phone_number_type_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_personphone RETAIN 168 HOURS;