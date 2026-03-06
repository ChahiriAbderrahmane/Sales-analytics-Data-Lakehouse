-- ═══════════════════════════════════════════════════════════════════════════
-- DDL — silver.person_emailaddress  (SCD TYPE 2)
-- Clé       : BusinessEntityID + EmailAddressID
-- Surveillé : EmailAddress
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS silver.person_emailaddress (
    business_entity_id      INT,
    email_address_id        INT,
    email_address           STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_emailaddress'
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
CREATE OR REPLACE TEMP VIEW person_emailaddress_ranked AS
SELECT
    CAST(BusinessEntityID   AS INT)       AS business_entity_id,
    CAST(EmailAddressID     AS INT)       AS email_address_id,
    CAST(EmailAddress       AS STRING)    AS email_address,
    CAST(rowguid            AS STRING)    AS rowguid,
    CAST(ModifiedDate       AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY BusinessEntityID, EmailAddressID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY BusinessEntityID, EmailAddressID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_emailaddress
WHERE BusinessEntityID IS NOT NULL
  AND EmailAddressID   IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 1 : Identifier les lignes actives à fermer
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP VIEW emailaddress_ids_to_close AS
SELECT
    b.business_entity_id,
    b.email_address_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_emailaddress_ranked b
JOIN silver.person_emailaddress t
    ON  b.business_entity_id = t.business_entity_id
    AND b.email_address_id   = t.email_address_id
    AND t.is_current         = TRUE
WHERE b.email_address <> t.email_address
GROUP BY b.business_entity_id, b.email_address_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 2 : Fermer les lignes actives modifiées
-- ═══════════════════════════════════════════════════════════════════════════
MERGE INTO silver.person_emailaddress AS t
USING emailaddress_ids_to_close AS s
ON  t.business_entity_id = s.business_entity_id
AND t.email_address_id   = s.email_address_id
AND t.is_current         = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 3 : Insérer les nouvelles versions (IDs déjà présents dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.person_emailaddress
SELECT
    b.business_entity_id,
    b.email_address_id,
    b.email_address,
    b.rowguid,
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
        LAG(email_address) OVER (
            PARTITION BY business_entity_id, email_address_id
            ORDER BY modified_date ASC
        ) AS prev_email_address,
        LAG(email_address_id) OVER (
            PARTITION BY business_entity_id, email_address_id
            ORDER BY modified_date ASC
        ) AS prev_email_address_id
    FROM person_emailaddress_ranked
) b
JOIN emailaddress_ids_to_close s
    ON  b.business_entity_id = s.business_entity_id
    AND b.email_address_id   = s.email_address_id
WHERE
    (b.email_address    <> COALESCE(b.prev_email_address,    '')  OR
     b.email_address_id <> COALESCE(b.prev_email_address_id,  0));

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO silver.person_emailaddress
SELECT
    b.business_entity_id,
    b.email_address_id,
    b.email_address,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_emailaddress_ranked b
LEFT JOIN silver.person_emailaddress t
    ON  b.business_entity_id = t.business_entity_id
    AND b.email_address_id   = t.email_address_id
WHERE t.business_entity_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- ÉTAPE 5 : Optimisation
-- ═══════════════════════════════════════════════════════════════════════════
OPTIMIZE silver.person_emailaddress
ZORDER BY (business_entity_id, email_address_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_emailaddress RETAIN 168 HOURS;