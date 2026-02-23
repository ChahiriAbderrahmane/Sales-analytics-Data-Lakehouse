-- 3. person_person — SCD TYPE 2
--    Clé       : BusinessEntityID
--    Surveillé : FirstName, LastName, MiddleName, Title, EmailPromotion, PersonType

CREATE TABLE IF NOT EXISTS silver.person_person (
    business_entity_id      INT,
    person_type             STRING,
    name_style              BOOLEAN,
    title                   STRING,
    first_name              STRING,
    middle_name             STRING,
    last_name               STRING,
    suffix                  STRING,
    email_promotion         INT,
    additional_contact_info STRING,
    demographics            STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_person'
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
CREATE OR REPLACE TEMP VIEW person_person_ranked AS
SELECT
    CAST(BusinessEntityID       AS INT)       AS business_entity_id,
    CAST(PersonType             AS STRING)    AS person_type,
    CAST(NameStyle              AS BOOLEAN)   AS name_style,
    CAST(Title                  AS STRING)    AS title,
    CAST(FirstName              AS STRING)    AS first_name,
    CAST(MiddleName             AS STRING)    AS middle_name,
    CAST(LastName               AS STRING)    AS last_name,
    CAST(Suffix                 AS STRING)    AS suffix,
    CAST(EmailPromotion         AS INT)       AS email_promotion,
    CAST(AdditionalContactInfo  AS STRING)    AS additional_contact_info,
    CAST(Demographics           AS STRING)    AS demographics,
    CAST(rowguid                AS STRING)    AS rowguid,
    CAST(ModifiedDate           AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY BusinessEntityID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY BusinessEntityID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_person
WHERE BusinessEntityID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- Matérialisé en TEMP VIEW pour éviter les sous-requêtes dans le MERGE
-- (limitation Delta open source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.business_entity_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM person_person_ranked b
JOIN silver.person_person t
    ON  b.business_entity_id = t.business_entity_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.first_name      <> t.first_name
   OR b.last_name       <> t.last_name
   OR b.middle_name     <> t.middle_name
   OR b.title           <> t.title
   OR b.email_promotion <> t.email_promotion
   OR b.person_type     <> t.person_type
GROUP BY b.business_entity_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.person_person AS t
USING ids_to_close AS s
ON  t.business_entity_id = s.business_entity_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_person
SELECT
    b.business_entity_id,
    b.person_type,
    b.name_style,
    b.title,
    b.first_name,
    b.middle_name,
    b.last_name,
    b.suffix,
    b.email_promotion,
    b.additional_contact_info,
    b.demographics,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_person_ranked b
JOIN ids_to_close s
    ON b.business_entity_id = s.business_entity_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.person_person
SELECT
    b.business_entity_id,
    b.person_type,
    b.name_style,
    b.title,
    b.first_name,
    b.middle_name,
    b.last_name,
    b.suffix,
    b.email_promotion,
    b.additional_contact_info,
    b.demographics,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM person_person_ranked b
LEFT JOIN silver.person_person t
    ON b.business_entity_id = t.business_entity_id
WHERE t.business_entity_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.person_person
ZORDER BY (last_name, first_name);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_person RETAIN 168 HOURS;