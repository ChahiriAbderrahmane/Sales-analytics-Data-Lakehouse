CREATE TABLE IF NOT EXISTS gold.dim_customer (
    CustomerKey            INT             NOT NULL,
    GeographyKey           INT,
    CustomerAlternateKey   STRING,
    Title                   STRING,
    FirstName               STRING,
    MiddleName              STRING,
    LastName                STRING,
    NameStyle               BOOLEAN,
    BirthDate               DATE,
    MaritalStatus           STRING,
    Suffix                   STRING,
    Gender                  STRING,
    EmailAddress            STRING,
    YearlyIncome            STRING,
    TotalChildren           INT,
    NumberChildrenAtHome    INT,
    EnglishEducation        STRING,
    SpanishEducation        STRING,
    FrenchEducation         STRING,
    EnglishOccupation       STRING,
    SpanishOccupation       STRING,
    FrenchOccupation        STRING,
    HouseOwnerFlag          STRING,
    NumberCarsOwned         INT,
    AddressLine1            STRING,
    AddressLine2            STRING,
    Phone                   STRING,
    DateFirstPurchase       TIMESTAMP,
    CommuteDistance         STRING,
    valid_from              TIMESTAMP       NOT NULL,
    is_current              BOOLEAN,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
TBLPROPERTIES ( 
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);


CREATE TABLE IF NOT EXISTS gold.dim_customer_history (
    CustomerKey            INT             NOT NULL,
    GeographyKey           INT,
    CustomerAlternateKey   STRING,
    Title                   STRING,
    FirstName               STRING,
    MiddleName              STRING,
    LastName                STRING,
    NameStyle               BOOLEAN,
    BirthDate               DATE,
    MaritalStatus           STRING,
    Suffix                   STRING,
    Gender                  STRING,
    EmailAddress            STRING,
    YearlyIncome            STRING,
    TotalChildren           INT,
    NumberChildrenAtHome    INT,
    EnglishEducation        STRING,
    SpanishEducation        STRING,
    FrenchEducation         STRING,
    EnglishOccupation       STRING,
    SpanishOccupation       STRING,
    FrenchOccupation        STRING,
    HouseOwnerFlag          STRING,
    NumberCarsOwned         INT,
    AddressLine1            STRING,
    AddressLine2            STRING,
    Phone                   STRING,
    DateFirstPurchase       TIMESTAMP,
    CommuteDistance         STRING,
    valid_from              TIMESTAMP       NOT NULL,
    is_current              BOOLEAN,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer_history'
TBLPROPERTIES ( 
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);


-- ============================================================
-- TABLE DE CONTRÔLE DES VERSIONS (à créer une seule fois)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.pipeline_control (
    table_name          STRING      NOT NULL,
    last_version        BIGINT,
    last_run_timestamp  TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/pipeline_control';

-- Utiliser "DESCRIBE HISTORY silver.sales_customer" pour récupérer la version actuelle de cette table
--       et l'insérer manuellement dans la pipeline_control     ----->>> limite à surpasser
INSERT INTO gold.pipeline_control
SELECT
    'silver.sales_customer'     AS table_name,
    7                           AS last_version,
    current_timestamp()         AS last_run_timestamp
WHERE NOT EXISTS (
    SELECT 1 FROM gold.pipeline_control
    WHERE table_name = 'silver.sales_customer'
);
-- ============================================================
-- ÉTAPE 1 : CACHE du résultat enrichi (remplace DISK_ONLY)
-- ============================================================
-- CACHE TABLE force la matérialisation physique comme persist()
-- La différence : Spark utilise MEMORY_AND_DISK (spill si RAM pleine)
CACHE TABLE silver_customer_enriched AS
WITH demographics_parsed AS (
    SELECT
        business_entity_id,
        regexp_replace(demographics, ' xmlns="[^"]*"', '') AS xml_clean
    FROM silver.person_person
    WHERE is_current = TRUE
),
-- Lire UNIQUEMENT les customer_id qui ont changé via CDF
-- La version de départ vient de la table de contrôle
cdf_changes AS (
    SELECT DISTINCT customer_id
    FROM table_changes('silver.sales_customer',
        -- Lire la dernière version traitée depuis pipeline_control
        (SELECT last_version + 1 FROM gold.pipeline_control
         WHERE table_name = 'silver.sales_customer')
    )
    WHERE _change_type IN ('insert', 'update_postimage')
)
SELECT
    c.customer_id                                           AS CustomerKey,
    addr.state_province_id                                  AS GeographyKey,
    c.account_number                                        AS CustomerAlternateKey,
    pp.title                                                AS Title,
    pp.first_name                                           AS FirstName,
    pp.middle_name                                          AS MiddleName,
    pp.last_name                                            AS LastName,
    pp.name_style                                           AS NameStyle,
    TO_DATE(xpath_string(d.xml_clean,
        '//IndividualSurvey/BirthDate'))                    AS BirthDate,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/MaritalStatus')                 AS MaritalStatus,
    pp.suffix                                               AS Suffix,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/Gender')                        AS Gender,
    ea.email_address                                        AS EmailAddress,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/YearlyIncome')                  AS YearlyIncome,
    CAST(xpath_string(d.xml_clean,
        '//IndividualSurvey/TotalChildren')       AS INT)   AS TotalChildren,
    CAST(xpath_string(d.xml_clean,
        '//IndividualSurvey/NumberChildrenAtHome') AS INT)  AS NumberChildrenAtHome,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/Education')                     AS EnglishEducation,
    COALESCE(edu.SpanishValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Education'))
                                                            AS SpanishEducation,
    COALESCE(edu.FrenchValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Education'))
                                                            AS FrenchEducation,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/Occupation')                    AS EnglishOccupation,
    COALESCE(occ.SpanishValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Occupation'))
                                                            AS SpanishOccupation,
    COALESCE(occ.FrenchValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Occupation'))
                                                            AS FrenchOccupation,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/HomeOwnerFlag')                 AS HouseOwnerFlag,
    CAST(xpath_string(d.xml_clean,
        '//IndividualSurvey/NumberCarsOwned')     AS INT)   AS NumberCarsOwned,
    addr.address_line1                                      AS AddressLine1,
    addr.address_line2                                      AS AddressLine2,
    ph.phone_number                                         AS Phone,
    c.modified_date                                         AS DateFirstPurchase,
    xpath_string(d.xml_clean,
        '//IndividualSurvey/CommuteDistance')               AS CommuteDistance,
    c.modified_date                                         AS valid_from,
    c.is_current
FROM silver.sales_customer c
-- JOIN sur les seuls customer_id changés → pas de full scan
INNER JOIN cdf_changes chg
    ON c.customer_id = chg.customer_id
LEFT JOIN silver.person_person pp
    ON c.person_id = pp.business_entity_id
    AND pp.is_current = TRUE
LEFT JOIN demographics_parsed d
    ON pp.business_entity_id = d.business_entity_id
LEFT JOIN silver.person_emailaddress ea
    ON pp.business_entity_id = ea.business_entity_id
    AND ea.is_current = TRUE
LEFT JOIN silver.person_personphone ph
    ON pp.business_entity_id = ph.business_entity_id
    AND ph.is_current = TRUE
LEFT JOIN silver.person_businessentityaddress bea
    ON pp.business_entity_id = bea.business_entity_id
    AND bea.is_current = TRUE
LEFT JOIN silver.person_address addr
    ON bea.address_id = addr.address_id
    AND addr.is_current = TRUE
LEFT JOIN silver.person_stateprovince sp
    ON addr.state_province_id = sp.state_province_id
    AND sp.is_current = TRUE
LEFT JOIN gold.ref_education_translation edu
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Education') = edu.EnglishValue
LEFT JOIN gold.ref_occupation_translation occ
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Occupation') = occ.EnglishValue;


-- ============================================================
-- ÉTAPE 2 : Archiver les anciennes versions dans history
--           (AVANT d'écraser dim_customer)
-- ============================================================
INSERT INTO gold.dim_customer_history
SELECT
    tgt.CustomerKey,
    tgt.GeographyKey,
    tgt.CustomerAlternateKey,
    tgt.Title,
    tgt.FirstName,
    tgt.MiddleName,
    tgt.LastName,
    tgt.NameStyle,
    tgt.BirthDate,
    tgt.MaritalStatus,
    tgt.Suffix,
    tgt.Gender,
    tgt.EmailAddress,
    tgt.YearlyIncome,
    tgt.TotalChildren,
    tgt.NumberChildrenAtHome,
    tgt.EnglishEducation,
    tgt.SpanishEducation,
    tgt.FrenchEducation,
    tgt.EnglishOccupation,
    tgt.SpanishOccupation,
    tgt.FrenchOccupation,
    tgt.HouseOwnerFlag,
    tgt.NumberCarsOwned,
    tgt.AddressLine1,
    tgt.AddressLine2,
    tgt.Phone,
    tgt.DateFirstPurchase,
    tgt.CommuteDistance,
    tgt.valid_from,
    current_timestamp()         AS valid_to,
    FALSE                       AS is_current,
    current_timestamp()         AS closed_timestamp,
    current_timestamp()         AS ingestion_timestamp,
    YEAR(current_timestamp())   AS year_valid_to
FROM gold.dim_customer tgt
-- Uniquement les clients qui ont un changement entrant
INNER JOIN silver_customer_enriched src
    ON tgt.CustomerKey = src.CustomerKey
WHERE tgt.is_current = TRUE;


-- ============================================================
-- ÉTAPE 3 : MERGE dans dim_customer depuis le cache
-- ============================================================
MERGE INTO gold.dim_customer tgt
USING silver_customer_enriched src
ON tgt.CustomerKey = src.CustomerKey

-- Client existant modifié → mise à jour
WHEN MATCHED AND src.is_current = TRUE THEN UPDATE SET
    tgt.GeographyKey            = src.GeographyKey,
    tgt.Title                   = src.Title,
    tgt.FirstName               = src.FirstName,
    tgt.MiddleName              = src.MiddleName,
    tgt.LastName                = src.LastName,
    tgt.NameStyle               = src.NameStyle,
    tgt.BirthDate               = src.BirthDate,
    tgt.MaritalStatus           = src.MaritalStatus,
    tgt.Suffix                  = src.Suffix,
    tgt.Gender                  = src.Gender,
    tgt.EmailAddress            = src.EmailAddress,
    tgt.YearlyIncome            = src.YearlyIncome,
    tgt.TotalChildren           = src.TotalChildren,
    tgt.NumberChildrenAtHome    = src.NumberChildrenAtHome,
    tgt.EnglishEducation        = src.EnglishEducation,
    tgt.SpanishEducation        = src.SpanishEducation,
    tgt.FrenchEducation         = src.FrenchEducation,
    tgt.EnglishOccupation       = src.EnglishOccupation,
    tgt.SpanishOccupation       = src.SpanishOccupation,
    tgt.FrenchOccupation        = src.FrenchOccupation,
    tgt.HouseOwnerFlag          = src.HouseOwnerFlag,
    tgt.NumberCarsOwned         = src.NumberCarsOwned,
    tgt.AddressLine1            = src.AddressLine1,
    tgt.AddressLine2            = src.AddressLine2,
    tgt.Phone                   = src.Phone,
    tgt.DateFirstPurchase       = src.DateFirstPurchase,
    tgt.CommuteDistance         = src.CommuteDistance,
    tgt.valid_from              = src.valid_from,
    tgt.is_current              = TRUE,
    tgt.ingestion_timestamp     = current_timestamp()

-- Nouveau client → insertion
WHEN NOT MATCHED THEN INSERT (
    CustomerKey, GeographyKey, CustomerAlternateKey,
    Title, FirstName, MiddleName, LastName, NameStyle,
    BirthDate, MaritalStatus, Suffix, Gender,
    EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome,
    EnglishEducation, SpanishEducation, FrenchEducation,
    EnglishOccupation, SpanishOccupation, FrenchOccupation,
    HouseOwnerFlag, NumberCarsOwned,
    AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance,
    valid_from, is_current, ingestion_timestamp
) VALUES (
    src.CustomerKey, src.GeographyKey, src.CustomerAlternateKey,
    src.Title, src.FirstName, src.MiddleName, src.LastName, src.NameStyle,
    src.BirthDate, src.MaritalStatus, src.Suffix, src.Gender,
    src.EmailAddress, src.YearlyIncome, src.TotalChildren, src.NumberChildrenAtHome,
    src.EnglishEducation, src.SpanishEducation, src.FrenchEducation,
    src.EnglishOccupation, src.SpanishOccupation, src.FrenchOccupation,
    src.HouseOwnerFlag, src.NumberCarsOwned,
    src.AddressLine1, src.AddressLine2, src.Phone, src.DateFirstPurchase, src.CommuteDistance,
    src.valid_from, TRUE, current_timestamp()
);


-- ============================================================
-- ÉTAPE 4 : Mettre à jour la version dans pipeline_control
-- ============================================================
MERGE INTO gold.pipeline_control tgt
USING (
    SELECT
        'silver.sales_customer'             AS table_name,
        -- Récupérer la version actuelle de la table silver
        (SELECT MAX(version)
         FROM (DESCRIBE HISTORY silver.sales_customer)) AS last_version,
        current_timestamp()                 AS last_run_timestamp
) src
ON tgt.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET
    tgt.last_version        = src.last_version,
    tgt.last_run_timestamp  = src.last_run_timestamp;


-- ============================================================
-- ÉTAPE 5 : Libérer le cache et optimiser
-- ============================================================
UNCACHE TABLE silver_customer_enriched;

OPTIMIZE gold.dim_customer         ZORDER BY (CustomerKey);
OPTIMIZE gold.dim_customer_history ZORDER BY (CustomerKey, valid_from);


-- ## Résumé du flux complet
-- ```
-- table_changes('silver.sales_customer', last_version+1)
--         ↓ uniquement les customer_id modifiés
-- CACHE TABLE silver_customer_enriched   ← joins exécutés UNE SEULE FOIS
--         ↓                    ↓
--    INSERT history         MERGE dim_customer
-- (archiver anciennes)    (update + insert nouveaux)
--         ↓
-- MERGE pipeline_control   ← sauvegarder la version
--         ↓
-- UNCACHE + OPTIMIZE