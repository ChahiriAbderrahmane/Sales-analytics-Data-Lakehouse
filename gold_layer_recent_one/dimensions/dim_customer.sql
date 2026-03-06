-- ============================================================
-- Table de mapping statique pour les traductions
--    (ces valeurs viennent d'AdventureWorksDW directement)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.ref_education_translation (
    EnglishValue    STRING,
    SpanishValue    STRING,
    FrenchValue     STRING
) USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/ref_education_translation';

INSERT INTO gold.ref_education_translation VALUES
    ('Bachelors',           'Licenciatura',             'Bac + 4'),
    ('Graduate Degree',     'Postgrado',                'Bac + 5'),
    ('High School',         'Bachillerato',             'Lycée'),
    ('Partial College',     'Estudios universitarios',  'Etudes universitaires'),
    ('Partial High School', 'Estudios de bachillerato', 'Etudes de lycée');

CREATE TABLE IF NOT EXISTS gold.ref_occupation_translation (
    EnglishValue    STRING,
    SpanishValue    STRING,
    FrenchValue     STRING
) USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/ref_occupation_translation';

INSERT INTO gold.ref_occupation_translation VALUES
    ('Management',      'Gestión',      'Direction'),
    ('Professional',    'Profesional',  'Cadre'),
    ('Skilled Manual',  'Artesano',     'Technicien'),
    ('Clerical',        'Administrativo','Administratif'),
    ('Manual',          'Manual',       'Ouvrier');

-- ============================================================
-- 1. dim_customer (versions courantes)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    CustomerKey             INT         NOT NULL,
    GeographyKey            INT,
    CustomerAlternateKey    STRING,
    Title                   STRING,
    FirstName               STRING,
    MiddleName              STRING,
    LastName                STRING,
    NameStyle               BOOLEAN,
    BirthDate               DATE,
    MaritalStatus           STRING,
    Suffix                  STRING,
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
    valid_from              TIMESTAMP   NOT NULL,
    is_current              BOOLEAN     NOT NULL,
    ingestion_timestamp     TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableDeletionVectors'      = 'true',
    'delta.columnMapping.mode'         = 'name'
);

-- ============================================================
-- 3. Vue commune enrichie
-- ============================================================
CREATE OR REPLACE TEMP VIEW silver_customer_enriched AS
WITH demographics_parsed AS (
    SELECT
        pp.business_entity_id,
        -- Suppression du namespace une seule fois pour toute la vue
        regexp_replace(pp.demographics, ' xmlns="[^"]*"', '') AS xml_clean
    FROM silver.person_person pp
    WHERE pp.is_current = TRUE
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

    -- Extraction XML depuis demographics_parsed (namespace déjà retiré)
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

    -- Education avec traductions depuis la table de mapping
    xpath_string(d.xml_clean,
        '//IndividualSurvey/Education')                     AS EnglishEducation,
    COALESCE(edu.SpanishValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Education'))
                                                            AS SpanishEducation,
    COALESCE(edu.FrenchValue,
        xpath_string(d.xml_clean, '//IndividualSurvey/Education'))
                                                            AS FrenchEducation,

    -- Occupation avec traductions depuis la table de mapping
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
    c.is_current,
    c._ingestion_timestamp

FROM silver.sales_customer c
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
-- Joins traductions
LEFT JOIN gold.ref_education_translation edu
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Education') = edu.EnglishValue
LEFT JOIN gold.ref_occupation_translation occ
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Occupation') = occ.EnglishValue;

-- ============================================================
-- 4. INSERT dim_customer (courant)
-- ============================================================
INSERT INTO gold.dim_customer
SELECT
    CustomerKey, GeographyKey, CustomerAlternateKey,
    Title, FirstName, MiddleName, LastName, NameStyle,
    BirthDate, MaritalStatus, Suffix, Gender,
    EmailAddress, YearlyIncome,
    TotalChildren, NumberChildrenAtHome,
    EnglishEducation, SpanishEducation, FrenchEducation,
    EnglishOccupation, SpanishOccupation, FrenchOccupation,
    HouseOwnerFlag, NumberCarsOwned,
    AddressLine1, AddressLine2,
    Phone, DateFirstPurchase, CommuteDistance,
    valid_from,
    TRUE                    AS is_current,
    current_timestamp()     AS ingestion_timestamp
FROM silver_customer_enriched
WHERE is_current = TRUE;

-- ============================================================
-- 5. INSERT dim_customer_history (fermées)
-- ============================================================
INSERT INTO gold.dim_customer_history
SELECT
    CustomerKey, GeographyKey, CustomerAlternateKey,
    Title, FirstName, MiddleName, LastName, NameStyle,
    BirthDate, MaritalStatus, Suffix, Gender,
    EmailAddress, YearlyIncome,
    TotalChildren, NumberChildrenAtHome,
    EnglishEducation, SpanishEducation, FrenchEducation,
    EnglishOccupation, SpanishOccupation, FrenchOccupation,
    HouseOwnerFlag, NumberCarsOwned,
    AddressLine1, AddressLine2,
    Phone, DateFirstPurchase, CommuteDistance,
    valid_from,
    current_timestamp()         AS valid_to,
    FALSE                       AS is_current,
    current_timestamp()         AS closed_timestamp,
    current_timestamp()         AS ingestion_timestamp,
    YEAR(current_timestamp())   AS year_valid_to
FROM silver_customer_enriched
WHERE is_current = FALSE;

-- ============================================================
-- 6. Optimisation
-- ============================================================
OPTIMIZE gold.dim_customer ZORDER BY (CustomerKey);
