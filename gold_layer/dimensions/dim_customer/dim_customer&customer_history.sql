-- ============================================================
-- ÉTAPE 0 : MATÉRIALISATION PHYSIQUE DU CDF 
-- ============================================================
DROP TABLE IF EXISTS gold.temp_cdf_staging;

CREATE TABLE gold.temp_cdf_staging USING DELTA AS 
SELECT * FROM table_changes('silver.sales_customer', 9)
WHERE _change_type IN ('insert', 'update_postimage');

-- ============================================================
-- ÉTAPE 1 : CACHE (Lecture directe et totale depuis le CDF)
-- ============================================================
CACHE TABLE silver_customer_enriched AS
WITH demographics_parsed AS (
    SELECT
        business_entity_id,
        -- Ton correctif défensif pour le parseur XML
        CASE WHEN trim(demographics) LIKE '<%' THEN regexp_replace(demographics, ' xmlns="[^"]*"', '') ELSE NULL END AS xml_clean
    FROM silver.person_person
    WHERE is_current = TRUE
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
    TO_DATE(xpath_string(d.xml_clean, '//IndividualSurvey/BirthDate')) AS BirthDate,
    xpath_string(d.xml_clean, '//IndividualSurvey/MaritalStatus')      AS MaritalStatus,
    pp.suffix                                               AS Suffix,
    xpath_string(d.xml_clean, '//IndividualSurvey/Gender')             AS Gender,
    ea.email_address                                        AS EmailAddress,
    xpath_string(d.xml_clean, '//IndividualSurvey/YearlyIncome')       AS YearlyIncome,
    CAST(xpath_string(d.xml_clean, '//IndividualSurvey/TotalChildren') AS INT) AS TotalChildren,
    CAST(xpath_string(d.xml_clean, '//IndividualSurvey/NumberChildrenAtHome') AS INT) AS NumberChildrenAtHome,
    xpath_string(d.xml_clean, '//IndividualSurvey/Education')          AS EnglishEducation,
    COALESCE(edu.SpanishValue, xpath_string(d.xml_clean, '//IndividualSurvey/Education')) AS SpanishEducation,
    COALESCE(edu.FrenchValue, xpath_string(d.xml_clean, '//IndividualSurvey/Education'))  AS FrenchEducation,
    xpath_string(d.xml_clean, '//IndividualSurvey/Occupation')         AS EnglishOccupation,
    COALESCE(occ.SpanishValue, xpath_string(d.xml_clean, '//IndividualSurvey/Occupation')) AS SpanishOccupation,
    COALESCE(occ.FrenchValue, xpath_string(d.xml_clean, '//IndividualSurvey/Occupation'))  AS FrenchOccupation,
    xpath_string(d.xml_clean, '//IndividualSurvey/HomeOwnerFlag')      AS HouseOwnerFlag,
    CAST(xpath_string(d.xml_clean, '//IndividualSurvey/NumberCarsOwned') AS INT) AS NumberCarsOwned,
    addr.address_line1                                      AS AddressLine1,
    addr.address_line2                                      AS AddressLine2,
    ph.phone_number                                         AS Phone,
    c.modified_date                                         AS DateFirstPurchase,
    xpath_string(d.xml_clean, '//IndividualSurvey/CommuteDistance')    AS CommuteDistance,
    c.modified_date                                         AS valid_from,
    c.end_date                                              AS valid_to, -- On capte la fin de vie depuis Silver
    c.is_current                                            -- On capte le statut (TRUE/FALSE) depuis Silver
FROM gold.temp_cdf_staging c  
LEFT JOIN silver.person_person pp
    ON c.person_id = pp.business_entity_id AND pp.is_current = TRUE
LEFT JOIN demographics_parsed d
    ON pp.business_entity_id = d.business_entity_id
LEFT JOIN silver.person_emailaddress ea
    ON pp.business_entity_id = ea.business_entity_id AND ea.is_current = TRUE
LEFT JOIN silver.person_personphone ph
    ON pp.business_entity_id = ph.business_entity_id AND ph.is_current = TRUE
LEFT JOIN silver.person_businessentityaddress bea
    ON pp.business_entity_id = bea.business_entity_id AND bea.is_current = TRUE
LEFT JOIN silver.person_address addr
    ON bea.address_id = addr.address_id AND addr.is_current = TRUE
LEFT JOIN silver.person_stateprovince sp
    ON addr.state_province_id = sp.state_province_id AND sp.is_current = TRUE
LEFT JOIN gold.ref_education_translation edu
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Education') = edu.EnglishValue
LEFT JOIN gold.ref_occupation_translation occ
    ON xpath_string(d.xml_clean, '//IndividualSurvey/Occupation') = occ.EnglishValue
WHERE c._change_type IN ('insert', 'update_postimage');

-- ============================================================
-- ÉTAPE 2 : Archiver dans history 
-- ============================================================
-- On prend les lignes "is_current = FALSE" du CDF et on les pousse en archive
INSERT INTO gold.dim_customer_history
SELECT
    CustomerKey, GeographyKey, CustomerAlternateKey,
    Title, FirstName, MiddleName, LastName, NameStyle,
    BirthDate, MaritalStatus, Suffix, Gender,
    EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome,
    EnglishEducation, SpanishEducation, FrenchEducation,
    EnglishOccupation, SpanishOccupation, FrenchOccupation,
    HouseOwnerFlag, NumberCarsOwned,
    AddressLine1, AddressLine2,
    Phone, DateFirstPurchase, CommuteDistance,
    valid_from,
    COALESCE(valid_to, current_timestamp()) AS valid_to,
    FALSE                                   AS is_current,
    current_timestamp()                     AS closed_timestamp,
    current_timestamp()                     AS ingestion_timestamp,
    YEAR(COALESCE(valid_to, current_timestamp())) AS year_valid_to
FROM silver_customer_enriched
WHERE is_current = FALSE;


-- ============================================================
-- ÉTAPE 3 : MERGE dim_customer (Uniquement les actifs)
-- ============================================================
MERGE INTO gold.dim_customer tgt
USING (SELECT * FROM silver_customer_enriched WHERE is_current = TRUE) src
ON tgt.CustomerKey = src.CustomerKey
WHEN MATCHED THEN UPDATE SET
    tgt.GeographyKey = src.GeographyKey, tgt.Title = src.Title,
    tgt.FirstName = src.FirstName, tgt.MiddleName = src.MiddleName,
    tgt.LastName = src.LastName, tgt.NameStyle = src.NameStyle,
    tgt.BirthDate = src.BirthDate, tgt.MaritalStatus = src.MaritalStatus,
    tgt.Suffix = src.Suffix, tgt.Gender = src.Gender,
    tgt.EmailAddress = src.EmailAddress, tgt.YearlyIncome = src.YearlyIncome,
    tgt.TotalChildren = src.TotalChildren,
    tgt.NumberChildrenAtHome = src.NumberChildrenAtHome,
    tgt.EnglishEducation = src.EnglishEducation,
    tgt.SpanishEducation = src.SpanishEducation,
    tgt.FrenchEducation = src.FrenchEducation,
    tgt.EnglishOccupation = src.EnglishOccupation,
    tgt.SpanishOccupation = src.SpanishOccupation,
    tgt.FrenchOccupation = src.FrenchOccupation,
    tgt.HouseOwnerFlag = src.HouseOwnerFlag,
    tgt.NumberCarsOwned = src.NumberCarsOwned,
    tgt.AddressLine1 = src.AddressLine1, tgt.AddressLine2 = src.AddressLine2,
    tgt.Phone = src.Phone, tgt.DateFirstPurchase = src.DateFirstPurchase,
    tgt.CommuteDistance = src.CommuteDistance,
    tgt.valid_from = src.valid_from,
    tgt.is_current = TRUE,
    tgt.ingestion_timestamp = current_timestamp()
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
-- ÉTAPE 4 : Mettre à jour pipeline_control
-- ============================================================
MERGE INTO gold.pipeline_control tgt
USING (
    SELECT
        'silver.sales_customer' AS table_name,
        12                      AS last_version,
        current_timestamp()     AS last_run_timestamp
) src
ON tgt.table_name = src.table_name
WHEN MATCHED THEN UPDATE SET
    tgt.last_version       = src.last_version,
    tgt.last_run_timestamp = src.last_run_timestamp;

-- ============================================================
-- ÉTAPE 5 : Nettoyage Final
-- ============================================================
UNCACHE TABLE silver_customer_enriched;
DROP TABLE IF EXISTS gold.temp_cdf_staging; 
OPTIMIZE gold.dim_customer         ZORDER BY (CustomerKey);
OPTIMIZE gold.dim_customer_history ZORDER BY (CustomerKey, valid_from);