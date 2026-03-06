CREATE TABLE IF NOT EXISTS gold.dim_customer (
    CustomerKey             INT             NOT NULL,
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

-- ============================================================
-- COUCHE GOLD : DIM_CUSTOMER_HISTORY (Table d'archivage)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.dim_customer_history (
    CustomerKey             INT             NOT NULL,
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
    valid_from              TIMESTAMP       NOT NULL,
    valid_to                TIMESTAMP,      
    is_current              BOOLEAN,
    closed_timestamp        TIMESTAMP,      
    ingestion_timestamp     TIMESTAMP,
    year_valid_to           INT             
)
USING DELTA
PARTITIONED BY (year_valid_to)             
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
-- 1. CRÉATION ET REMPLISSAGE DE LA TABLE EDUCATION
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.ref_education_translation (
    EnglishValue STRING,
    SpanishValue STRING,
    FrenchValue  STRING
) USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/ref_education_translation';

-- Suppression des données existantes si ce script est relancé
DELETE FROM gold.ref_education_translation;

-- Insertion des traductions
INSERT INTO gold.ref_education_translation VALUES 
    ('Bachelors', 'Licenciatura', 'Licence'),
    ('Partial College', 'Estudios universitarios parciales', 'Études universitaires partielles'),
    ('High School', 'Educación secundaria', 'Lycée'),
    ('Partial High School', 'Educación secundaria parcial', 'Lycée partiel'),
    ('Graduate Degree', 'Título de posgrado', 'Diplôme d''études supérieures');

-- ============================================================
-- 2. CRÉATION ET REMPLISSAGE DE LA TABLE OCCUPATION
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.ref_occupation_translation (
    EnglishValue STRING,
    SpanishValue STRING,
    FrenchValue  STRING
) USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/ref_occupation_translation';

-- Suppression des données existantes
DELETE FROM gold.ref_occupation_translation;

-- Insertion des traductions
INSERT INTO gold.ref_occupation_translation VALUES 
    ('Professional', 'Profesional', 'Professionnel'),
    ('Management', 'Gestión', 'Cadre'),
    ('Skilled Manual', 'Trabajo manual cualificado', 'Ouvrier qualifié'),
    ('Clerical', 'Administrativo', 'Employé de bureau'),
    ('Manual', 'Trabajo manual', 'Ouvrier');