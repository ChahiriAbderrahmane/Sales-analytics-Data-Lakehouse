-- ============================================================
-- 2. dim_customer_history (versions fermées)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.dim_customer_history (
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
    valid_to                TIMESTAMP   NOT NULL,
    is_current              BOOLEAN     NOT NULL,
    closed_timestamp        TIMESTAMP   NOT NULL,
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
    'delta.columnMapping.mode'         = 'name'
);


-- la vue commune crée dans dim_customer !!!

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

OPTIMIZE gold.dim_customer_history ZORDER BY (CustomerKey, valid_from);