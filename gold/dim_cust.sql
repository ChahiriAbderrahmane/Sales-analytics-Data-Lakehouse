CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id             INT         NOT NULL,
    account_number          STRING,
    store_id                INT,
    territory_id            INT,
    first_name              STRING,
    middle_name             STRING,
    last_name               STRING,
    full_name               STRING,
    person_type             STRING,
    email_promotion         INT,
    address_line1           STRING,
    address_line2           STRING,
    city                    STRING,
    postal_code             STRING,
    state_province_code     STRING,
    state_province_name     STRING,
    country_region_code     STRING,
    country_name            STRING,
    valid_from              TIMESTAMP   NOT NULL,
    valid_to                TIMESTAMP,              -- NULL = courant
    is_current              BOOLEAN     NOT NULL,
    ingestion_timestamp     TIMESTAMP,
    year_valid_from         INT
)
USING DELTA
PARTITIONED BY (year_valid_from)
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
);

CREATE TABLE IF NOT EXISTS gold.dim_customer_history (
    customer_id             INT         NOT NULL,
    account_number          STRING,
    store_id                INT,
    territory_id            INT,
    first_name              STRING,
    middle_name             STRING,
    last_name               STRING,
    full_name               STRING,
    person_type             STRING,
    email_promotion         INT,
    address_line1           STRING,
    address_line2           STRING,
    city                    STRING,
    postal_code             STRING,
    state_province_code     STRING,
    state_province_name     STRING,
    country_region_code     STRING,
    country_name            STRING,
    valid_from              TIMESTAMP   NOT NULL,
    valid_to                TIMESTAMP   NOT NULL,
    is_current              BOOLEAN     NOT NULL,   -- toujours FALSE
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

-- **********

-- Vue enrichie commune pour l'initialisation

DROP TABLE IF EXISTS tmp.silver_enriched_full;


CREATE OR REPLACE TABLE tmp.silver_enriched_full
USING DELTA AS
SELECT
    c.customer_id,
    c.account_number,
    c.store_id,
    c.territory_id,
    p.first_name,
    p.middle_name,
    p.last_name,
    p.person_type,
    p.email_promotion,
    a.address_line1,
    a.address_line2,
    a.city,
    a.postal_code,
    sp.state_province_code,
    sp.name              AS state_province_name,
    cr.country_region_code,
    cr.name              AS country_name,
    c.modified_date      AS valid_from,
    c.end_date           AS valid_to,
    c.is_current,
    c._ingestion_timestamp
FROM silver.sales_customer c
LEFT JOIN silver.person_person        p  ON c.person_id            = p.business_entity_id     AND p.is_current  = TRUE
LEFT JOIN silver.person_address       a  ON c.customer_id          = a.business_entity_id     AND a.is_current  = TRUE
LEFT JOIN silver.person_stateprovince sp ON a.state_province_id    = sp.state_province_id     AND sp.is_current = TRUE
LEFT JOIN silver.person_countryregion cr ON sp.country_region_code = cr.country_region_code   AND cr.is_current = TRUE;


-- 2A : Remplir dim_customer (versions courantes uniquement)
INSERT INTO gold.dim_customer
SELECT
    customer_id,
    account_number,
    store_id,
    territory_id,
    first_name,
    middle_name,
    last_name,
    person_type,
    email_promotion,
    address_line1,
    address_line2,
    city,
    postal_code,
    state_province_code,
    state_province_name,
    country_region_code,
    country_name,
    valid_from,
    NULL                    AS valid_to,    -- courant → NULL
    TRUE                    AS is_current,
    current_timestamp()     AS ingestion_timestamp,
    YEAR(valid_from)        AS year_valid_from
FROM silver_enriched_full
WHERE is_current = TRUE;


-- 2B : Remplir dim_customer_history (versions fermées uniquement)
INSERT INTO gold.dim_customer_history
SELECT
    customer_id,
    account_number,
    store_id,
    territory_id,
    first_name,
    middle_name,
    last_name,
    person_type,
    email_promotion,
    address_line1,
    address_line2,
    city,
    postal_code,
    state_province_code,
    state_province_name,
    country_region_code,
    country_name,
    valid_from,
    valid_to,
    FALSE                   AS is_current,
    current_timestamp()     AS closed_timestamp,
    current_timestamp()     AS ingestion_timestamp,
    YEAR(valid_to)          AS year_valid_to
FROM silver_enriched_full
WHERE is_current = FALSE;


DROP TABLE IF EXISTS tmp.silver_enriched_full;


OPTIMIZE gold.dim_customer ZORDER BY (customer_id);
OPTIMIZE gold.dim_customer_history ZORDER BY (customer_id, valid_from);