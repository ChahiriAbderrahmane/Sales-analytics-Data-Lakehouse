-- TABLE COURANTE : 1 ligne par client (version la plus récente)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk             BIGINT      NOT NULL,   -- STABLE : même valeur pour toutes les versions d’un client
    customer_id             INT         NOT NULL,
    account_number          STRING,
    store_id                INT,
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
    valid_to                TIMESTAMP,              -- NULL = toujours valide
    is_current              BOOLEAN     NOT NULL,
    ingestion_timestamp     TIMESTAMP,
    year_valid_from         INT        -- pour partitionnement
)
USING DELTA
PARTITIONED BY (year_valid_from)
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'
);


OPTIMIZE gold.dim_customer ZORDER BY (customer_id, is_current);


-- TABLE HISTORIQUE : seulement les versions fermées (append-only)
CREATE TABLE IF NOT EXISTS gold.dim_customer_history (
    history_sk              BIGINT      , 
    customer_sk             BIGINT      NOT NULL,   -- même valeur que dans dim_customer
    customer_id             INT         NOT NULL,
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
    is_current              BOOLEAN     NOT NULL,   -- toujours FALSE ici
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

OPTIMIZE gold.dim_customer_history ZORDER BY (customer_id, valid_from);


-- =============================================
-- PIPELINE INCRÉMENTAL avec CDF (Change Data Feed)
-- =============================================
######################################################################################""
-- 1. Récupérer les changements depuis silver (CDF)
ALTER TABLE silver.sales_customer SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
-- On utilise la vue _changes générée automatiquement

CREATE OR REPLACE TEMP VIEW customer_changes AS
SELECT
    c.customer_id,
    c.account_number,
    c.store_id,
    p.first_name,
    p.middle_name,
    p.last_name,
    CONCAT_WS(' ', p.first_name, p.middle_name, p.last_name) AS full_name,
    p.person_type,
    p.email_promotion,

    -- Adresse (à adapter selon ton choix précédent)
    a.address_line1,
    a.address_line2,
    a.city,
    a.postal_code,
    sp.state_province_code,
    sp.name AS state_province_name,
    cr.country_region_code,
    cr.name AS country_name,

    c.modified_date AS valid_from,
    c.end_date      AS valid_to,
    c.is_current,

    c._change_type,               -- insert, update_preimage, update_postimage, delete
    c._commit_version,
    c._commit_timestamp

FROM table_changes('silver.sales_customer', 0) c   -- depuis la version 0 ou la dernière version traitée, faut la chnager à chaque fois
-- ou bien créer une table de suivi des versions traitées et faire un JOIN pour ne récupérer que les nouvelles

LEFT JOIN silver.person_person p ON c.person_id = p.business_entity_id AND p.is_current = TRUE
-- ... jointures adresse / state / country comme avant
LEFT JOIN silver.person_address a ON c.address_id = a.address_id AND a.is_current = TRUE
LEFT JOIN silver.person_stateprovince sp ON a.state_province_id = sp.state_province_id AND sp.is_current = TRUE
LEFT JOIN silver.person_countryregion cr ON sp.country_region_code = cr.country_region_code AND cr.is_current = TRUE


WHERE c._change_type IN ('insert', 'update_postimage');  -- on ne traite que les ajouts et mises à jour finales

-- 2. Déplacer l’ancienne version courante vers l’historique (avant update)
INSERT INTO gold.dim_customer_history
SELECT
    monotonically_increasing_id() AS history_sk,
    d.customer_sk,
    d.customer_id,
    d.first_name, d.middle_name, d.last_name, d.full_name,
    d.person_type, d.email_promotion,
    d.address_line1, d.address_line2, d.city, d.postal_code,
    d.state_province_code, d.state_province_name,
    d.country_region_code, d.country_name,
    d.valid_from,
    b.valid_from AS valid_to,          -- la nouvelle version clôture l’ancienne
    FALSE AS is_current,
    current_timestamp() AS closed_timestamp,
    current_timestamp() AS ingestion_timestamp
FROM gold.dim_customer d
JOIN customer_changes b ON d.customer_id = b.customer_id
WHERE d.is_current = TRUE
  AND b._change_type = 'update_postimage';   -- seulement quand il y a un vrai update

-- 3. MERGE : upsert la version courante (stable customer_sk)
MERGE INTO gold.dim_customer AS target
USING (
    SELECT
        -- customer_sk stable : on le récupère si existe, sinon on en crée un nouveau
        COALESCE(
            (SELECT customer_sk FROM gold.dim_customer WHERE customer_id = source.customer_id),
            monotonically_increasing_id()
        ) AS customer_sk,

        source.*,
        YEAR(valid_from) AS year_valid_from

    FROM customer_changes source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _commit_timestamp DESC) = 1   -- dernière version du batch
) AS source

ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET
        target.account_number       = source.account_number,
        target.store_id             = source.store_id,
        target.first_name           = source.first_name,
        target.middle_name          = source.middle_name,
        target.last_name            = source.last_name,
        target.full_name            = source.full_name,
        target.person_type          = source.person_type,
        target.email_promotion      = source.email_promotion,
        target.address_line1        = source.address_line1,
        target.city                 = source.city,
        target.country_name         = source.country_name,
        target.valid_from           = source.valid_from,
        target.valid_to             = NULL,
        target.is_current           = TRUE,
        target.ingestion_timestamp  = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        customer_sk, customer_id, account_number, store_id,
        first_name, middle_name, last_name, full_name,
        person_type, email_promotion,
        address_line1, address_line2, city, postal_code,
        state_province_code, state_province_name,
        country_region_code, country_name,
        valid_from, valid_to, is_current,
        ingestion_timestamp
    )
    VALUES (
        source.customer_sk,
        source.customer_id, source.account_number, source.store_id,
        source.first_name, source.middle_name, source.last_name, source.full_name,
        source.person_type, source.email_promotion,
        source.address_line1, source.address_line2, source.city, source.postal_code,
        source.state_province_code, source.state_province_name,
        source.country_region_code, source.country_name,
        source.valid_from, NULL, TRUE,
        current_timestamp()
    );

-- Ou via VACUUM + retention
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM gold.dim_customer_history RETAIN 1825 HOURS;  -- ~76 jours par défaut, à ajuster