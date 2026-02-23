-- 6. sales_customer — SCD TYPE 2
--    Clé       : CustomerID
--    Surveillé : TerritoryID, StoreID, PersonID, AccountNumber

CREATE TABLE IF NOT EXISTS silver.sales_customer (
    customer_id             INT,
    person_id               INT,
    store_id                INT,
    territory_id            INT,
    account_number          STRING,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_customer'
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
CREATE OR REPLACE TEMP VIEW sales_customer_ranked AS
SELECT
    CAST(CustomerID     AS INT)       AS customer_id,
    CAST(PersonID       AS INT)       AS person_id,
    CAST(StoreID        AS INT)       AS store_id,
    CAST(TerritoryID    AS INT)       AS territory_id,
    CAST(AccountNumber  AS STRING)    AS account_number,
    CAST(rowguid        AS STRING)    AS rowguid,
    CAST(ModifiedDate   AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY CustomerID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY CustomerID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.sales_customer
WHERE CustomerID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- IS DISTINCT FROM remplace <> pour gérer correctement les valeurs NULL
-- (person_id, store_id, territory_id peuvent être NULL en source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.customer_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM sales_customer_ranked b
JOIN silver.sales_customer t
    ON  b.customer_id = t.customer_id
    AND t.is_current = TRUE
    AND b.rn = 1
WHERE b.territory_id   IS DISTINCT FROM t.territory_id
   OR b.store_id       IS DISTINCT FROM t.store_id
   OR b.person_id      IS DISTINCT FROM t.person_id
   OR b.account_number IS DISTINCT FROM t.account_number
GROUP BY b.customer_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.sales_customer AS t
USING ids_to_close AS s
ON  t.customer_id = s.customer_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_customer
SELECT
    b.customer_id,
    b.person_id,
    b.store_id,
    b.territory_id,
    b.account_number,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_customer_ranked b
JOIN ids_to_close s
    ON b.customer_id = s.customer_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_customer
SELECT
    b.customer_id,
    b.person_id,
    b.store_id,
    b.territory_id,
    b.account_number,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_customer_ranked b
LEFT JOIN silver.sales_customer t
    ON b.customer_id = t.customer_id
WHERE t.customer_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.sales_customer
ZORDER BY (customer_id, territory_id);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_customer RETAIN 168 HOURS;