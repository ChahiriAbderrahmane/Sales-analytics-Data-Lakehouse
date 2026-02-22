-- ███████████████████████████████████████████████████████████████████████████
-- 6. sales_customer — SCD TYPE 2
--    Clé       : CustomerID
--    Surveillé : TerritoryID, StoreID, PersonID, AccountNumber
-- ███████████████████████████████████████████████████████████████████████████

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

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.sales_customer AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM sales_customer_ranked b
        WHERE b.customer_id = t.customer_id
    )
WHERE t.is_current = TRUE
  AND t.customer_id IN (SELECT DISTINCT customer_id FROM sales_customer_ranked)
  AND EXISTS (
      SELECT 1 FROM sales_customer_ranked b
      WHERE b.customer_id = t.customer_id AND b.rn = 1
        AND (
            b.territory_id   <> t.territory_id   OR
            b.store_id       <> t.store_id       OR
            b.person_id      <> t.person_id      OR
            b.account_number <> t.account_number
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.sales_customer
SELECT
    b.customer_id, b.person_id, b.store_id, b.territory_id,
    b.account_number, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_customer_ranked b
WHERE b.customer_id IN (
    SELECT DISTINCT b2.customer_id
    FROM sales_customer_ranked b2
    JOIN silver.sales_customer t
        ON b2.customer_id = t.customer_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.territory_id   <> t.territory_id   OR
          b2.store_id       <> t.store_id       OR
          b2.person_id      <> t.person_id      OR
          b2.account_number <> t.account_number
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.sales_customer
SELECT
    b.customer_id, b.person_id, b.store_id, b.territory_id,
    b.account_number, b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_customer_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.sales_customer t
    WHERE t.customer_id = b.customer_id
);

OPTIMIZE silver.sales_customer ZORDER BY (customer_id, territory_id);


SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_customer  RETAIN 2 HOURS;