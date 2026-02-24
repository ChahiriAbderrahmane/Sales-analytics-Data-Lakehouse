-- SCD TYPE 2 — silver.sales_salesorderheader
-- Clé       : sales_order_id
-- Changement détecté sur : sub_total, status_code, total_due
-- conserver la dernirère version active parmi celles dans le fichier à insérer, la meme logique pour les autres table dans 
--       silver sauf quelques unes.

CREATE TABLE IF NOT EXISTS silver.sales_salesorderheader (
    sales_order_id              INT,
    revision_number             TINYINT,
    order_date                  DATE,
    due_date                    DATE,
    ship_date                   DATE,
    status_code                 TINYINT,
    online_order_flag           BOOLEAN,
    sales_order_number          STRING,
    purchase_order_number       STRING,
    account_number              STRING,
    customer_id                 INT,
    sales_person_id             INT,
    territory_id                INT,
    bill_to_address_id          INT,
    ship_to_address_id          INT,
    ship_method_id              INT,
    credit_card_id              INT,
    credit_card_approval_code   STRING,
    currency_rate_id            INT,
    sub_total                   DECIMAL(19,4),
    tax_amt                     DECIMAL(19,4),
    freight                     DECIMAL(19,4),
    total_due                   DECIMAL(19,4),
    comment                     STRING,
    rowguid                     STRING,
    modified_date               DATE,
    _ingestion_timestamp        TIMESTAMP,
    is_current                  BOOLEAN,
    end_date                    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesorderheader'
TBLPROPERTIES (
    'delta.minReaderVersion'                  = '1',
    'delta.minWriterVersion'                  = '2',
    'delta.enableChangeDataFeed'              = 'true',
    'delta.autoOptimize.optimizeWrite'        = 'true',
    'delta.autoOptimize.autoCompact'          = 'true',
    'delta.enableDeletionVectors'             = 'true',
    'delta.columnMapping.mode'                = 'name'
);


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 0A : Lire le fichier source (CSV) et créer une vue
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW raw_source (
    SalesOrderID STRING,
    RevisionNumber STRING,
    OrderDate STRING,
    DueDate STRING,
    ShipDate STRING,
    Status STRING,
    OnlineOrderFlag STRING,
    SalesOrderNumber STRING,
    PurchaseOrderNumber STRING,
    AccountNumber STRING,
    CustomerID STRING,
    SalesPersonID STRING,
    TerritoryID STRING,
    BillToAddressID STRING,
    ShipToAddressID STRING,
    ShipMethodID STRING,
    CreditCardID STRING,
    CreditCardApprovalCode STRING,
    CurrencyRateID STRING,
    SubTotal STRING,
    TaxAmt STRING,
    Freight STRING,
    TotalDue STRING,
    Comment STRING,
    rowguid STRING,
    ModifiedDate STRING
)
USING csv
OPTIONS (
    path '/user/hadoop/sales_data_mart/bronze/Sales_SalesOrderHeader/',
    header 'false',
    sep ','
);

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 0B : Classer les lignes bronze
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW bronze_ranked AS
SELECT
    CAST(SalesOrderID           AS INT)             AS sales_order_id,
    CAST(RevisionNumber         AS TINYINT)         AS revision_number,
    CAST(OrderDate              AS DATE)            AS order_date,
    CAST(DueDate                AS DATE)            AS due_date,
    CAST(ShipDate               AS DATE)            AS ship_date,
    CAST(Status                 AS TINYINT)         AS status_code,
    CAST(OnlineOrderFlag        AS BOOLEAN)         AS online_order_flag,
    CAST(SalesOrderNumber       AS STRING)          AS sales_order_number,
    CAST(PurchaseOrderNumber    AS STRING)          AS purchase_order_number,
    CAST(AccountNumber          AS STRING)          AS account_number,
    CAST(CustomerID             AS INT)             AS customer_id,
    CAST(SalesPersonID          AS INT)             AS sales_person_id,
    CAST(TerritoryID            AS INT)             AS territory_id,
    CAST(BillToAddressID        AS INT)             AS bill_to_address_id,
    CAST(ShipToAddressID        AS INT)             AS ship_to_address_id,
    CAST(ShipMethodID           AS INT)             AS ship_method_id,
    CAST(CreditCardID           AS INT)             AS credit_card_id,
    CAST(CreditCardApprovalCode AS STRING)          AS credit_card_approval_code,
    CAST(CurrencyRateID         AS INT)             AS currency_rate_id,
    ABS(CAST(SubTotal           AS DECIMAL(19,4)))  AS sub_total,
    ABS(CAST(TaxAmt             AS DECIMAL(19,4)))  AS tax_amt,
    ABS(CAST(Freight            AS DECIMAL(19,4)))  AS freight,
    ABS(CAST(TotalDue           AS DECIMAL(19,4)))  AS total_due,
    CAST(Comment                AS STRING)          AS comment,
    CAST(rowguid                AS STRING)          AS rowguid,
    CAST(ModifiedDate           AS DATE)            AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY SalesOrderID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY SalesOrderID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM raw_source
WHERE SalesOrderID IS NOT NULL;


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- On matérialise le résultat dans une TEMP VIEW pour éviter les sous-requêtes
-- dans le MERGE (limitation Delta open source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.sales_order_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM bronze_ranked b
JOIN silver.sales_salesorderheader t
    ON b.sales_order_id = t.sales_order_id
    AND t.is_current = TRUE
WHERE b.sub_total   <> t.sub_total
   OR b.status_code <> t.status_code
   OR b.total_due   <> t.total_due
GROUP BY b.sales_order_id;


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.sales_salesorderheader AS t
USING ids_to_close AS s
ON t.sales_order_id = s.sales_order_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN remplace le NOT EXISTS / IN avec sous-requête
-- ───────────────────────────────────────────────────────────────────────────
-- Étape 3 corrigée : Insérer seulement les versions qui représentent un vrai changement
INSERT INTO silver.sales_salesorderheader
SELECT
    b.sales_order_id,
    b.revision_number,
    b.order_date,
    b.due_date,
    b.ship_date,
    b.status_code,
    b.online_order_flag,
    b.sales_order_number,
    b.purchase_order_number,
    b.account_number,
    b.customer_id,
    b.sales_person_id,
    b.territory_id,
    b.bill_to_address_id,
    b.ship_to_address_id,
    b.ship_method_id,
    b.credit_card_id,
    b.credit_card_approval_code,
    b.currency_rate_id,
    b.sub_total,
    b.tax_amt,
    b.freight,
    b.total_due,
    b.comment,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM (
    -- On ranke chronologiquement (ASC) pour calculer le précédent état
    SELECT 
        *,
        LAG(sub_total)     OVER (PARTITION BY sales_order_id ORDER BY modified_date ASC) AS prev_sub_total,
        LAG(status_code)   OVER (PARTITION BY sales_order_id ORDER BY modified_date ASC) AS prev_status_code,
        LAG(total_due)     OVER (PARTITION BY sales_order_id ORDER BY modified_date ASC) AS prev_total_due
    FROM bronze_ranked
) b
JOIN ids_to_close s ON b.sales_order_id = s.sales_order_id
WHERE 
    -- On insère seulement si cette ligne diffère de la précédente (dans le batch ou de silver)
    (b.sub_total     <> COALESCE(b.prev_sub_total,     0)     OR
     b.status_code   <> COALESCE(b.prev_status_code,   0)     OR
     b.total_due     <> COALESCE(b.prev_total_due,     0))
    -- OU si c'est la première ligne du batch et qu'elle diffère de silver (déjà couvert par ids_to_close)
;
-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_salesorderheader
SELECT
    b.sales_order_id,
    b.revision_number,
    b.order_date,
    b.due_date,
    b.ship_date,
    b.status_code,
    b.online_order_flag,
    b.sales_order_number,
    b.purchase_order_number,
    b.account_number,
    b.customer_id,
    b.sales_person_id,
    b.territory_id,
    b.bill_to_address_id,
    b.ship_to_address_id,
    b.ship_method_id,
    b.credit_card_id,
    b.credit_card_approval_code,
    b.currency_rate_id,
    b.sub_total,
    b.tax_amt,
    b.freight,
    b.total_due,
    b.comment,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM bronze_ranked b
-- LEFT JOIN + IS NULL remplace NOT EXISTS
LEFT JOIN silver.sales_salesorderheader t
    ON b.sales_order_id = t.sales_order_id
WHERE t.sales_order_id IS NULL;


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.sales_salesorderheader
ZORDER BY (customer_id, order_date);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesorderheader RETAIN 168 HOURS;