-- ═══════════════════════════════════════════════════════════════════════════
-- SCD TYPE 2 — silver.sales_salesorderheader
-- Source    : Fichier CSV ou Parquet lu au vol
-- Clé       : sales_order_id
-- Ordre     : modified_date DESC
-- Changement détecté sur : sub_total, status_code, total_due
-- Résultat  : toutes les versions conservées, seule la dernière est active
-- ═══════════════════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE PRÉALABLE : Créer la table silver si elle n'existe pas encore
-- ───────────────────────────────────────────────────────────────────────────
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
-- ÉTAPE 0A : Lire le fichier source (CSV ou Parquet) et créer une vue
--
--   👉 Pour un CSV   → remplace FORMAT='csv' et adapte les options
--   👉 Pour un Parquet → remplace FORMAT='parquet' (pas besoin des options CSV)
--
--   Remplace le chemin '/chemin/vers/ton/fichier/' par ton vrai chemin.
-- ───────────────────────────────────────────────────────────────────────────

-- ── Option A : source CSV ──────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW raw_source AS
SELECT *
FROM read_files(
    '/chemin/vers/ton/fichier/*.csv',   -- 👈 adapte ce chemin
    FORMAT         => 'csv',
    HEADER         => 'true',
    INFERSCHEMA    => 'true',
    SEP            => ','               -- adapte le séparateur si besoin (';', '|', etc.)
);

-- ── Option B : source Parquet (commente A, décommente B) ──────────────────
-- CREATE OR REPLACE TEMP VIEW raw_source AS
-- SELECT *
-- FROM read_files(
--     '/chemin/vers/ton/fichier/*.parquet',   -- 👈 adapte ce chemin
--     FORMAT => 'parquet'
-- );


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 0B : Nettoyer, renommer et classer les lignes du fichier source
--            ROW_NUMBER → 1 = version la plus récente
--            LEAD        → calcule la end_date de chaque version intermédiaire
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW bronze_ranked AS
SELECT
    CAST(SalesOrderID           AS INT)            AS sales_order_id,
    CAST(RevisionNumber         AS TINYINT)        AS revision_number,
    CAST(OrderDate              AS DATE)           AS order_date,
    CAST(DueDate                AS DATE)           AS due_date,
    CAST(ShipDate               AS DATE)           AS ship_date,
    CAST(Status                 AS TINYINT)        AS status_code,
    CAST(OnlineOrderFlag        AS BOOLEAN)        AS online_order_flag,
    CAST(SalesOrderNumber       AS STRING)         AS sales_order_number,
    CAST(PurchaseOrderNumber    AS STRING)         AS purchase_order_number,
    CAST(AccountNumber          AS STRING)         AS account_number,
    CAST(CustomerID             AS INT)            AS customer_id,
    CAST(SalesPersonID          AS INT)            AS sales_person_id,
    CAST(TerritoryID            AS INT)            AS territory_id,
    CAST(BillToAddressID        AS INT)            AS bill_to_address_id,
    CAST(ShipToAddressID        AS INT)            AS ship_to_address_id,
    CAST(ShipMethodID           AS INT)            AS ship_method_id,
    CAST(CreditCardID           AS INT)            AS credit_card_id,
    CAST(CreditCardApprovalCode AS STRING)         AS credit_card_approval_code,
    CAST(CurrencyRateID         AS INT)            AS currency_rate_id,
    ABS(CAST(SubTotal           AS DECIMAL(19,4))) AS sub_total,
    ABS(CAST(TaxAmt             AS DECIMAL(19,4))) AS tax_amt,
    ABS(CAST(Freight            AS DECIMAL(19,4))) AS freight,
    ABS(CAST(TotalDue           AS DECIMAL(19,4))) AS total_due,
    CAST(Comment                AS STRING)         AS comment,
    CAST(rowguid                AS STRING)         AS rowguid,
    CAST(ModifiedDate           AS DATE)           AS modified_date,

    -- Rang : 1 = version la plus récente pour ce sales_order_id
    ROW_NUMBER() OVER (
        PARTITION BY SalesOrderID
        ORDER BY ModifiedDate DESC
    ) AS rn,

    -- end_date de cette version = modified_date de la version qui la remplace
    -- (LEAD du plus récent → plus ancien donne la date "juste après" chronologiquement)
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY SalesOrderID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date

FROM raw_source
WHERE SalesOrderID IS NOT NULL;


-- ───────────────────────────────────────────────────────────────────────────
-- VÉRIFICATION (optionnel, à commenter en prod)
-- Affiche les lignes classées pour contrôle visuel avant d'écrire dans silver
-- ───────────────────────────────────────────────────────────────────────────
-- SELECT sales_order_id, sub_total, status_code, total_due, modified_date, rn, next_version_date
-- FROM bronze_ranked
-- ORDER BY sales_order_id, rn;


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Fermer les lignes actives dans silver
--           Condition : l'ID existe dans bronze ET au moins une valeur a changé
--           On ferme à la date de la version bronze la plus ancienne qui arrive
-- ───────────────────────────────────────────────────────────────────────────
UPDATE silver.sales_salesorderheader AS t
SET
    is_current = FALSE,
    end_date   = (
        -- Date de début de la première version bronze = moment où l'ancienne ligne "expire"
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM bronze_ranked b
        WHERE b.sales_order_id = t.sales_order_id
    )
WHERE t.is_current = TRUE
  AND t.sales_order_id IN (SELECT DISTINCT sales_order_id FROM bronze_ranked)
  -- On ne ferme QUE si un vrai changement est détecté (évite les doublons inutiles)
  AND EXISTS (
      SELECT 1
      FROM bronze_ranked b
      WHERE b.sales_order_id = t.sales_order_id
        AND b.rn = 1   -- on compare avec la version la plus récente du batch
        AND (
            b.sub_total   <> t.sub_total   OR
            b.status_code <> t.status_code OR
            b.total_due   <> t.total_due
        )
  );


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Insérer toutes les versions bronze pour les IDs qui ont changé
--           Chaque version intermédiaire est fermée (is_current = FALSE)
--           Seule la version rn = 1 est active (is_current = TRUE)
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
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date

FROM bronze_ranked b
-- Seulement les IDs dont la dernière version a un changement détecté
WHERE b.sales_order_id IN (
    SELECT DISTINCT b2.sales_order_id
    FROM bronze_ranked b2
    JOIN silver.sales_salesorderheader t
        ON b2.sales_order_id = t.sales_order_id
        -- is_current = FALSE car on vient de les fermer à l'étape 1
        AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.sub_total   <> t.sub_total   OR
          b2.status_code <> t.status_code OR
          b2.total_due   <> t.total_due
      )
);


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer les nouveaux IDs (jamais vus dans silver)
--           Même logique : toutes leurs versions, seule la dernière est active
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
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date

FROM bronze_ranked b
-- L'ID n'existe nulle part dans silver (ni actif ni fermé)
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.sales_salesorderheader t
    WHERE t.sales_order_id = b.sales_order_id
);


-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Optimisation Delta Lake
-- ───────────────────────────────────────────────────────────────────────────

-- Regroupe les petits fichiers Parquet et optimise les lectures filtrées
OPTIMIZE silver.sales_salesorderheader
ZORDER BY (customer_id, order_date);

-- ⚠️ RETAIN 2 HOURS est dangereux en production (pas de time travel possible)
-- En production → remplace par RETAIN 168 HOURS (7 jours)
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesorderheader RETAIN 2 HOURS;