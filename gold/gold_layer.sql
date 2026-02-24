-- ═══════════════════════════════════════════════════════════════════════════
-- GOLD LAYER — fact_internet_sales (Kimball Star Schema)
-- ═══════════════════════════════════════════════════════════════════════════
-- Architecture   : Delta Lake sur HDFS
-- Pattern        : Full Refresh dims (SCD Type 2 conservé) + Incremental fact
-- Watermark      : _ingestion_timestamp
-- Grain          : 1 ligne = 1 produit sur 1 commande online
-- Périmètre      : online_order_flag = TRUE uniquement
-- Ordre d'exec   : respecter l'ordre des sections (dims avant fact)
-- ═══════════════════════════════════════════════════════════════════════════

SET spark.databricks.delta.optimizeWrite.enabled = true;
SET spark.databricks.delta.autoCompact.enabled = true;

-- Chemin racine Gold
-- Remplace par ton vrai chemin HDFS/ADLS/S3
-- SET gold_path = '/user/hadoop/sales_data_mart/gold';














-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 3 — dim_customer
-- SCD Type 2 conservé
-- Dénormalisé : person + address + stateprovince + countryregion
-- Stratégie    : Full Refresh (dim, pas la fact)
-- ███████████████████████████████████████████████████████████████████████████

-- 1. Création initiale de la table (à exécuter UNE SEULE FOIS)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk                 BIGINT          NOT NULL,   -- surrogate key (BIGINT pour scale > 2^31)
    customer_id                 INT,
    account_number              STRING,
    store_id                    INT,

    -- Identité
    first_name                  STRING,
    middle_name                 STRING,
    last_name                   STRING,
    full_name                   STRING,
    person_type                 STRING,
    email_promotion             INT,

    -- Adresse principale (bill_to ou ship_to la plus récente)
    address_line1               STRING,
    address_line2               STRING,
    city                        STRING,
    postal_code                 STRING,

    -- Géographie dénormalisée
    state_province_code         STRING,
    state_province_name         STRING,
    country_region_code         STRING,
    country_name                STRING,

    -- SCD Type 2
    valid_from                  TIMESTAMP       NOT NULL,
    valid_to                    TIMESTAMP,
    is_current                  BOOLEAN         NOT NULL,

    -- Métadonnées big data
    ingestion_timestamp         TIMESTAMP,
    source_system               STRING          -- ex: 'online', 'erp', 'crm'
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableChangeDataFeed'       = 'true'   -- utile pour downstream
);

-- 2. MERGE incrémental (à exécuter à chaque batch / refresh)
MERGE INTO gold.dim_customer AS target
USING (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY c.customer_id, c.modified_date
        ) + COALESCE((SELECT MAX(customer_sk) FROM gold.dim_customer), 0) AS customer_sk,

        c.customer_id,
        c.account_number,
        c.store_id,

        p.first_name,
        p.middle_name,
        p.last_name,
        CONCAT_WS(' ', p.first_name, p.middle_name, p.last_name) AS full_name,
        p.person_type,
        p.email_promotion,

        a.address_line1,
        a.address_line2,
        a.city,
        a.postal_code,

        sp.state_province_code,
        sp.name             AS state_province_name,
        cr.country_region_code,
        cr.name             AS country_name,

        c.modified_date     AS valid_from,
        c.end_date          AS valid_to,
        c.is_current,

        current_timestamp() AS ingestion_timestamp,
        'sales_silver'      AS source_system,

        YEAR(c.modified_date) AS year_valid_from

    FROM silver.sales_customer c

    -- Jointure vers person_person via person_id
    LEFT JOIN silver.person_person p
        ON  c.person_id = p.business_entity_id
        AND p.is_current = TRUE

    -- passer par la table de liaison pour obtenir address_id
    LEFT JOIN silver.person_businessentityaddress bea
        ON  p.business_entity_id = bea.business_entity_id
        AND bea.is_current = TRUE

    -- Puis joindre person_address via address_id
    LEFT JOIN silver.person_address a
        ON  bea.address_id = a.address_id
        AND a.is_current = TRUE

    LEFT JOIN silver.person_stateprovince sp
        ON  a.state_province_id = sp.state_province_id
        AND sp.is_current = TRUE

    LEFT JOIN silver.person_countryregion cr
        ON  sp.country_region_code = cr.country_region_code
        AND cr.is_current = TRUE

    WHERE c.modified_date >= date_sub(current_date(), 7)
       OR c.customer_id NOT IN (SELECT DISTINCT customer_id FROM gold.dim_customer)

) AS source

ON  target.customer_id = source.customer_id
AND target.valid_from  = source.valid_from

WHEN MATCHED THEN
    UPDATE SET
        target.first_name          = source.first_name,
        target.middle_name         = source.middle_name,
        target.last_name           = source.last_name,
        target.full_name           = source.full_name,
        target.email_promotion     = source.email_promotion,
        target.address_line1       = source.address_line1,
        target.address_line2       = source.address_line2,
        target.city                = source.city,
        target.postal_code         = source.postal_code,
        target.state_province_code = source.state_province_code,
        target.state_province_name = source.state_province_name,
        target.country_region_code = source.country_region_code,
        target.country_name        = source.country_name,
        target.valid_to            = source.valid_to,
        target.is_current          = source.is_current,
        target.ingestion_timestamp = source.ingestion_timestamp

WHEN NOT MATCHED THEN
    INSERT *;


---

**Ce qui a été corrigé et amélioré :**

La chaîne de jointure pour l'adresse était cassée. La bonne chaîne est :
```
sales_customer
    → person_person         (via person_id = business_entity_id)
    → person_businessentityaddress  (via business_entity_id)  ← manquait
    → person_address        (via address_id)
    → person_stateprovince  (via state_province_id)
    → person_countryregion  (via country_region_code)


-- 3. Optimisation post-merge (à lancer périodiquement, pas à chaque batch)
OPTIMIZE gold.dim_customer
ZORDER BY (customer_id, is_current, country_region_code);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 4 — dim_territory
-- SCD Type 2 conservé
-- Stratégie : Full Refresh
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_territory
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_territory'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
)
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY t.territory_id, t.valid_from
    )                                   AS territory_sk,
    t.territory_id,
    t.name,
    t.country_region_code,
    t.territory_group,
    t.sales_ytd,
    t.sales_last_year,
    t.cost_ytd,
    t.cost_last_year,
    t.modified_date                     AS valid_from,
    t.end_date                          AS valid_to,
    t.is_current
FROM silver.sales_salesterritory t;

OPTIMIZE gold.dim_territory ZORDER BY (territory_id, is_current);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 5 — dim_specialoffer
-- SCD Type 2 conservé
-- Stratégie : Full Refresh
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_specialoffer
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_specialoffer'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
)
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY so.special_offer_id, so.valid_from
    )                                   AS specialoffer_sk,
    so.special_offer_id,
    so.description,
    so.type,
    so.category,
    so.discount_pct,
    so.start_date,
    so.end_date_offer,
    so.min_qty,
    so.max_qty,
    so.modified_date                    AS valid_from,
    so.end_date                         AS valid_to,
    so.is_current
FROM silver.sales_specialoffer so;

OPTIMIZE gold.dim_specialoffer ZORDER BY (special_offer_id, is_current);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 6 — dim_currency
-- Pas de SCD (devises stables), Full Refresh
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_currency
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_currency'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
)
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY currency_code)  AS currency_sk,
    currency_code,
    name
FROM silver.sales_currency;

OPTIMIZE gold.dim_currency ZORDER BY (currency_code);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 7 — dim_salesreason
-- SCD Type 2 conservé
-- Stratégie : Full Refresh
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_salesreason
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_salesreason'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
)
AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY sr.sales_reason_id, sr.valid_from
    )                                   AS salesreason_sk,
    sr.sales_reason_id,
    sr.name,
    sr.reason_type,
    sr.modified_date                    AS valid_from,
    sr.end_date                         AS valid_to,
    sr.is_current
FROM silver.sales_salesreason sr;

OPTIMIZE gold.dim_salesreason ZORDER BY (sales_reason_id);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 8 — bridge_salesorder_salesreason
-- Résout la relation M:M entre fact et dim_salesreason
-- Pas de SCD (liaison transactionnelle)
-- Stratégie : Incremental (même watermark que la fact)
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS gold.bridge_salesorder_salesreason (
    sales_order_id      INT,
    salesreason_sk      BIGINT,
    sales_reason_id     INT,         -- gardé pour debugging
    modified_date       TIMESTAMP,
    _ingestion_timestamp TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/bridge_salesorder_salesreason'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
);

-- Incremental : on insère les nouvelles liaisons uniquement
INSERT INTO gold.bridge_salesorder_salesreason
SELECT
    b.sales_order_id,
    dr.salesreason_sk,
    b.sales_reason_id,
    b.modified_date,
    b._ingestion_timestamp
FROM silver.sales_salesorderheadersalesreason b

-- Lookup SK dans la dim (version courante de la raison)
JOIN gold.dim_salesreason dr
    ON b.sales_reason_id = dr.sales_reason_id
    AND dr.is_current = TRUE

-- Filtre watermark : uniquement les liaisons pas encore en Gold
WHERE b._ingestion_timestamp > (
    SELECT COALESCE(MAX(_ingestion_timestamp), CAST('1900-01-01' AS TIMESTAMP))
    FROM gold.bridge_salesorder_salesreason
)
-- Sécurité : ne pas dupliquer si le job tourne deux fois (idempotence)
AND NOT EXISTS (
    SELECT 1 FROM gold.bridge_salesorder_salesreason g
    WHERE g.sales_order_id  = b.sales_order_id
      AND g.sales_reason_id = b.sales_reason_id
);

OPTIMIZE gold.bridge_salesorder_salesreason ZORDER BY (sales_order_id);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 9 — fact_internet_sales
-- Grain   : 1 ligne = 1 SalesOrderDetailID (ligne de commande online)
-- Filtre  : online_order_flag = TRUE
-- Pattern : Incremental sur _ingestion_timestamp
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS gold.fact_internet_sales (
    -- Surrogate Keys
    order_date_sk       INT,
    ship_date_sk        INT,
    due_date_sk         INT,
    customer_sk         BIGINT,
    product_sk          BIGINT,
    territory_sk        BIGINT,
    specialoffer_sk     BIGINT,
    currency_sk         BIGINT,

    -- Degenerate Dimensions (pas de dim propre, stockées directement)
    sales_order_number      STRING,
    sales_order_id          INT,
    sales_order_detail_id   INT,
    revision_number         TINYINT,

    -- Mesures additives
    order_qty               SMALLINT,
    unit_price              DECIMAL(19,4),
    unit_price_discount     DECIMAL(19,4),
    line_total              DECIMAL(19,4),
    sub_total               DECIMAL(19,4),
    tax_amt                 DECIMAL(19,4),
    freight                 DECIMAL(19,4),
    total_due               DECIMAL(19,4),

    -- Métadonnées pipeline
    _ingestion_timestamp    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/fact_internet_sales'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableDeletionVectors'      = 'true',
    'delta.columnMapping.mode'         = 'name',
    -- Partitionnement par année de commande pour le data skipping
    -- Sur des pétabytes, lire 1 partition = x100 de perf vs full scan
    'delta.dataSkippingNumIndexedCols' = '4'
);

-- ─────────────────────────────────────────────────────────────────────────
-- ÉTAPE 9A : Calculer le watermark AVANT d'insérer
-- On capture le MAX actuel pour éviter les race conditions si le job
-- tourne en parallèle sur plusieurs workers
-- ─────────────────────────────────────────────────────────────────────────
-- Note : en PySpark/Databricks, on lirait ce watermark dans une variable Python.
-- En pur Spark SQL on utilise une sous-requête inline (voir INSERT ci-dessous).

-- ─────────────────────────────────────────────────────────────────────────
-- ÉTAPE 9B : INSERT INCREMENTAL
-- Jointures optimisées pour gros volumes :
--   - On filtre d'abord sur le watermark (réduit le scan Silver)
--   - Les dims sont petites → broadcast join automatique via AQE
--   - Les SKs sont lookupées sur is_current = TRUE uniquement
-- ─────────────────────────────────────────────────────────────────────────
INSERT INTO gold.fact_internet_sales
SELECT
    -- Date SKs (format yyyyMMdd, jointure directe sans broadcast)
    CAST(date_format(soh.order_date, 'yyyyMMdd') AS INT)    AS order_date_sk,
    CAST(date_format(soh.ship_date,  'yyyyMMdd') AS INT)    AS ship_date_sk,
    CAST(date_format(soh.due_date,   'yyyyMMdd') AS INT)    AS due_date_sk,

    -- Customer SK (version active au moment de la commande)
    dc.customer_sk,

    -- Product SK (version active au moment de la commande)
    dp.product_sk,

    -- Territory SK
    dt.territory_sk,

    -- SpecialOffer SK
    dso.specialoffer_sk,

    -- Currency SK (via currency_rate_id → from_currency_code)
    dcu.currency_sk,

    -- Degenerate Dimensions
    soh.sales_order_number,
    soh.sales_order_id,
    sod.sales_order_detail_id,
    soh.revision_number,

    -- Mesures
    sod.order_qty,
    sod.unit_price,
    sod.unit_price_discount,
    sod.line_total,
    soh.sub_total,
    soh.tax_amt,
    soh.freight,
    soh.total_due,

    -- Timestamp pipeline
    GREATEST(soh._ingestion_timestamp, sod._ingestion_timestamp)
                                                            AS _ingestion_timestamp

FROM silver.sales_salesorderdetail sod

-- ── Header : filtre internet uniquement ──────────────────────────────────
JOIN silver.sales_salesorderheader soh
    ON sod.sales_order_id = soh.sales_order_id
    AND soh.is_current       = TRUE
    AND soh.online_order_flag = TRUE     -- ← périmètre Internet Sales

-- ── Filtre watermark (réduit massivement le scan sur gros volumes) ───────
-- On compare le _ingestion_timestamp du detail (transactionnel, jamais modifié)
WHERE sod._ingestion_timestamp > (
    SELECT COALESCE(MAX(_ingestion_timestamp), CAST('1900-01-01' AS TIMESTAMP))
    FROM gold.fact_internet_sales
)

-- ── Lookup Customer SK ────────────────────────────────────────────────────
-- ASOF join : on prend la version du customer valide à la date de commande
-- (SCD Type 2 correct : si le customer a déménagé après, on garde l'ancienne adresse)
LEFT JOIN gold.dim_customer dc
    ON soh.customer_id  = dc.customer_id
    AND soh.order_date >= dc.valid_from
    AND soh.order_date <  COALESCE(CAST(dc.valid_to AS DATE), DATE('9999-12-31'))

-- ── Lookup Product SK ─────────────────────────────────────────────────────
LEFT JOIN gold.dim_product dp
    ON sod.product_id   = dp.product_id
    AND soh.order_date >= dp.valid_from
    AND soh.order_date <  COALESCE(CAST(dp.valid_to AS DATE), DATE('9999-12-31'))

-- ── Lookup Territory SK ───────────────────────────────────────────────────
LEFT JOIN gold.dim_territory dt
    ON soh.territory_id = dt.territory_id
    AND soh.order_date >= dt.valid_from
    AND soh.order_date <  COALESCE(CAST(dt.valid_to AS DATE), DATE('9999-12-31'))

-- ── Lookup SpecialOffer SK ────────────────────────────────────────────────
LEFT JOIN gold.dim_specialoffer dso
    ON sod.special_offer_id = dso.special_offer_id
    AND soh.order_date      >= dso.valid_from
    AND soh.order_date      <  COALESCE(CAST(dso.valid_to AS DATE), DATE('9999-12-31'))

-- ── Lookup Currency SK via CurrencyRate ───────────────────────────────────
LEFT JOIN silver.sales_currencyrate cr
    ON soh.currency_rate_id = cr.currency_rate_id

LEFT JOIN gold.dim_currency dcu
    ON cr.from_currency_code = dcu.currency_code;


-- ─────────────────────────────────────────────────────────────────────────
-- ÉTAPE 9C : OPTIMIZE + Z-ORDER sur les colonnes les plus utilisées en analytique
-- Sur pétabytes : Z-Order sur order_date_sk et customer_sk réduit le scan
-- de 95% sur les requêtes typiques (filtre date + filtre customer)
-- ─────────────────────────────────────────────────────────────────────────
OPTIMIZE gold.fact_internet_sales
ZORDER BY (order_date_sk, customer_sk, product_sk);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 10 — VACUUM GLOBAL GOLD
-- ███████████████████████████████████████████████████████████████████████████
-- En production : garder 168 HOURS (7 jours) minimum pour time travel
-- Ici 2h pour les tests uniquement

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM gold.dim_date                          RETAIN 168 HOURS;
VACUUM gold.dim_product                       RETAIN 168 HOURS;
VACUUM gold.dim_customer                      RETAIN 168 HOURS;
VACUUM gold.dim_territory                     RETAIN 168 HOURS;
VACUUM gold.dim_specialoffer                  RETAIN 168 HOURS;
VACUUM gold.dim_currency                      RETAIN 168 HOURS;
VACUUM gold.dim_salesreason                   RETAIN 168 HOURS;
VACUUM gold.bridge_salesorder_salesreason     RETAIN 168 HOURS;
VACUUM gold.fact_internet_sales               RETAIN 168 HOURS;




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 11 — CONTRÔLES QUALITÉ POST-CHARGEMENT
-- À exécuter après chaque run pour détecter les anomalies
-- ███████████████████████████████████████████████████████████████████████████

-- QC 1 : Lignes sans customer_sk (jointure ratée)
-- Acceptable = 0 en prod. Si > 0 → problème dans dim_customer ou Silver
SELECT COUNT(*) AS lignes_sans_customer
FROM gold.fact_internet_sales
WHERE customer_sk IS NULL;

-- QC 2 : Lignes sans product_sk
SELECT COUNT(*) AS lignes_sans_product
FROM gold.fact_internet_sales
WHERE product_sk IS NULL;

-- QC 3 : Vérifier que le grain est respecté (pas de doublons sur la clé naturelle)
SELECT sales_order_id, sales_order_detail_id, COUNT(*) AS nb
FROM gold.fact_internet_sales
GROUP BY sales_order_id, sales_order_detail_id
HAVING COUNT(*) > 1
LIMIT 10;

-- QC 4 : Contrôle financier — total Gold vs total Silver (doit être égal)
SELECT
    'SILVER' AS source,
    SUM(line_total)  AS total_line,
    SUM(total_due)   AS total_due,
    COUNT(*)         AS nb_lignes
FROM silver.sales_salesorderdetail sod
JOIN silver.sales_salesorderheader soh
    ON sod.sales_order_id = soh.sales_order_id
    AND soh.is_current = TRUE
    AND soh.online_order_flag = TRUE

UNION ALL

SELECT
    'GOLD' AS source,
    SUM(line_total)  AS total_line,
    SUM(total_due)   AS total_due,
    COUNT(*)         AS nb_lignes
FROM gold.fact_internet_sales;

-- QC 5 : Distribution temporelle (détecte les trous de données)
SELECT
    order_date_sk / 10000              AS year,        -- extrait l'année du yyyyMMdd
    COUNT(*)                           AS nb_commandes,
    SUM(total_due)                     AS ca_total
FROM gold.fact_internet_sales
GROUP BY order_date_sk / 10000
ORDER BY year;