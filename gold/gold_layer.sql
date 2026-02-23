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
-- SECTION 1 — dim_date
-- Générée synthétiquement, jamais depuis Silver
-- Pas de SCD (les dates ne changent pas), Full Refresh annuel
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk         INT          NOT NULL,   -- yyyyMMdd, ex: 20240315
    date            DATE         NOT NULL,
    year            INT,
    quarter         INT,
    quarter_name    STRING,
    month           INT,
    month_name      STRING,
    day             INT,
    day_of_week     INT,
    day_name        STRING,
    week_of_year    INT,
    is_weekday      BOOLEAN,
    is_weekend      BOOLEAN,
    is_last_day_of_month BOOLEAN,
    -- Colonnes fiscales (exercice commençant en juillet - Adventure Works)
    fiscal_year     INT,
    fiscal_quarter  INT,
    fiscal_month    INT
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_date'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
);

-- Full Refresh (table petite, ~7300 lignes pour 20 ans, pas de risque)
CREATE OR REPLACE TABLE gold.dim_date
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_date'
AS
SELECT
    CAST(date_format(d.date, 'yyyyMMdd') AS INT)        AS date_sk,
    d.date,
    YEAR(d.date)                                         AS year,
    QUARTER(d.date)                                      AS quarter,
    CONCAT('Q', QUARTER(d.date))                         AS quarter_name,
    MONTH(d.date)                                        AS month,
    date_format(d.date, 'MMMM')                         AS month_name,
    DAY(d.date)                                          AS day,
    DAYOFWEEK(d.date)                                    AS day_of_week,
    date_format(d.date, 'EEEE')                         AS day_name,
    WEEKOFYEAR(d.date)                                   AS week_of_year,
    CASE WHEN DAYOFWEEK(d.date) BETWEEN 2 AND 6
         THEN TRUE ELSE FALSE END                        AS is_weekday,
    CASE WHEN DAYOFWEEK(d.date) IN (1, 7)
         THEN TRUE ELSE FALSE END                        AS is_weekend,
    CASE WHEN d.date = LAST_DAY(d.date)
         THEN TRUE ELSE FALSE END                        AS is_last_day_of_month,
    -- Fiscal year : juillet = début (Adventure Works convention)
    CASE WHEN MONTH(d.date) >= 7
         THEN YEAR(d.date) + 1
         ELSE YEAR(d.date) END                           AS fiscal_year,
    CASE WHEN MONTH(d.date) IN (7,8,9)   THEN 1
         WHEN MONTH(d.date) IN (10,11,12) THEN 2
         WHEN MONTH(d.date) IN (1,2,3)   THEN 3
         ELSE 4 END                                      AS fiscal_quarter,
    CASE WHEN MONTH(d.date) >= 7
         THEN MONTH(d.date) - 6
         ELSE MONTH(d.date) + 6 END                     AS fiscal_month
FROM (
    SELECT EXPLODE(
        SEQUENCE(TO_DATE('2010-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)
    ) AS date
) d;




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 2 — dim_product
-- SCD Type 2 conservé depuis Silver
-- Dénormalisé : subcategory + category intégrés
-- Stratégie    : Full Refresh (volume dim produit << volume fact)
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_product
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_product'
TBLPROPERTIES (
    'delta.minReaderVersion'           = '1',
    'delta.minWriterVersion'           = '2',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.columnMapping.mode'         = 'name'
)
AS
SELECT
    -- Surrogate key : ROW_NUMBER garantit unicité même avec l'historique SCD
    ROW_NUMBER() OVER (
        ORDER BY p.product_id, p.valid_from
    )                                   AS product_sk,

    -- Clé naturelle (gardée pour debugging et jointures ad hoc)
    p.product_id,

    -- Attributs produit
    p.name,
    p.product_number,
    p.color,
    p.size,
    p.weight,
    p.size_unit_measure_code,
    p.weight_unit_measure_code,
    p.standard_cost,
    p.list_price,
    p.product_line,
    p.class,
    p.style,
    p.days_to_manufacture,
    p.make_flag,
    p.finished_goods_flag,
    p.sell_start_date,
    p.sell_end_date,
    p.discontinued_date,

    -- Dénormalisation subcategory (Kimball : pas de snowflake en Gold)
    psc.name                            AS subcategory_name,

    -- Dénormalisation category
    pc.name                             AS category_name,

    -- SCD Type 2 tracé depuis Silver
    p.modified_date                     AS valid_from,
    p.end_date                          AS valid_to,
    p.is_current

FROM silver.production_product p

-- LEFT JOIN pour ne pas perdre les produits sans sous-catégorie
LEFT JOIN silver.production_productsubcategory psc
    ON p.product_subcategory_id = psc.product_subcategory_id
    -- On joint la version Silver active de la sous-catégorie
    -- (la version historique du produit est liée à la version courante de la hiérarchie)
    AND psc.is_current = TRUE

LEFT JOIN silver.production_productcategory pc
    ON psc.product_category_id = pc.product_category_id
    AND pc.is_current = TRUE;

-- Z-Order sur les colonnes les plus filtrées en analytique
OPTIMIZE gold.dim_product ZORDER BY (product_id, is_current);




-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 3 — dim_customer
-- SCD Type 2 conservé
-- Dénormalisé : person + address + stateprovince + countryregion
-- Stratégie    : Full Refresh (dim, pas la fact)
-- ███████████████████████████████████████████████████████████████████████████

CREATE OR REPLACE TABLE gold.dim_customer
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/dim_customer'
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
        ORDER BY c.customer_id, c.valid_from
    )                                   AS customer_sk,

    c.customer_id,
    c.account_number,
    c.store_id,

    -- Identité (person_person)
    p.first_name,
    p.middle_name,
    p.last_name,
    CONCAT_WS(' ', p.first_name, p.middle_name, p.last_name)
                                        AS full_name,
    p.person_type,
    p.email_promotion,

    -- Adresse (person_address)
    a.address_line1,
    a.address_line2,
    a.city,
    a.postal_code,

    -- Géographie dénormalisée (stateprovince + countryregion)
    sp.state_province_code,
    sp.name                             AS state_province_name,
    cr.country_region_code,
    cr.name                             AS country_name,

    -- SCD Type 2
    c.modified_date                     AS valid_from,
    c.end_date                          AS valid_to,
    c.is_current

FROM silver.sales_customer c

-- person : le PersonID du customer = BusinessEntityID dans person_person
LEFT JOIN silver.person_person p
    ON c.person_id = p.business_entity_id
    AND p.is_current = TRUE

-- address : jointure via ship_to_address ou bill_to dans le header
-- En Gold on rattache l'adresse principale du customer
-- (en pratique on utilise bill_to_address_id du header, mais ici on enrichit la dim)
LEFT JOIN silver.person_address a
    ON p.business_entity_id = a.address_id   -- simplification : 1 adresse par person
    AND a.is_current = TRUE

LEFT JOIN silver.person_stateprovince sp
    ON a.state_province_id = sp.state_province_id
    AND sp.is_current = TRUE

LEFT JOIN silver.person_countryregion cr
    ON sp.country_region_code = cr.country_region_code
    AND cr.is_current = TRUE;

OPTIMIZE gold.dim_customer ZORDER BY (customer_id, is_current);




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