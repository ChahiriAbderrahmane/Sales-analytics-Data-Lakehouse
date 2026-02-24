-- 4. sales_specialoffer — SCD TYPE 2
--    Clé       : SpecialOfferID
--    Surveillé : Description, DiscountPct, Type, Category,
--                StartDate, EndDate, MinQty, MaxQty

CREATE TABLE IF NOT EXISTS silver.sales_specialoffer (
    special_offer_id        INT,
    description             STRING,
    discount_pct            DECIMAL(19,4),
    type                    STRING,
    category                STRING,
    start_date              TIMESTAMP,
    end_date_offer          TIMESTAMP,
    min_qty                 INT,
    max_qty                 INT,
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_specialoffer'
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
-- Note : EndDate de l'offre → renommé end_date_offer pour éviter la collision
--        avec la colonne SCD end_date
CREATE OR REPLACE TEMP VIEW sales_specialoffer_ranked AS
SELECT
    CAST(SpecialOfferID AS INT)            AS special_offer_id,
    CAST(Description    AS STRING)         AS description,
    ABS(CAST(DiscountPct AS DECIMAL(19,4))) AS discount_pct,
    CAST(Type           AS STRING)         AS type,
    CAST(Category       AS STRING)         AS category,
    CAST(StartDate      AS TIMESTAMP)      AS start_date,
    CAST(EndDate        AS TIMESTAMP)      AS end_date_offer, 
    CAST(MinQty         AS INT)            AS min_qty,
    CAST(MaxQty         AS INT)            AS max_qty,
    CAST(rowguid        AS STRING)         AS rowguid,
    CAST(ModifiedDate   AS TIMESTAMP)      AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY SpecialOfferID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY SpecialOfferID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.sales_specialoffer
WHERE SpecialOfferID IS NOT NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 1 : Identifier les IDs à fermer
-- IS DISTINCT FROM remplace <> pour gérer correctement les valeurs NULL
-- (max_qty notamment est nullable en source)
-- ───────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TEMP VIEW ids_to_close AS
SELECT
    b.special_offer_id,
    CAST(MIN(b.modified_date) AS TIMESTAMP) AS new_end_date
FROM sales_specialoffer_ranked b
JOIN silver.sales_specialoffer t
    ON  b.special_offer_id = t.special_offer_id
    AND t.is_current = TRUE
WHERE b.description    IS DISTINCT FROM t.description
   OR b.discount_pct   IS DISTINCT FROM t.discount_pct
   OR b.type           IS DISTINCT FROM t.type
   OR b.category       IS DISTINCT FROM t.category
   OR b.start_date     IS DISTINCT FROM t.start_date
   OR b.end_date_offer IS DISTINCT FROM t.end_date_offer
   OR b.min_qty        IS DISTINCT FROM t.min_qty
   OR b.max_qty        IS DISTINCT FROM t.max_qty
GROUP BY b.special_offer_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 2 : Fermer les lignes actives via MERGE
-- MERGE est la seule façon de faire un UPDATE conditionnel
-- sans sous-requêtes sur Delta open source
-- ───────────────────────────────────────────────────────────────────────────
MERGE INTO silver.sales_specialoffer AS t
USING ids_to_close AS s
ON  t.special_offer_id = s.special_offer_id
AND t.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date   = s.new_end_date;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 3 : Insérer toutes les versions pour les IDs modifiés
-- JOIN sur ids_to_close remplace la sous-requête IN (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_specialoffer
SELECT
    b.special_offer_id,
    b.description,
    b.discount_pct,
    b.type,
    b.category,
    b.start_date,
    b.end_date_offer,
    b.min_qty,
    b.max_qty,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_specialoffer_ranked b
JOIN ids_to_close s
    ON b.special_offer_id = s.special_offer_id;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 4 : Insérer les nouveaux IDs (jamais vus dans silver)
-- LEFT JOIN + IS NULL remplace NOT EXISTS (...)
-- ───────────────────────────────────────────────────────────────────────────
INSERT INTO silver.sales_specialoffer
SELECT
    b.special_offer_id,
    b.description,
    b.discount_pct,
    b.type,
    b.category,
    b.start_date,
    b.end_date_offer,
    b.min_qty,
    b.max_qty,
    b.rowguid,
    b.modified_date,
    current_timestamp()                             AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END     AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                             AS end_date
FROM sales_specialoffer_ranked b
LEFT JOIN silver.sales_specialoffer t
    ON b.special_offer_id = t.special_offer_id
WHERE t.special_offer_id IS NULL;

-- ───────────────────────────────────────────────────────────────────────────
-- ÉTAPE 5 : Optimisation
-- ───────────────────────────────────────────────────────────────────────────
OPTIMIZE silver.sales_specialoffer
ZORDER BY (special_offer_id, start_date);

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_specialoffer RETAIN 168 HOURS;