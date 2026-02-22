-- ███████████████████████████████████████████████████████████████████████████
-- 4. sales_specialoffer — SCD TYPE 2
--    Clé       : SpecialOfferID
--    Surveillé : Description, DiscountPct, Type, Category,
--                StartDate, EndDate, MinQty, MaxQty
-- ███████████████████████████████████████████████████████████████████████████

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
    CAST(EndDate        AS TIMESTAMP)      AS end_date_offer,  -- ⚠️ renommé
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

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.sales_specialoffer AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM sales_specialoffer_ranked b
        WHERE b.special_offer_id = t.special_offer_id
    )
WHERE t.is_current = TRUE
  AND t.special_offer_id IN (SELECT DISTINCT special_offer_id FROM sales_specialoffer_ranked)
  AND EXISTS (
      SELECT 1 FROM sales_specialoffer_ranked b
      WHERE b.special_offer_id = t.special_offer_id AND b.rn = 1
        AND (
            b.description    <> t.description    OR
            b.discount_pct   <> t.discount_pct   OR
            b.type           <> t.type           OR
            b.category       <> t.category       OR
            b.start_date     <> t.start_date     OR
            b.end_date_offer <> t.end_date_offer OR
            b.min_qty        <> t.min_qty        OR
            b.max_qty        <> t.max_qty
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.sales_specialoffer
SELECT
    b.special_offer_id, b.description, b.discount_pct, b.type, b.category,
    b.start_date, b.end_date_offer, b.min_qty, b.max_qty,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_specialoffer_ranked b
WHERE b.special_offer_id IN (
    SELECT DISTINCT b2.special_offer_id
    FROM sales_specialoffer_ranked b2
    JOIN silver.sales_specialoffer t
        ON b2.special_offer_id = t.special_offer_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.description    <> t.description    OR
          b2.discount_pct   <> t.discount_pct   OR
          b2.type           <> t.type           OR
          b2.category       <> t.category       OR
          b2.start_date     <> t.start_date     OR
          b2.end_date_offer <> t.end_date_offer OR
          b2.min_qty        <> t.min_qty        OR
          b2.max_qty        <> t.max_qty
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.sales_specialoffer
SELECT
    b.special_offer_id, b.description, b.discount_pct, b.type, b.category,
    b.start_date, b.end_date_offer, b.min_qty, b.max_qty,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_specialoffer_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.sales_specialoffer t
    WHERE t.special_offer_id = b.special_offer_id
);

OPTIMIZE silver.sales_specialoffer ZORDER BY (special_offer_id, start_date);


SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_specialoffer  RETAIN 2 HOURS;