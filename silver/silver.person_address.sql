-- ███████████████████████████████████████████████████████████████████████████
-- 2. person_address — SCD TYPE 2
--    Clé       : AddressID
--    Surveillé : AddressLine1, AddressLine2, City, PostalCode, StateProvinceID
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.person_address (
    address_id          INT,
    address_line1       STRING,
    address_line2       STRING,
    city                STRING,
    state_province_id   INT,
    postal_code         STRING,
    spatial_location    STRING,
    rowguid             STRING,
    modified_date       TIMESTAMP,
    _ingestion_timestamp TIMESTAMP,
    is_current          BOOLEAN,
    end_date            TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/person_address'
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
CREATE OR REPLACE TEMP VIEW person_address_ranked AS
SELECT
    CAST(AddressID          AS INT)       AS address_id,
    CAST(AddressLine1       AS STRING)    AS address_line1,
    CAST(AddressLine2       AS STRING)    AS address_line2,
    CAST(City               AS STRING)    AS city,
    CAST(StateProvinceID    AS INT)       AS state_province_id,
    CAST(PostalCode         AS STRING)    AS postal_code,
    CAST(SpatialLocation    AS STRING)    AS spatial_location,
    CAST(rowguid            AS STRING)    AS rowguid,
    CAST(ModifiedDate       AS TIMESTAMP) AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY AddressID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY AddressID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.person_address
WHERE AddressID IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.person_address AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM person_address_ranked b
        WHERE b.address_id = t.address_id
    )
WHERE t.is_current = TRUE
  AND t.address_id IN (SELECT DISTINCT address_id FROM person_address_ranked)
  AND EXISTS (
      SELECT 1 FROM person_address_ranked b
      WHERE b.address_id = t.address_id AND b.rn = 1
        AND (
            b.address_line1     <> t.address_line1     OR
            b.address_line2     <> t.address_line2     OR
            b.city              <> t.city              OR
            b.postal_code       <> t.postal_code       OR
            b.state_province_id <> t.state_province_id
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.person_address
SELECT
    b.address_id, b.address_line1, b.address_line2, b.city,
    b.state_province_id, b.postal_code, b.spatial_location, b.rowguid,
    b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_address_ranked b
WHERE b.address_id IN (
    SELECT DISTINCT b2.address_id
    FROM person_address_ranked b2
    JOIN silver.person_address t ON b2.address_id = t.address_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.address_line1     <> t.address_line1     OR
          b2.address_line2     <> t.address_line2     OR
          b2.city              <> t.city              OR
          b2.postal_code       <> t.postal_code       OR
          b2.state_province_id <> t.state_province_id
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.person_address
SELECT
    b.address_id, b.address_line1, b.address_line2, b.city,
    b.state_province_id, b.postal_code, b.spatial_location, b.rowguid,
    b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM person_address_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.person_address t
    WHERE t.address_id = b.address_id
);

OPTIMIZE silver.person_address ZORDER BY (city, state_province_id);


SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.person_address          RETAIN 2 HOURS;