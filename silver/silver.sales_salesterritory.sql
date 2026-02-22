-- ███████████████████████████████████████████████████████████████████████████
-- 3. sales_salesterritory — SCD TYPE 2
--    Clé       : TerritoryID
--    Surveillé : Name, CountryRegionCode, Group
--    Note      : SalesYTD/CostYTD sont des agrégats recalculés automatiquement
--                → non surveillés car ils changent à chaque transaction
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.sales_salesterritory (
    territory_id            INT,
    name                    STRING,
    country_region_code     STRING,
    territory_group         STRING,
    sales_ytd               DECIMAL(19,4),
    sales_last_year         DECIMAL(19,4),
    cost_ytd                DECIMAL(19,4),
    cost_last_year          DECIMAL(19,4),
    rowguid                 STRING,
    modified_date           TIMESTAMP,
    _ingestion_timestamp    TIMESTAMP,
    is_current              BOOLEAN,
    end_date                TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesterritory'
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
-- Note : `Group` est un mot réservé Spark SQL → on utilise des backticks
CREATE OR REPLACE TEMP VIEW sales_salesterritory_ranked AS
SELECT
    CAST(TerritoryID        AS INT)            AS territory_id,
    CAST(Name               AS STRING)         AS name,
    CAST(CountryRegionCode  AS STRING)         AS country_region_code,
    CAST(`Group`            AS STRING)         AS territory_group,
    ABS(CAST(SalesYTD       AS DECIMAL(19,4))) AS sales_ytd,
    ABS(CAST(SalesLastYear  AS DECIMAL(19,4))) AS sales_last_year,
    ABS(CAST(CostYTD        AS DECIMAL(19,4))) AS cost_ytd,
    ABS(CAST(CostLastYear   AS DECIMAL(19,4))) AS cost_last_year,
    CAST(rowguid            AS STRING)         AS rowguid,
    CAST(ModifiedDate       AS TIMESTAMP)      AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY TerritoryID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY TerritoryID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.sales_salesterritory
WHERE TerritoryID IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.sales_salesterritory AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM sales_salesterritory_ranked b
        WHERE b.territory_id = t.territory_id
    )
WHERE t.is_current = TRUE
  AND t.territory_id IN (SELECT DISTINCT territory_id FROM sales_salesterritory_ranked)
  AND EXISTS (
      SELECT 1 FROM sales_salesterritory_ranked b
      WHERE b.territory_id = t.territory_id AND b.rn = 1
        AND (
            b.name                <> t.name                OR
            b.country_region_code <> t.country_region_code OR
            b.territory_group     <> t.territory_group
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.sales_salesterritory
SELECT
    b.territory_id, b.name, b.country_region_code, b.territory_group,
    b.sales_ytd, b.sales_last_year, b.cost_ytd, b.cost_last_year,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_salesterritory_ranked b
WHERE b.territory_id IN (
    SELECT DISTINCT b2.territory_id
    FROM sales_salesterritory_ranked b2
    JOIN silver.sales_salesterritory t
        ON b2.territory_id = t.territory_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.name                <> t.name                OR
          b2.country_region_code <> t.country_region_code OR
          b2.territory_group     <> t.territory_group
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.sales_salesterritory
SELECT
    b.territory_id, b.name, b.country_region_code, b.territory_group,
    b.sales_ytd, b.sales_last_year, b.cost_ytd, b.cost_last_year,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM sales_salesterritory_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.sales_salesterritory t
    WHERE t.territory_id = b.territory_id
);

OPTIMIZE silver.sales_salesterritory ZORDER BY (country_region_code, territory_id);



SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesterritory  RETAIN 2 HOURS;