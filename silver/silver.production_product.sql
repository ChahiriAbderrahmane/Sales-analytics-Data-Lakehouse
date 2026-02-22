-- ███████████████████████████████████████████████████████████████████████████
-- 1. production_product — SCD TYPE 2
--    Clé       : ProductID
--    Surveillé : Name, ListPrice, StandardCost, ProductLine, Class,
--                ProductSubcategoryID
-- ███████████████████████████████████████████████████████████████████████████

CREATE TABLE IF NOT EXISTS silver.production_product (
    product_id                  INT,
    name                        STRING,
    product_number              STRING,
    make_flag                   BOOLEAN,
    finished_goods_flag         BOOLEAN,
    color                       STRING,
    safety_stock_level          SMALLINT,
    reorder_point               SMALLINT,
    standard_cost               DECIMAL(19,4),
    list_price                  DECIMAL(19,4),
    size                        STRING,
    size_unit_measure_code      STRING,
    weight_unit_measure_code    STRING,
    weight                      DECIMAL(19,4),
    days_to_manufacture         INT,
    product_line                STRING,
    class                       STRING,
    style                       STRING,
    product_subcategory_id      INT,
    product_model_id            INT,
    sell_start_date             TIMESTAMP,
    sell_end_date               TIMESTAMP,
    discontinued_date           TIMESTAMP,
    rowguid                     STRING,
    modified_date               TIMESTAMP,
    _ingestion_timestamp        TIMESTAMP,
    is_current                  BOOLEAN,
    end_date                    TIMESTAMP
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/production_product'
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
CREATE OR REPLACE TEMP VIEW production_product_ranked AS
SELECT
    CAST(ProductID              AS INT)            AS product_id,
    CAST(Name                   AS STRING)         AS name,
    CAST(ProductNumber          AS STRING)         AS product_number,
    CAST(MakeFlag               AS BOOLEAN)        AS make_flag,
    CAST(FinishedGoodsFlag      AS BOOLEAN)        AS finished_goods_flag,
    CAST(Color                  AS STRING)         AS color,
    CAST(SafetyStockLevel       AS SMALLINT)       AS safety_stock_level,
    CAST(ReorderPoint           AS SMALLINT)       AS reorder_point,
    ABS(CAST(StandardCost       AS DECIMAL(19,4))) AS standard_cost,
    ABS(CAST(ListPrice          AS DECIMAL(19,4))) AS list_price,
    CAST(Size                   AS STRING)         AS size,
    CAST(SizeUnitMeasureCode    AS STRING)         AS size_unit_measure_code,
    CAST(WeightUnitMeasureCode  AS STRING)         AS weight_unit_measure_code,
    CAST(Weight                 AS DECIMAL(19,4))  AS weight,
    CAST(DaysToManufacture      AS INT)            AS days_to_manufacture,
    CAST(ProductLine            AS STRING)         AS product_line,
    CAST(Class                  AS STRING)         AS class,
    CAST(Style                  AS STRING)         AS style,
    CAST(ProductSubcategoryID   AS INT)            AS product_subcategory_id,
    CAST(ProductModelID         AS INT)            AS product_model_id,
    CAST(SellStartDate          AS TIMESTAMP)      AS sell_start_date,
    CAST(SellEndDate            AS TIMESTAMP)      AS sell_end_date,
    CAST(DiscontinuedDate       AS TIMESTAMP)      AS discontinued_date,
    CAST(rowguid                AS STRING)         AS rowguid,
    CAST(ModifiedDate           AS TIMESTAMP)      AS modified_date,
    ROW_NUMBER() OVER (
        PARTITION BY ProductID
        ORDER BY ModifiedDate DESC
    ) AS rn,
    LEAD(CAST(ModifiedDate AS TIMESTAMP)) OVER (
        PARTITION BY ProductID
        ORDER BY ModifiedDate DESC
    ) AS next_version_date
FROM bronze.production_product
WHERE ProductID IS NOT NULL;

-- ÉTAPE 1 : Fermer les lignes actives modifiées
UPDATE silver.production_product AS t
SET
    is_current = FALSE,
    end_date   = (
        SELECT CAST(MIN(b.modified_date) AS TIMESTAMP)
        FROM production_product_ranked b
        WHERE b.product_id = t.product_id
    )
WHERE t.is_current = TRUE
  AND t.product_id IN (SELECT DISTINCT product_id FROM production_product_ranked)
  AND EXISTS (
      SELECT 1 FROM production_product_ranked b
      WHERE b.product_id = t.product_id AND b.rn = 1
        AND (
            b.name                   <> t.name                   OR
            b.list_price             <> t.list_price             OR
            b.standard_cost          <> t.standard_cost          OR
            b.product_line           <> t.product_line           OR
            b.class                  <> t.class                  OR
            b.product_subcategory_id <> t.product_subcategory_id
        )
  );

-- ÉTAPE 2 : Insérer toutes les versions pour les IDs modifiés
INSERT INTO silver.production_product
SELECT
    b.product_id, b.name, b.product_number, b.make_flag, b.finished_goods_flag,
    b.color, b.safety_stock_level, b.reorder_point, b.standard_cost, b.list_price,
    b.size, b.size_unit_measure_code, b.weight_unit_measure_code, b.weight,
    b.days_to_manufacture, b.product_line, b.class, b.style,
    b.product_subcategory_id, b.product_model_id,
    b.sell_start_date, b.sell_end_date, b.discontinued_date,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_product_ranked b
WHERE b.product_id IN (
    SELECT DISTINCT b2.product_id
    FROM production_product_ranked b2
    JOIN silver.production_product t
        ON b2.product_id = t.product_id AND t.is_current = FALSE
    WHERE b2.rn = 1
      AND (
          b2.name                   <> t.name                   OR
          b2.list_price             <> t.list_price             OR
          b2.standard_cost          <> t.standard_cost          OR
          b2.product_line           <> t.product_line           OR
          b2.class                  <> t.class                  OR
          b2.product_subcategory_id <> t.product_subcategory_id
      )
);

-- ÉTAPE 3 : Insérer les nouveaux IDs
INSERT INTO silver.production_product
SELECT
    b.product_id, b.name, b.product_number, b.make_flag, b.finished_goods_flag,
    b.color, b.safety_stock_level, b.reorder_point, b.standard_cost, b.list_price,
    b.size, b.size_unit_measure_code, b.weight_unit_measure_code, b.weight,
    b.days_to_manufacture, b.product_line, b.class, b.style,
    b.product_subcategory_id, b.product_model_id,
    b.sell_start_date, b.sell_end_date, b.discontinued_date,
    b.rowguid, b.modified_date,
    current_timestamp()                              AS _ingestion_timestamp,
    CASE WHEN b.rn = 1 THEN TRUE ELSE FALSE END      AS is_current,
    CASE
        WHEN b.rn = 1 THEN CAST('9999-12-31' AS TIMESTAMP)
        ELSE b.next_version_date
    END                                              AS end_date
FROM production_product_ranked b
WHERE NOT EXISTS (
    SELECT 1 FROM silver.production_product t
    WHERE t.product_id = b.product_id
);

OPTIMIZE silver.production_product ZORDER BY (product_subcategory_id, product_id);

