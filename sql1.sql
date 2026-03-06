CREATE OR REPLACE TEMP VIEW silver_product_enriched AS
SELECT
    p.product_id                    AS ProductKey,
    p.product_number                AS ProductNumber,
    p.product_subcategory_id        AS ProductSubcategoryKey,
    p.size_unit_measure_code        AS SizeUnitMeasureCode,
    p.weight_unit_measure_code      AS WeightUnitMeasureCode,
    p.name                          AS ProductName,
    p.standard_cost                 AS StandardCost,
    p.finished_goods_flag           AS FinishedGoodsFlag,
    p.color                         AS Color,
    p.safety_stock_level            AS SafetyStockLevel,
    p.reorder_point                 AS ReorderPoint,
    p.list_price                    AS ListPrice,
    p.size                          AS Size,
    p.weight                        AS Weight,
    p.days_to_manufacture           AS DaysToManufacture,
    p.product_line                  AS ProductLine,
    p.class                         AS Class,
    p.style                         AS Style,
    pm.name                         AS ModelName,
    ph.large_photo_file_name        AS LargePhoto,
    p.sell_start_date               AS StartDate,
    p.sell_end_date                 AS EndDate,
    p.discontinued_date             AS Status,
    p.modified_date                 AS valid_from,
    p.is_current,
    p._ingestion_timestamp
FROM silver.production_product p
LEFT JOIN silver.production_productmodel pm
    ON p.product_model_id = pm.product_model_id     
    AND pm.is_current = TRUE
LEFT JOIN silver.production_productproductphoto ppp
    ON p.product_id = ppp.product_id                 
    AND ppp.is_current = TRUE
LEFT JOIN silver.production_productphoto ph
    ON ppp.product_photo_id = ph.product_photo_id;    







    CREATE OR REPLACE TEMP VIEW silver_product_enriched AS
SELECT
    p.product_id                AS ProductKey,
    p.ProductNumber             AS ProductNumber, 
    p.ProductSubcategoryID      AS ProductSubcategoryKey,
    p.SizeUnitMeasureCode,
    p.WeightUnitMeasureCode,
    p.name                      AS ProductName,
    p.StandardCost,
    p.FinishedGoodsFlag,
    p.Color,
    p.SafetyStockLevel,
    p.ReorderPoint,
    p.ListPrice,
    p.Size,
    p.Weight,
    p.DaysToManufacture,
    p.ProductLine,
    p.Class,
    p.Style,
    pm.Name                     AS ModelName,
    ph.LargePhoto,
    p.SellStartDate             AS StartDate,
    p.SellEndDate               AS EndDate,
    p.DiscontinuedDate          AS Status,
    p.modified_date             AS valid_from,
    p.is_current,
    p._ingestion_timestamp
FROM silver.production_product p
LEFT JOIN silver.production_productmodel pm          
    ON p.ProductModelID = pm.ProductModelID
    AND pm.is_current = TRUE
LEFT JOIN silver.production_productproductphoto ppp
    ON p.ProductID = ppp.ProductID
    AND ppp.is_current = TRUE
LEFT JOIN silver.production_productphoto ph
    ON ppp.ProductPhotoID = ph.ProductPhotoID;