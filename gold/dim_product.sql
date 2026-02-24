-- ███████████████████████████████████████████████████████████████████████████
-- SECTION 2 — dim_product
-- SCD Type 2 conservé depuis Silver
-- Dénormalisé : subcategory + category intégrés
-- Stratégie    : Full Refresh (volume dim produit << volume fact)
-- ███████████████████████████████████████████████████████████████████████████


--- cette table ne gorssira jamais très vites (maintenant, elle mesure des centaines de lignes), c'est pour ça que y'a pas de partition by
    --- et donc toutes les données seront traitées dans le même worker, donc ça marche
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
        ORDER BY p.product_id, p.modified_date 
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