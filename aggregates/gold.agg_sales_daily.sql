-- Table d'agrégation pour dashboarding
-- GRANULARITÉ : Mois / Pays / Groupe de Territoire / Ligne de Produit

CREATE OR REPLACE TABLE gold.agg_monthly_sales
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/gold/agg_monthly_sales'
AS
SELECT 
    -- 1. Dimensions Temporelles
    YEAR(f.OrderDate)                           AS OrderYear,
    MONTH(f.OrderDate)                          AS OrderMonth,
    
    -- 2. Dimensions Géographiques
    t.sales_country_region_code                 AS CountryRegionCode,
    t.sales_group_name                          AS TerritoryGroup,
    
    -- 3. Dimensions Produit
    COALESCE(p.ProductLine, 'Unknown')          AS ProductLine,
    
    -- 4. Métriques / KPIs 
    COUNT(DISTINCT f.SalesOrderNumber)          AS TotalOrders,
    SUM(f.OrderQuantity)                        AS TotalQuantitySold,
    
    ROUND(SUM(f.SalesAmount), 2)                AS TotalSalesAmount,
    ROUND(SUM(f.TotalProductCost), 2)           AS TotalProductCost,
    
    -- Marge Brute (Gross Profit) = Ventes - Coûts
    ROUND(SUM(f.SalesAmount) - SUM(f.TotalProductCost), 2) AS GrossProfit,
    
    -- % de Marge
    ROUND(
        (SUM(f.SalesAmount) - SUM(f.TotalProductCost)) / NULLIF(SUM(f.SalesAmount), 0) * 100, 
    2)                                          AS GrossMarginPct,

    ROUND(SUM(f.TaxAmt), 2)                     AS TotalTax,
    ROUND(SUM(f.Freight), 2)                    AS TotalFreight,
    ROUND(SUM(f.DiscountAmount), 2)             AS TotalDiscountGiven,
    
    -- 5. Métadonnées de rafraîchissement
    current_timestamp()                         AS last_refresh_timestamp

FROM gold.fact_internet_sales f
LEFT JOIN gold.dim_sales_territory t
    ON f.SalesTerritoryKey = t.sales_territory_id
    
LEFT JOIN gold.dim_product p
    ON f.ProductKey = p.ProductKey
GROUP BY 
    YEAR(f.OrderDate),
    MONTH(f.OrderDate),
    t.sales_country_region_code,
    t.sales_group_name,
    p.ProductLine;

-- OPTIMISATION 
OPTIMIZE gold.agg_monthly_sales ZORDER BY (OrderYear, OrderMonth, CountryRegionCode);