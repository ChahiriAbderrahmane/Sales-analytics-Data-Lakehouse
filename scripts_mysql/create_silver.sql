CREATE DATABASE IF NOT EXISTS silver;

USE silver;

-- Création de la table avec toutes les colonnes + colonnes SCD2



-- sales_order_header
CREATE TABLE IF NOT EXISTS sales_salesorderheader (
    sales_order_id INT,
    revision_number TINYINT,
    order_date DATE,
    due_date DATE,
    ship_date DATE,
    status_code TINYINT,
    online_order_flag BOOLEAN,
    sales_order_number STRING,
    purchase_order_number STRING,
    account_number STRING,
    customer_id INT,
    sales_person_id INT,
    territory_id INT,
    bill_to_address_id INT,
    ship_to_address_id INT,
    ship_method_id INT,
    credit_card_id INT,
    credit_card_approval_code STRING,
    currency_rate_id INT,
    sub_total DECIMAL(19,4),
    tax_amt DECIMAL(19,4),
    freight DECIMAL(19,4),
    total_due DECIMAL(19,4),
    comment STRING,
    rowguid STRING,
    modified_date DATE,
    _ingestion_timestamp TIMESTAMP,
    is_current BOOLEAN,        -- TRUE pour la version active
    end_date TIMESTAMP         -- '9999-12-31' pour la version active
)
USING DELTA
LOCATION '/user/hadoop/sales_data_mart/silver/sales_salesorderheader'
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2',
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableDeletionVectors' = 'true', -- Accélère les UPDATE en évitant le Copy-on-Write immédiat
  'delta.columnMapping.mode' = 'name'     -- Permet de renommer des colonnes sans réécrire les parquets
);


MERGE INTO silver.sales_salesorderheader AS target
USING (
    SELECT 
        SalesOrderID AS sales_order_id,
        RevisionNumber AS revision_number,
        OrderDate AS order_date,
        DueDate AS due_date,
        ShipDate AS ship_date,
        Status AS status_code,
        OnlineOrderFlag AS online_order_flag,
        SalesOrderNumber AS sales_order_number,
        PurchaseOrderNumber AS purchase_order_number,
        AccountNumber AS account_number,
        CustomerID AS customer_id,
        SalesPersonID AS sales_person_id,
        TerritoryID AS territory_id,
        BillToAddressID AS bill_to_address_id,
        ShipToAddressID AS ship_to_address_id,
        ShipMethodID AS ship_method_id,
        CreditCardID AS credit_card_id,
        CreditCardApprovalCode AS credit_card_approval_code,
        CurrencyRateID AS currency_rate_id,
        ABS(CAST(SubTotal AS DECIMAL(19,4))) AS sub_total, 
        ABS(CAST(TaxAmt AS DECIMAL(19,4))) AS tax_amt,    
        ABS(CAST(Freight AS DECIMAL(19,4))) AS freight,
        ABS(CAST(TotalDue AS DECIMAL(19,4))) AS total_due,  
        Comment AS comment,
        rowguid AS rowguid,
        ModifiedDate AS modified_date,
        current_timestamp() AS _ingestion_timestamp,
        TRUE AS is_current,
        CAST('9999-12-31' AS TIMESTAMP) AS end_date
    FROM bronze.sales_salesorderheader
    WHERE SalesOrderID IS NOT NULL
) AS source
ON target.sales_order_id = source.sales_order_id AND target.is_current = TRUE

-- CAS 1 : Mise à jour (On ferme l'ancienne ligne)
WHEN MATCHED AND (
    target.sub_total <> source.sub_total OR 
    target.status_code <> source.status_code OR
    target.total_due <> source.total_due
) THEN
    UPDATE SET 
        target.is_current = FALSE,
        target.end_date = source._ingestion_timestamp

-- CAS 2 : Insertion (Nouvelle ligne ou première version)
WHEN NOT MATCHED THEN
    INSERT *;


-- Compaction des fichiers pour le Data Skipping (Z-Ordering)
OPTIMIZE silver.sales_salesorderheader 
ZORDER BY (customer_id, order_date);

-- Nettoyage agressif des fichiers orphelins (2 heures)
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.sales_salesorderheader RETAIN 2 HOURS;



-- sales_specialoffer
CREATE TABLE IF NOT EXISTS sales_specialoffer (
    special_offer_id INT,
    description STRING,
    discount_pct DECIMAL(5,2),
    type STRING,
    category STRING,
    start_date DATE,
    end_date DATE,
    min_qty INT,
    max_qty INT,
    rowguid STRING,
    modified_date DATE
)