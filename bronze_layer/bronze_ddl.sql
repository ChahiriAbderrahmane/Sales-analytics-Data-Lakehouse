CREATE DATABASE IF NOT EXISTS bronze
LOCATION '/user/hadoop/sales_data_mart/bronze';

-- sales_salesorderheader
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesorderheader (
    SalesOrderID        INT,
    RevisionNumber      TINYINT,
    OrderDate           TIMESTAMP,
    DueDate             TIMESTAMP,
    ShipDate            TIMESTAMP,
    Status              TINYINT,
    OnlineOrderFlag     BOOLEAN,
    SalesOrderNumber    STRING,
    PurchaseOrderNumber STRING,
    AccountNumber       STRING,
    CustomerID          INT,
    SalesPersonID       INT,
    TerritoryID         INT,
    BillToAddressID     INT,
    ShipToAddressID     INT,
    ShipMethodID        INT,
    CreditCardID        INT,
    CreditCardApprovalCode STRING,
    CurrencyRateID      INT,
    SubTotal            DOUBLE,
    TaxAmt              DOUBLE,
    Freight             DOUBLE,
    TotalDue            DOUBLE,
    Comment             STRING,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Sales_SalesOrderHeader'
TBLPROPERTIES (
    "skip.header.line.count"="0",
    "serialization.null.format"="null"
);


-- sales_salesorderdetail
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesorderdetail (
    SalesOrderID        INT,
    SalesOrderDetailID  INT,
    CarrierTrackingNumber STRING,
    OrderQty            SMALLINT,
    ProductID           INT,
    SpecialOfferID      INT,
    UnitPrice           DOUBLE,
    UnitPriceDiscount   DOUBLE,
    LineTotal           DOUBLE,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Sales_SalesOrderDetail'
TBLPROPERTIES (
    "skip.header.line.count"="0",
    "serialization.null.format"="null"
);



-- Person.Address
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_address (
    AddressID           INT,
    AddressLine1        STRING,
    AddressLine2        STRING,
    City                STRING,
    StateProvinceID     INT,
    PostalCode          STRING,
    SpatialLocation     STRING,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_Address'
TBLPROPERTIES ("serialization.null.format"="null");



-- Person.CountryRegion
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_countryregion (
    CountryRegionCode   STRING,
    Name                STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_CountryRegion'
TBLPROPERTIES ("serialization.null.format"="null");


-- Person.Person
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_person (
    BusinessEntityID        INT,
    PersonType              STRING,
    NameStyle               BOOLEAN,
    Title                   STRING,
    FirstName               STRING,
    MiddleName              STRING,
    LastName                STRING,
    Suffix                  STRING,
    EmailPromotion          INT,
    AdditionalContactInfo   STRING,
    Demographics            STRING,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_Person'
TBLPROPERTIES ("serialization.null.format"="null");


-- Person.StateProvince
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_stateprovince (
    StateProvinceID         INT,
    StateProvinceCode       STRING,
    CountryRegionCode       STRING,
    IsOnlyStateProvinceFlag BOOLEAN,
    Name                    STRING,
    TerritoryID             INT,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_StateProvince'
TBLPROPERTIES ("serialization.null.format"="null");

-- ============================================================


-- Production.Product
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_product (
    ProductID               INT,
    Name                    STRING,
    ProductNumber           STRING,
    MakeFlag                BOOLEAN,
    FinishedGoodsFlag       BOOLEAN,
    Color                   STRING,
    SafetyStockLevel        SMALLINT,
    ReorderPoint            SMALLINT,
    StandardCost            DOUBLE,
    ListPrice               DOUBLE,
    Size                    STRING,
    SizeUnitMeasureCode     STRING,
    WeightUnitMeasureCode   STRING,
    Weight                  DOUBLE,
    DaysToManufacture       INT,
    ProductLine             STRING,
    Class                   STRING,
    Style                   STRING,
    ProductSubcategoryID    INT,
    ProductModelID          INT,
    SellStartDate           TIMESTAMP,
    SellEndDate             TIMESTAMP,
    DiscontinuedDate        TIMESTAMP,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_Product'
TBLPROPERTIES ("serialization.null.format"="null");




-- Production.ProductCategory
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productcategory (
    ProductCategoryID   INT,
    Name                STRING,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductCategory'
TBLPROPERTIES ("serialization.null.format"="null");


-- Production.ProductSubcategory
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productsubcategory (
    ProductSubcategoryID    INT,
    ProductCategoryID       INT,
    Name                    STRING,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductSubcategory'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.Currency
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_currency (
    CurrencyCode    STRING,
    Name            STRING,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Currency'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.CurrencyRate
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_currencyrate (
    CurrencyRateID      INT,
    CurrencyRateDate    TIMESTAMP,
    FromCurrencyCode    STRING,
    ToCurrencyCode      STRING,
    AverageRate         DOUBLE,
    EndOfDayRate        DOUBLE,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/CurrencyRate'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.Customer
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_customer (
    CustomerID      INT,
    PersonID        INT,
    StoreID         INT,
    TerritoryID     INT,
    AccountNumber   STRING,
    rowguid         STRING,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Customer'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.SalesOrderDetail
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesorderdetail (
    SalesOrderID            INT,
    SalesOrderDetailID      INT,
    CarrierTrackingNumber   STRING,
    OrderQty                SMALLINT,
    ProductID               INT,
    SpecialOfferID          INT,
    UnitPrice               DOUBLE,
    UnitPriceDiscount       DOUBLE,
    LineTotal               DOUBLE,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Sales_SalesOrderDetail'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.SalesOrderHeaderSalesReason
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesorderheadersalesreason (
    SalesOrderID    INT,
    SalesReasonID   INT,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/SalesOrderHeaderSalesReason'
TBLPROPERTIES ("serialization.null.format"="null");


-- Sales.SalesReason
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesreason (
    SalesReasonID   INT,
    Name            STRING,
    ReasonType      STRING,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/SalesReason'
TBLPROPERTIES ("serialization.null.format"="null");



-- Sales.SalesTerritory
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_salesterritory (
    TerritoryID         INT,
    Name                STRING,
    CountryRegionCode   STRING,
    `Group`               STRING,
    SalesYTD            DOUBLE,
    SalesLastYear       DOUBLE,
    CostYTD             DOUBLE,
    CostLastYear        DOUBLE,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/SalesTerritory'
TBLPROPERTIES ("serialization.null.format"="null");




-- Sales.SpecialOffer
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_specialoffer (
    SpecialOfferID  INT,
    Description     STRING,
    DiscountPct     DOUBLE,
    Type            STRING,
    Category        STRING,
    StartDate       TIMESTAMP,
    EndDate         TIMESTAMP,
    MinQty          INT,
    MaxQty          INT,
    rowguid         STRING,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/SpecialOffer'
TBLPROPERTIES ("serialization.null.format"="null");


-- Person.BusinessEntityAddress
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_businessentityaddress (
    business_entity_id      INT,
    address_id              INT,
    address_type_id         INT,
    rowguid                 STRING,
    modified_date           TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_BusinessEntityAddress'
TBLPROPERTIES ("serialization.null.format"="null");


-- Person.EmailAddress
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_emailaddress (
    BusinessEntityID    INT,
    EmailAddressID     INT,
    EmailAddress       STRING,
    rowguid             STRING,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_EmailAddress'
TBLPROPERTIES ("serialization.null.format"="null");

-- Person.PersonPhone
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.person_personphone (
    BusinessEntityID    INT,
    PhoneNumber        STRING,
    PhoneNumberTypeID  INT,
    ModifiedDate        TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_PersonPhone'
TBLPROPERTIES ("serialization.null.format"="null");

-- Production.ProductModel 
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productmodel (
    ProductModelID     INT,
    Name               STRING,
    CatalogDescription STRING,
    Instructions       STRING,
    rowguid            STRING,
    ModifiedDate       TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductModel'
TBLPROPERTIES ("serialization.null.format"="null");

SELECT ProductModelID, Name, LENGTH(Instructions) AS xml_chars
FROM bronze.production_productmodel
WHERE Instructions IS NOT NULL
LIMIT 5;

DROP TABLE IF EXISTS bronze.person_person;
CREATE EXTERNAL TABLE bronze.person_person (
    BusinessEntityID        INT,
    PersonType              STRING,
    NameStyle               BOOLEAN,
    Title                   STRING,
    FirstName               STRING,
    MiddleName              STRING,
    LastName                STRING,
    Suffix                  STRING,
    EmailPromotion          INT,
    AdditionalContactInfo   STRING,
    Demographics            STRING,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Person_Person'
TBLPROPERTIES ("serialization.null.format"="null");

SELECT BusinessEntityID, LENGTH(Demographics) AS xml_chars
FROM bronze.person_person
WHERE Demographics IS NOT NULL
LIMIT 5;

-- ProductDescription
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productdescription (
    ProductDescriptionID   INT,
    Description           STRING,
    rowguid               STRING,
    ModifiedDate          TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductDescription'
TBLPROPERTIES ("serialization.null.format"="null");



-- ProductModelProductDescriptionCulture
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productmodelproductdescriptionculture (
    ProductModelID          INT,
    ProductDescriptionID   INT,
    Culture                 STRING,
    rowguid                 STRING,
    ModifiedDate            TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE  
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductModelProductDescriptionCulture'
TBLPROPERTIES ("serialization.null.format"="null");



-- ProductPhoto
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productphoto (
    ProductPhotoID     INT,
    ThumbNailPhoto    BINARY,
    ThumbnailPhotoFileName STRING,
    LargePhoto        BINARY,
    LargePhotoFileName STRING,
    rowguid           STRING,
    ModifiedDate      TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductPhoto'
TBLPROPERTIES ("serialization.null.format"="null");


-- ProductProductPhoto
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_productproductphoto (
    ProductID       INT,
    ProductPhotoID  INT,
    rowguid         STRING,
    ModifiedDate    TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_ProductProductPhoto'
TBLPROPERTIES ("serialization.null.format"="null");



-- UnitMeasure
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.production_unitmeasure (
    UnitMeasureCode    STRING,
    Name               STRING,
    ModifiedDate       TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/sales_data_mart/bronze/Production_UnitMeasure'
TBLPROPERTIES ("serialization.null.format"="null");








person_businessentityaddress
person_address
person_countryregion
person_person
person_stateprovince
production_product
production_productcategory
production_productsubcategory
sales_currency
sales_currencyrate
sales_customer
sales_salesorderdetail
sales_salesorderheadersalesreason
sales_salesreason
sales_salesterritory
sales_salesorderheader
sales_specialoffer
and many more...


