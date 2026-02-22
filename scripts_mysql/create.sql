CREATE DATABASE sales_data_mart; ;
USE sales_data_mart ;

CREATE TABLE SalesTerritory (
    TerritoryID INT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    CountryRegionCode VARCHAR(3),
    `Group` VARCHAR(50),
    SalesYTD DECIMAL(15,2),
    SalesLastYear DECIMAL(15,2),
    CostYTD DECIMAL(15,2),
    CostLastYear DECIMAL(15,2),
    rowguid VARCHAR(64),
    ModifiedDate DATE
);

CREATE TABLE SalesPerson (
    BusinessEntityID INT PRIMARY KEY,
    TerritoryID INT,
    SalesQuota DECIMAL(15,2),
    Bonus DECIMAL(15,2),
    CommissionPct DECIMAL(5,4),
    SalesYTD DECIMAL(15,2),
    SalesLastYear DECIMAL(15,2),
    rowguid VARCHAR(64),
    ModifiedDate DATE,
    FOREIGN KEY (TerritoryID) REFERENCES SalesTerritory(TerritoryID)
);

CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    PersonID INT,
    StoreID INT,
    TerritoryID INT,
    AccountNumber VARCHAR(20),
    rowguid VARCHAR(64),
    ModifiedDate DATE,
    FOREIGN KEY (TerritoryID) REFERENCES SalesTerritory(TerritoryID)
);

CREATE TABLE SpecialOffer (
    SpecialOfferID INT PRIMARY KEY,
    Description VARCHAR(255),
    DiscountPct DECIMAL(5,4),
    Type VARCHAR(50),
    Category VARCHAR(50),
    StartDate DATE,
    EndDate DATE,
    MinQty INT,
    MaxQty INT,
    rowguid VARCHAR(64),
    ModifiedDate DATE
);

CREATE TABLE SalesOrderHeader (
    SalesOrderID INT PRIMARY KEY,
    RevisionNumber TINYINT,
    OrderDate DATE,
    DueDate DATE,
    ShipDate DATE,
    Status TINYINT,
    OnlineOrderFlag TINYINT(1), -- Boolean
    SalesOrderNumber VARCHAR(25),
    PurchaseOrderNumber VARCHAR(25),
    AccountNumber VARCHAR(20),
    CustomerID INT,
    SalesPersonID INT,
    TerritoryID INT,
    BillToAddressID INT,
    ShipToAddressID INT,
    ShipMethodID INT,
    CreditCardID INT,
    CreditCardApprovalCode VARCHAR(15),
    CurrencyRateID INT,
    SubTotal DECIMAL(15,2),
    TaxAmt DECIMAL(15,2),
    Freight DECIMAL(15,2),
    TotalDue DECIMAL(15,2),
    Comment TINYINT(1), -- Boolean dans votre CSV
    rowguid VARCHAR(64),
    ModifiedDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (SalesPersonID) REFERENCES SalesPerson(BusinessEntityID),
    FOREIGN KEY (TerritoryID) REFERENCES SalesTerritory(TerritoryID)
);

CREATE TABLE SalesOrderDetail (
    SalesOrderID INT,
    SalesOrderDetailID INT,
    CarrierTrackingNumber VARCHAR(25),
    OrderQty SMALLINT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice DECIMAL(15,2),
    UnitPriceDiscount DECIMAL(15,2),
    LineTotal DECIMAL(15,2),
    rowguid VARCHAR(64),
    ModifiedDate DATE,
    PRIMARY KEY (SalesOrderID, SalesOrderDetailID), -- Clé composée
    FOREIGN KEY (SalesOrderID) REFERENCES SalesOrderHeader(SalesOrderID),
    FOREIGN KEY (SpecialOfferID) REFERENCES SpecialOffer(SpecialOfferID)
);

LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_SalesTerritory.csv'
INTO TABLE SalesTerritory
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_SalesPerson.csv'
INTO TABLE SalesPerson
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_Customer.csv'
INTO TABLE Customer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_SpecialOffer.csv'
INTO TABLE SpecialOffer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_SalesOrderHeader.csv'
INTO TABLE SalesOrderHeader
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/home/ubuntu/adventureworks_csv_data/Sales_SalesOrderDetail.csv'
INTO TABLE SalesOrderDetail
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;




