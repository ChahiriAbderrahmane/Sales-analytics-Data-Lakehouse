<div align="center">
  <h1>🚵‍♂️ AdventureWorks Hybrid Data Lakehouse</h1>
  <h3>On-Premise Cluster to Cloud-Native BI | FinOps & Medallion Architecture</h3>

  <p>
    An enterprise-grade Big Data engineering project transforming raw transaction data into serverless analytics 
    via a modern stack: <strong>Hadoop, Spark, Delta Lake, AWS S3, Athena, and QuickSight</strong>.
  </p>

  <img src="https://img.shields.io/badge/Infrastructure-AWS%20EC2-FF9900?style=for-the-badge&logo=amazon-aws" />
  <img src="https://img.shields.io/badge/Storage-HDFS%20%7C%20S3-66CC00?style=for-the-badge&logo=apache-hadoop" />
  <img src="https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark" />
  <img src="https://img.shields.io/badge/Format-Delta%20Lake-43CBFF?style=for-the-badge&logo=databricks" />
  <img src="https://img.shields.io/badge/Serverless-AWS%20Athena-FF4F8B?style=for-the-badge&logo=amazon-aws" />
  <img src="https://img.shields.io/badge/BI-Amazon%20QuickSight-005B9F?style=for-the-badge&logo=amazon-aws" />
</div>

<br>

# **Project Architecture**
![Project Architecture](Hybrid_Data_Lakehouse_Architecture_Diagram.png)

<br>

## 📝 Table of Contents
1. [Project Overview](#overview)
2. [Infrastructure & Cluster Setup](#infrastructure)
3. [Data Architecture & Modeling](#architecture)
4. [ETL Pipeline (Medallion)](#etl)
5. [Cloud Native & FinOps Optimization](#finops)
6. [Business Intelligence (QuickSight)](#bi)
7. [Installation & Deployment](#install)
8. [Contact](#contact)

<hr>

<a name="overview"></a>
## 🔭 Project Overview

This project simulates a real-world enterprise data migration and modernization strategy. It extracts transactional data from a simulated "On-Premise" environment (hosted on AWS EC2), performs heavy distributed processing using a Hadoop/Spark cluster, and ultimately serves the data via a Cloud-Native, serverless architecture to optimize costs (FinOps).

**Key Features:**
* **Hybrid Architecture:** Bridging IaaS (EC2 Cluster) with PaaS/SaaS (S3, Athena, QuickSight).
* **Medallion Pipeline:** Ingestion of raw data into Bronze (HiveQL), Silver, and Gold layers (SparkSQL).
* **Advanced Modeling:** Snowflake schema design with **Slowly Changing Dimensions (SCD Type 2)** in the Silver layer.
* **FinOps Strategy:** Decoupling storage and compute by shutting down the heavy processing cluster and querying data serverlessly via Athena.

<a name="infrastructure"></a>
## 🖥️ Infrastructure & Cluster Setup (IaaS)

I provisioned and configured a distributed Big Data cluster from scratch using 4 **AWS EC2 instances (`t3.medium`)**, demonstrating strong Linux administration and networking capabilities.

* **Machine 1 (Source & Metastore):** Hosts the source SQL Server database (AdventureWorks2022) and a PostgreSQL database acting as the Hive Metastore.
* **Machine 2 (Master Node):** The brain of the cluster (Hadoop NameNode, Spark Master, Hive, Sqoop).
* **Machines 3 & 4 (Worker Nodes):** The processing muscle (Hadoop DataNodes, Spark Workers).

<a name="architecture"></a>
## 🏗️ Data Architecture & Modeling

The project focuses on the **InternetSales** business process. The data undergoes rigorous transformation to ensure analytical performance and historical tracking.

### The Snowflake Schema & SCD Type 2
I designed a highly normalized Snowflake schema for the Data Warehouse layer. To maintain historical accuracy of business entities (like product price changes or customer addresses), I implemented **SCD Type 2** in the Silver layer.

<div align="center">
  <img src="assets/snowflake_schema_model.png" alt="Snowflake Schema" width="800">
  <p><em>Snowflake Schema illustrating the InternetSales dimensions and facts.</em></p>
</div>

<a name="etl"></a>
## 🌪️ ETL Pipeline (Medallion Architecture)

The data flows through a strict Medallion architecture, ensuring data quality and ACID compliance using **Delta Lake**.

1. **Ingestion (Extract & Load):** **Apache Sqoop** extracts raw tables from the SQL Server and loads them directly into HDFS natively, preventing network bottlenecks.
2. **🥉 Bronze Layer:** Raw ingested data managed via **HiveQL**.
3. **🥈 Silver Layer:** Cleaned, filtered, and standardized data processed via **SparkSQL**, acting as the Enterprise Data Warehouse (with SCD Type 2).
4. **🥇 Gold Layer (Data Marts):** Business-level aggregations. I built the `monthly_sales` aggregate table via SparkSQL to serve the BI layer directly.

<a name="finops"></a>
## ☁️ Cloud Native & FinOps Optimization

**The Problem:** Keeping a 4-node EC2 cluster running 24/7 just to serve a dashboard via Spark Thrift Server is highly inefficient and expensive.
**The Solution:** Separation of Storage and Compute.

![FinOps Transition](assets/finops_transition_flow.png)

I exported the `gold.agg_monthly_sales` table from local HDFS to an **Amazon S3** bucket in Parquet format. I then cataloged this S3 data using **Amazon Athena**. 
* **Impact:** The EC2 cluster can be safely terminated after the ETL job completes. BI users query the data using Athena's serverless SQL engine, meaning we **only pay for queries executed**, reducing infrastructure costs by +80%.

<a name="bi"></a>
## 📊 Business Intelligence (Amazon QuickSight)

The final data product is an interactive dashboard built in **Amazon QuickSight**, natively connected to the Athena serverless engine.

### InternetSales Performance Dashboard
*Focus: Monthly revenue trends, gross margin tracking, tax calculations, and territorial performance.*
![QuickSight Dashboard](assets/quicksight_internetsales_dashboard.png)

<a name="install"></a>
## 💻 How to Run

### Prerequisites
* AWS Account (for EC2, S3, Athena)
* SSH Client (to access the EC2 cluster)
* Hadoop 3.x, Spark 3.x, Hive installed on the instances.

### Steps

1. **Clone the repository**
   bash
   git clone [https://github.com/ChahiriAbderrahmane/adventureworks-hybrid-lakehouse.git](https://github.com/ChahiriAbderrahmane/adventureworks-hybrid-lakehouse.git)



2. **Ingest Data via Sqoop (Run on Master Node)** bash
bash scripts/ingest_adventureworks_sqoop.sh






3. **Execute the Spark ETL Pipeline**
bash
spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
  --class com.adventureworks.lakehouse.Main \
  target/lakehouse-etl-1.0.jar




4. **Export Gold Data to S3**
bash
aws s3 cp hdfs:///user/hive/warehouse/gold.db/monthly_sales s3://my-adventureworks-bucket/gold/monthly_sales/ --recursive




5. **Serve via Athena**
Execute the DDL script found in `sql/athena_setup.sql` in the AWS Athena console to register the external table.

<a name="contact"></a>

## 📨 Contact Me

[LinkedIn](https://www.linkedin.com/in/abderrahmane-chahiri-151b26237/) •
[Gmail](mailto:chahiri.abderrahmane.eng@gmail.com)

<div align="center">
Made with ❤️ by <a href="https://www.linkedin.com/in/abderrahmane-chahiri-151b26237/">Abderrahmane Chahiri</a>
</div>



